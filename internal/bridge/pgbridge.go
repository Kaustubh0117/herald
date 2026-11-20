// Package bridge turns committed Postgres row changes into Herald publishes.
//
// It reads the write-ahead log through a logical replication slot (pgoutput
// plugin), so applications need no publish() calls and a notification
// exists if and only if its transaction committed. The commit LSN is the
// version: monotonic by construction, unlike the wall-clock versions the
// paper warns about (Sec. 3.1).
//
// Delivery is at-least-once: a transaction's position is acknowledged to
// Postgres only after every object it touched was published. A crash
// before that means Postgres redelivers it, and the Matcher ignores
// versions it already has.
package bridge

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/Kaustubh0117/herald/internal/metrics"
	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// Publisher is the Bridge's view of a cell (server.Cell satisfies it).
// stamps, if given, carry the commit and Bridge times for latency
// measurement.
type Publisher interface {
	Publish(id p.ObjectID, v p.Version, source p.ClientID, stamps ...p.Stamps) error
}

type Bridge struct {
	DSN         string // a normal connection string; replication=database is added
	Slot        string // created beforehand, one per cell: the durable cursor
	Publication string // which tables to watch
	Out         Publisher

	StatusInterval time.Duration // how often to confirm progress (default 10s)
	Logf           func(format string, args ...any)
}

// txn collects the objects touched by one transaction.
type txn struct {
	objects map[p.ObjectID]bool
}

// add records the row object ("products:42") and the table object
// ("products:*") so list views can register for "anything in this table".
func (t *txn) add(rel *pglogrepl.RelationMessage, tup *pglogrepl.TupleData) {
	if rel == nil || tup == nil {
		return
	}
	var key []string
	for i, col := range rel.Columns {
		if col.Flags == 1 && i < len(tup.Columns) { // part of the replica identity
			key = append(key, string(tup.Columns[i].Data))
		}
	}
	t.objects[p.ObjectID(rel.RelationName+":"+strings.Join(key, ","))] = true
	t.objects[tableObject(rel)] = true
}

func tableObject(rel *pglogrepl.RelationMessage) p.ObjectID {
	if rel == nil {
		return ""
	}
	return p.ObjectID(rel.RelationName + ":*")
}

// replicationDSN adds replication=database, which puts the connection in
// walsender mode so it can run START_REPLICATION.
func replicationDSN(dsn string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "replication=database"
}

func (b *Bridge) logf(format string, args ...any) {
	if b.Logf != nil {
		b.Logf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// Run streams changes until ctx is cancelled or the connection fails. The
// caller restarts it on error; the slot makes restarts lossless.
func (b *Bridge) Run(ctx context.Context) error {
	interval := b.StatusInterval
	if interval == 0 {
		interval = 10 * time.Second
	}
	conn, err := pgconn.Connect(ctx, replicationDSN(b.DSN))
	if err != nil {
		return fmt.Errorf("bridge: connect: %w", err)
	}
	defer conn.Close(context.Background())

	// Start from LSN 0: Postgres resumes from the slot's confirmed position.
	err = pglogrepl.StartReplication(ctx, conn, b.Slot, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", b.Publication)},
	})
	if err != nil {
		return fmt.Errorf("bridge: start replication: %w", err)
	}

	relations := map[uint32]*pglogrepl.RelationMessage{}
	var cur *txn
	var acked pglogrepl.LSN // everything before this is published
	var nextStatus time.Time

	for {
		if !time.Now().Before(nextStatus) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: acked})
			if err != nil {
				return fmt.Errorf("bridge: status update: %w", err)
			}
			nextStatus = time.Now().Add(interval)
		}

		rctx, cancel := context.WithDeadline(ctx, nextStatus)
		raw, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				continue // time to send a status update
			}
			return fmt.Errorf("bridge: receive: %w", err)
		}
		if e, ok := raw.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("bridge: server error: %s", e.Message)
		}
		msg, ok := raw.(*pgproto3.CopyData)
		if !ok || len(msg.Data) == 0 {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("bridge: keepalive: %w", err)
			}
			// Between transactions, everything the server has sent us is
			// published, so confirming its position lets Postgres recycle
			// WAL on a quiet database.
			if cur == nil && ka.ServerWALEnd > acked {
				acked = ka.ServerWALEnd
			}
			if ka.ReplyRequested {
				nextStatus = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("bridge: xlogdata: %w", err)
			}
			lm, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("bridge: decode: %w", err)
			}

			switch m := lm.(type) {
			case *pglogrepl.RelationMessage:
				relations[m.RelationID] = m
			case *pglogrepl.BeginMessage:
				cur = &txn{objects: map[p.ObjectID]bool{}}
			case *pglogrepl.InsertMessage:
				cur.add(relations[m.RelationID], m.Tuple)
			case *pglogrepl.UpdateMessage:
				if m.OldTuple != nil { // primary key changed: old row "changed" too
					cur.add(relations[m.RelationID], m.OldTuple)
				}
				cur.add(relations[m.RelationID], m.NewTuple)
			case *pglogrepl.DeleteMessage:
				cur.add(relations[m.RelationID], m.OldTuple)
			case *pglogrepl.TruncateMessage:
				for _, id := range m.RelationIDs {
					if o := tableObject(relations[id]); o != "" {
						cur.objects[o] = true
					}
				}
			case *pglogrepl.CommitMessage:
				v := p.Version(m.CommitLSN)
				var stamps p.Stamps
				stamps.Stamp(p.StageCommit, m.CommitTime)
				stamps.Stamp(p.StageBridge, time.Now())
				for id := range cur.objects {
					if err := b.publish(ctx, id, v, stamps); err != nil {
						return err
					}
				}
				cur = nil
				acked = m.TransactionEndLSN // now safe to let Postgres forget it
				nextStatus = time.Time{}
			}
		}
	}
}

// publish retries until the cell accepts the version or ctx ends. Blind
// retries are safe because Publish is idempotent and only moves forward.
func (b *Bridge) publish(ctx context.Context, id p.ObjectID, v p.Version, stamps p.Stamps) error {
	backoff := 10 * time.Millisecond
	for {
		err := b.Out.Publish(id, v, "", stamps)
		if err == nil {
			metrics.PublishTotal.Inc()
			return nil
		}
		metrics.PublishRetries.Inc()
		b.logf("bridge: publish %s@%d failed (retrying in %v): %v", id, v, backoff, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 2*time.Second)
	}
}

// WatchLag samples the slot's lag into herald_bridge_slot_lag_bytes every
// interval until ctx ends. It uses its own ordinary connection: the
// replication connection is busy streaming.
func WatchLag(ctx context.Context, dsn, slot string, interval time.Duration) {
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			conn.Close(context.Background())
		}
	}()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if conn == nil {
			conn, _ = pgx.Connect(ctx, dsn) // nil on failure: retried next tick
		}
		if conn != nil {
			if lag, err := SlotLag(ctx, conn, slot); err == nil {
				metrics.SlotLagBytes.Set(float64(lag))
			} else {
				conn.Close(context.Background())
				conn = nil
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// EnsureSlot creates the logical replication slot if it doesn't exist.
func EnsureSlot(ctx context.Context, conn *pgx.Conn, slot string) error {
	_, err := conn.Exec(ctx, `
		SELECT pg_create_logical_replication_slot($1, 'pgoutput')
		WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slot)
	return err
}

// SlotLag reports how many bytes of WAL Postgres is keeping for the slot
// because the Bridge hasn't confirmed them yet. Alert when it grows: a
// stalled Bridge makes Postgres keep WAL until max_slot_wal_keep_size.
func SlotLag(ctx context.Context, conn *pgx.Conn, slot string) (int64, error) {
	var lag int64
	err := conn.QueryRow(ctx, `
		SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
		FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&lag)
	return lag, err
}

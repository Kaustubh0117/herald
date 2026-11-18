// Package store keeps a cell's soft state in Postgres (server.Store). Every
// write is one idempotent statement, guarded by version where order
// matters: the paper's Bigtable blind writes translated to upserts
// (Sec. 4.2). Each cell lives in its own schema, so cells can share a
// database.
package store

import (
	"context"
	_ "embed"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
)

//go:embed schema.sql
var schemaSQL string

// callTimeout bounds every statement. A timeout is just a failed write,
// which the server turns into a failed (and retried) operation.
const callTimeout = 5 * time.Second

type PG struct {
	db *pgxpool.Pool
	s  string // quoted schema name
}

var _ server.Store = (*PG)(nil)

// Open creates the cell's schema and tables if missing. Several processes
// may run it at once at startup, and concurrent CREATE ... IF NOT EXISTS can
// still collide inside Postgres, so it retries a few times.
func Open(ctx context.Context, db *pgxpool.Pool, schema string) (*PG, error) {
	st := &PG{db: db, s: pgx.Identifier{schema}.Sanitize()}
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if _, err = db.Exec(ctx, st.q(schemaSQL)); err == nil {
			return st, nil
		}
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}
	return nil, err
}

// q fills the schema into a statement.
func (st *PG) q(sql string) string { return strings.ReplaceAll(sql, "{S}", st.s) }

func (st *PG) exec(sql string, args ...any) error {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	_, err := st.db.Exec(ctx, st.q(sql), args...)
	return err
}

func key(id string) string { return p.PartitionKey(id) }

func (st *PG) Epoch() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	var e int64
	err := st.db.QueryRow(ctx, st.q(`
		WITH ins AS (
			INSERT INTO {S}.meta (key, value) VALUES ('epoch', $1)
			ON CONFLICT (key) DO NOTHING RETURNING value)
		SELECT value FROM ins UNION ALL SELECT value FROM {S}.meta WHERE key = 'epoch'
		LIMIT 1`), time.Now().UnixMilli()).Scan(&e)
	return uint64(e), err
}

// Wipe empties every table and moves the epoch forward (to at least the
// current time in ms), which makes every outstanding token stale. It is
// also the answer to schema migrations: drop the state, keep correctness.
func (st *PG) Wipe() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	tx, err := st.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, st.q(`TRUNCATE {S}.matcher_objects, {S}.matcher_registrations,
		{S}.registrar_clients, {S}.registrar_registrations, {S}.registrar_notifications`)); err != nil {
		return 0, err
	}
	var e int64
	if err := tx.QueryRow(ctx, st.q(`
		INSERT INTO {S}.meta (key, value) VALUES ('epoch', $1)
		ON CONFLICT (key) DO UPDATE SET value = GREATEST({S}.meta.value + 1, EXCLUDED.value)
		RETURNING value`), time.Now().UnixMilli()).Scan(&e); err != nil {
		return 0, err
	}
	return uint64(e), tx.Commit(ctx)
}

// ---- Matcher view ----

func (st *PG) SaveVersion(id p.ObjectID, v p.Version, src p.ClientID) error {
	return st.exec(`
		INSERT INTO {S}.matcher_objects AS m (pkey, object_id, version, source_client, pending)
		VALUES ($1, $2, $3, $4, true)
		ON CONFLICT (pkey) DO UPDATE
		   SET version = EXCLUDED.version, source_client = EXCLUDED.source_client, pending = true
		 WHERE m.version < EXCLUDED.version`,
		key(string(id)), string(id), int64(v), string(src))
}

// ClearPending removes the propagation marker -- but only if no newer
// version landed while the propagator was working.
func (st *PG) ClearPending(id p.ObjectID, v p.Version) error {
	return st.exec(`UPDATE {S}.matcher_objects SET pending = false WHERE pkey = $1 AND version = $2`,
		key(string(id)), int64(v))
}

func (st *PG) SaveMatcherReg(id p.ObjectID, c p.ClientID) error {
	return st.exec(`INSERT INTO {S}.matcher_registrations (obj_pkey, object_id, client_id)
		VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, key(string(id)), string(id), string(c))
}

func (st *PG) DeleteMatcherReg(id p.ObjectID, c p.ClientID) error {
	return st.exec(`DELETE FROM {S}.matcher_registrations WHERE obj_pkey = $1 AND client_id = $2`,
		key(string(id)), string(c))
}

// rangeArgs turns server i of n into a key range for `col >= $1 AND
// ($2 = ” OR col < $2)`: one index range scan per table.
func rangeArgs(i, n int) (string, string) { return p.KeyRange(i, n) }

func (st *PG) LoadMatcher(i, n int) ([]server.StoredObject, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*callTimeout)
	defer cancel()
	lo, hi := rangeArgs(i, n)
	byKey := map[string]*server.StoredObject{}

	rows, err := st.db.Query(ctx, st.q(`SELECT pkey, object_id, version, source_client, pending
		FROM {S}.matcher_objects WHERE pkey >= $1 AND ($2 = '' OR pkey < $2)`), lo, hi)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k, id, src string
		var v int64
		var pending bool
		if err := rows.Scan(&k, &id, &v, &src, &pending); err != nil {
			return nil, err
		}
		byKey[k] = &server.StoredObject{ID: p.ObjectID(id), Version: p.Version(v), Known: true,
			Source: p.ClientID(src), Pending: pending}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = st.db.Query(ctx, st.q(`SELECT obj_pkey, object_id, client_id
		FROM {S}.matcher_registrations WHERE obj_pkey >= $1 AND ($2 = '' OR obj_pkey < $2)`), lo, hi)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k, id, c string
		if err := rows.Scan(&k, &id, &c); err != nil {
			return nil, err
		}
		o := byKey[k]
		if o == nil {
			o = &server.StoredObject{ID: p.ObjectID(id)}
			byKey[k] = o
		}
		o.Clients = append(o.Clients, p.ClientID(c))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]server.StoredObject, 0, len(byKey))
	for _, o := range byKey {
		out = append(out, *o)
	}
	return out, nil
}

// ---- Registrar view ----

func (st *PG) SaveClient(c p.ClientID, seq uint64, addr string, lastSeen time.Time) error {
	return st.exec(`
		INSERT INTO {S}.registrar_clients (pkey, client_id, seq, gateway, last_seen)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (pkey) DO UPDATE
		   SET seq = EXCLUDED.seq, gateway = EXCLUDED.gateway, last_seen = EXCLUDED.last_seen`,
		key(string(c)), string(c), int64(seq), addr, lastSeen)
}

func (st *PG) DeleteClient(c p.ClientID) error {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	k := key(string(c))
	return pgx.BeginFunc(ctx, st.db, func(tx pgx.Tx) error {
		for _, t := range []string{"registrar_notifications", "registrar_registrations"} {
			if _, err := tx.Exec(ctx, st.q(`DELETE FROM {S}.`+t+` WHERE client_pkey = $1`), k); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, st.q(`DELETE FROM {S}.registrar_clients WHERE pkey = $1`), k)
		return err
	})
}

func (st *PG) SaveClientReg(c p.ClientID, id p.ObjectID, active bool) error {
	return st.exec(`
		INSERT INTO {S}.registrar_registrations (client_pkey, object_id, active, pending)
		VALUES ($1, $2, $3, true)
		ON CONFLICT (client_pkey, object_id) DO UPDATE SET active = EXCLUDED.active, pending = true`,
		key(string(c)), string(id), active)
}

func (st *PG) ClearClientReg(c p.ClientID, id p.ObjectID, active bool) error {
	if active {
		return st.exec(`UPDATE {S}.registrar_registrations SET pending = false
			WHERE client_pkey = $1 AND object_id = $2 AND active`, key(string(c)), string(id))
	}
	return st.exec(`DELETE FROM {S}.registrar_registrations
		WHERE client_pkey = $1 AND object_id = $2 AND NOT active`, key(string(c)), string(id))
}

func (st *PG) SaveNotification(c p.ClientID, n p.Notification) error {
	return st.exec(`
		INSERT INTO {S}.registrar_notifications (client_pkey, object_id, version, unknown, seq)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (client_pkey, object_id) DO UPDATE
		   SET version = EXCLUDED.version, unknown = EXCLUDED.unknown, seq = EXCLUDED.seq`,
		key(string(c)), string(n.Object), int64(n.Version), n.Unknown, int64(n.Seq))
}

func (st *PG) DeleteNotification(c p.ClientID, id p.ObjectID) error {
	return st.exec(`DELETE FROM {S}.registrar_notifications WHERE client_pkey = $1 AND object_id = $2`,
		key(string(c)), string(id))
}

func (st *PG) LoadRegistrar(i, n int) ([]server.StoredClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*callTimeout)
	defer cancel()
	lo, hi := rangeArgs(i, n)
	byKey := map[string]*server.StoredClient{}

	rows, err := st.db.Query(ctx, st.q(`SELECT pkey, client_id, seq, gateway, last_seen
		FROM {S}.registrar_clients WHERE pkey >= $1 AND ($2 = '' OR pkey < $2)`), lo, hi)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k, c, gw string
		var seq int64
		var seen time.Time
		if err := rows.Scan(&k, &c, &seq, &gw, &seen); err != nil {
			return nil, err
		}
		byKey[k] = &server.StoredClient{ID: p.ClientID(c), Seq: uint64(seq), Addr: gw, LastSeen: seen}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = st.db.Query(ctx, st.q(`SELECT client_pkey, object_id, active, pending
		FROM {S}.registrar_registrations WHERE client_pkey >= $1 AND ($2 = '' OR client_pkey < $2)`), lo, hi)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k, id string
		var r server.StoredReg
		if err := rows.Scan(&k, &id, &r.Active, &r.Pending); err != nil {
			return nil, err
		}
		if c := byKey[k]; c != nil {
			r.Object = p.ObjectID(id)
			c.Regs = append(c.Regs, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = st.db.Query(ctx, st.q(`SELECT client_pkey, object_id, version, unknown, seq
		FROM {S}.registrar_notifications WHERE client_pkey >= $1 AND ($2 = '' OR client_pkey < $2)`), lo, hi)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k, id string
		var v, seq int64
		var unknown bool
		if err := rows.Scan(&k, &id, &v, &unknown, &seq); err != nil {
			return nil, err
		}
		if c := byKey[k]; c != nil {
			c.Pending = append(c.Pending, p.Notification{Object: p.ObjectID(id), Version: p.Version(v),
				Unknown: unknown, Seq: uint64(seq)})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]server.StoredClient, 0, len(byKey))
	for _, c := range byKey {
		out = append(out, *c)
	}
	return out, nil
}

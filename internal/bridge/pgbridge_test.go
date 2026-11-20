package bridge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Kaustubh0117/herald/internal/client"
	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
)

// recorder is a fake Matcher: it fails its first few calls (an outage the
// Bridge must ride out) and remembers the latest version per object.
type recorder struct {
	mu       sync.Mutex
	failures int
	calls    int
	v        map[p.ObjectID]p.Version
}

func (r *recorder) Publish(id p.ObjectID, v p.Version, _ p.ClientID, _ ...p.Stamps) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.calls <= r.failures {
		return errors.New("matcher unavailable")
	}
	if v > r.v[id] {
		r.v[id] = v
	}
	return nil
}

func (r *recorder) latest() map[p.ObjectID]p.Version {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[p.ObjectID]p.Version{}
	for k, v := range r.v {
		out[k] = v
	}
	return out
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// setup creates a fresh schema holding one table, a publication and a slot,
// all named after the test so parallel or aborted runs don't collide.
// Object IDs use the bare table name, so the schema doesn't appear in them.
// Cleanups run last-registered-first, so the connection closes only after
// the slot is dropped (a leaked slot makes Postgres keep WAL forever).
func setup(t *testing.T, table, columns string) (db *pgx.Conn, dsn, name string) {
	t.Helper()
	dsn = os.Getenv("HERALD_PG")
	if dsn == "" {
		t.Skip("HERALD_PG not set; run `make test-pg`")
	}
	ctx := context.Background()
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(ctx) })

	name = fmt.Sprintf("herald_test_%d_%d", os.Getpid(), time.Now().UnixNano()%1e9)
	for _, sql := range []string{
		`CREATE SCHEMA ` + name,
		`CREATE TABLE ` + name + `.` + table + ` (` + columns + `)`,
		`CREATE PUBLICATION ` + name + ` FOR TABLE ` + name + `.` + table,
	} {
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if err := EnsureSlot(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// The slot stays "active" for a moment after the Bridge disconnects.
		var err error
		for i := 0; i < 50; i++ {
			if _, err = db.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, name); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			t.Errorf("leaked replication slot %s: %v", name, err)
		}
		db.Exec(ctx, `DROP PUBLICATION IF EXISTS `+name)
		db.Exec(ctx, `DROP SCHEMA IF EXISTS `+name+` CASCADE`)
	})
	return db, dsn, name
}

// start runs a Bridge in the background; stop cancels it and waits. stop
// also runs at cleanup (before the slot is dropped), so a test that fails
// half way never leaves the Bridge running past its end.
func start(t *testing.T, b *Bridge) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := b.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("bridge: %v", err)
		}
	}()
	stop = sync.OnceFunc(func() { cancel(); <-done })
	t.Cleanup(stop)
	return stop
}

func TestBridgeAgainstPostgres(t *testing.T) {
	db, dsn, name := setup(t, "products", "id int PRIMARY KEY, name text, price int")
	ctx := context.Background()
	exec := func(sql string) {
		t.Helper()
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	tbl := name + ".products"

	rec := &recorder{failures: 3, v: map[p.ObjectID]p.Version{}}
	run := func() func() {
		return start(t, &Bridge{DSN: dsn, Slot: name, Publication: name, Out: rec,
			StatusInterval: 100 * time.Millisecond, Logf: t.Logf})
	}
	stop := run()

	exec(`INSERT INTO ` + tbl + ` VALUES (1, 'lamp', 10), (2, 'desk', 90)`)
	exec(`UPDATE ` + tbl + ` SET price = 12 WHERE id = 1`)
	exec(`DELETE FROM ` + tbl + ` WHERE id = 2`)

	// The INSERT alone produces all three objects, so wait for the DELETE's
	// commit: it must order after the UPDATE and be the table's latest.
	waitFor(t, func() bool {
		l := rec.latest()
		return l["products:2"] > l["products:1"] && l["products:*"] == l["products:2"]
	})
	if l := rec.latest(); len(l) != 3 || l["products:1"] == 0 {
		t.Fatalf("unexpected objects: %v", l)
	}

	// Crash the bridge, write while it is down, restart: nothing is lost.
	stop()
	before := rec.latest()["products:1"]
	exec(`UPDATE ` + tbl + ` SET price = 15 WHERE id = 1`)
	stop = run()
	defer stop()
	waitFor(t, func() bool { return rec.latest()["products:1"] > before })

	// Once caught up, the confirmed position reaches the WAL end.
	waitFor(t, func() bool {
		lag, err := SlotLag(ctx, db, name)
		return err == nil && lag == 0
	})
}

func TestPrimaryKeyChangeNotifiesOldAndNewRow(t *testing.T) {
	db, dsn, name := setup(t, "items", "id int PRIMARY KEY")
	ctx := context.Background()
	rec := &recorder{v: map[p.ObjectID]p.Version{}}
	defer start(t, &Bridge{DSN: dsn, Slot: name, Publication: name, Out: rec, Logf: t.Logf})()

	db.Exec(ctx, `INSERT INTO `+name+`.items VALUES (1)`)
	db.Exec(ctx, `UPDATE `+name+`.items SET id = 7 WHERE id = 1`)
	waitFor(t, func() bool {
		l := rec.latest()
		return l["items:7"] > 0 && l["items:1"] == l["items:7"]
	})
}

// TestEndToEnd: a SQL UPDATE reaches an application's Notify callback
// through the Bridge, a real Cell, and the client library.
func TestEndToEnd(t *testing.T) {
	db, dsn, name := setup(t, "products", "id int PRIMARY KEY, price int")
	ctx := context.Background()
	if _, err := db.Exec(ctx, `INSERT INTO `+name+`.products VALUES (1, 10)`); err != nil {
		t.Fatal(err)
	}

	cell := server.NewCell("e2e", 2, 2, nil)
	bctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { // propagators on a timer, as a real server runs them
		for {
			select {
			case <-bctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
				cell.Step()
			}
		}
	}()
	defer start(t, &Bridge{DSN: dsn, Slot: name, Publication: name, Out: cell, Logf: t.Logf})()

	l := &notifyLog{}
	c := client.New(client.Config{
		Send:     cell.Handle,
		Listener: l,
		Observe:  l.observe,
	})
	c.Register("products:1")
	go c.Run(bctx, 5*time.Millisecond)

	// Registered before any change reached Herald: an unknown-version signal.
	waitFor(t, func() bool { return l.count() > 0 })
	if _, err := db.Exec(ctx, `UPDATE `+name+`.products SET price = 12 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return l.latest() > 0 })

	// The notification carries a stamp from every stage it crossed, in
	// order, starting at the commit (paper Sec. 6.3, Fig. 9). Wait for a
	// stamped one: the INSERT's version can also arrive by post-propagation,
	// which isn't caused by a commit and carries no stamps.
	waitFor(t, func() bool { return l.lastStamps()[p.StageCommit] != 0 })
	s := l.lastStamps()
	for _, st := range []p.Stage{p.StageCommit, p.StageBridge, p.StageMatcher, p.StageRegistrar} {
		if s[st] == 0 {
			t.Fatalf("missing %s stamp: %v", st, s)
		}
	}
	ds := s.Deltas(time.Now())
	var total time.Duration
	for _, d := range ds {
		if d.D < 0 {
			t.Fatalf("stage %s went back in time: %v", d.Stage, ds)
		}
		total += d.D
	}
	if total <= 0 || total > 10*time.Second {
		t.Fatalf("commit-to-client delay %v is implausible: %v", total, ds)
	}
	t.Logf("commit to client in %v: %v", total, ds)
}

type notifyLog struct {
	mu     sync.Mutex
	n      int
	v      p.Version
	stamps p.Stamps // of the latest notification that had any
}

func (l *notifyLog) observe(n p.Notification, _ time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n.Stamps != (p.Stamps{}) {
		l.stamps = n.Stamps
	}
}

func (l *notifyLog) lastStamps() p.Stamps {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stamps
}

func (l *notifyLog) Notify(_ p.ObjectID, v p.Version) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.n++
	l.v = v
}

func (l *notifyLog) NotifyUnknown(p.ObjectID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.n++
}

func (l *notifyLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.n
}

func (l *notifyLog) latest() p.Version {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.v
}

func (l *notifyLog) RegistrationStatusChanged(p.ObjectID, bool) {}
func (l *notifyLog) ReissueRegistrations()                      {}
func (l *notifyLog) WriteState([]byte)                          {}

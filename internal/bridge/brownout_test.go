package bridge

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kaustubh0117/herald/internal/client"
	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
	"github.com/Kaustubh0117/herald/internal/store"
)

// TestBrownout is Phase 8's DONE WHEN against real Postgres: with 25% of
// the Matcher key space unwritable, the replication slot keeps being acked
// (lag returns to zero during the outage), the other 75% of objects notify
// right away, and the parked versions arrive once the outage ends.
func TestBrownout(t *testing.T) {
	db, dsn, name := setup(t, "products", "id int PRIMARY KEY, price int")
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := db.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	tbl := name + ".products"
	const rows = 40
	for i := 0; i < rows; i++ {
		exec(`INSERT INTO `+tbl+` VALUES ($1, 0)`, i)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pg, err := store.Open(ctx, pool, name+"_state")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+name+`_state CASCADE`) })
	fs := &server.FaultyStore{Store: pg}
	cell, err := server.NewCellWithStore("bo", 2, 4, nil, fs)
	if err != nil {
		t.Fatal(err)
	}
	bctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() {
		for bctx.Err() == nil {
			cell.Step()
			time.Sleep(5 * time.Millisecond)
		}
	}()
	start(t, &Bridge{DSN: dsn, Slot: name, Publication: name, Out: cell,
		StatusInterval: 100 * time.Millisecond, Logf: t.Logf})

	l := &objectLog{v: map[p.ObjectID]p.Version{}}
	c := client.New(client.Config{Send: cell.Handle, Listener: l})
	var objs []p.ObjectID
	for i := 0; i < rows; i++ {
		id := p.ObjectID(fmt.Sprintf("products:%d", i))
		objs = append(objs, id)
		c.Register(id)
	}
	go c.Run(bctx, 10*time.Millisecond)
	waitFor(t, func() bool { return l.signalled(objs) == rows }) // the inserts reached everyone
	waitFor(t, func() bool { lag, err := SlotLag(ctx, db, name); return err == nil && lag == 0 })

	// Brown-out: a quarter of the key space becomes unwritable, and every
	// row changes, one of the unwritable ones many times over.
	fs.SetOutage(0.25)
	down, hot := map[p.ObjectID]bool{}, -1
	for i, id := range objs {
		if fs.Down(string(id)) {
			down[id] = true
			if hot < 0 {
				hot = i
			}
		}
	}
	if len(down) == 0 || len(down) == rows {
		t.Fatalf("outage should hit some but not all rows, hit %d", len(down))
	}
	l.reset()
	for i := 0; i < rows; i++ {
		exec(`UPDATE `+tbl+` SET price = 1 WHERE id = $1`, i)
	}
	for k := 0; k < 30; k++ {
		exec(`UPDATE `+tbl+` SET price = price + 1 WHERE id = $1`, hot)
	}

	// The slot keeps moving: every version was accepted, parked or not.
	waitFor(t, func() bool { lag, err := SlotLag(ctx, db, name); return err == nil && lag == 0 })
	waitFor(t, func() bool { return l.signalled(objs) == rows-len(down) })
	for id := range down {
		if l.signalled([]p.ObjectID{id}) != 0 {
			t.Fatalf("%s notified while its row was unwritable", id)
		}
	}
	if es, err := pg.LoadScratch(0, 1); err != nil || len(es) > len(down)+1 { // +1: products:*
		t.Fatalf("scratch should hold about one entry per unwritable object (%d), has %d (%v)",
			len(down), len(es), err)
	}

	// Recovery: the parked versions drain and reach the client.
	fs.SetOutage(0)
	waitFor(t, func() bool { return l.signalled(objs) == rows })
	waitFor(t, func() bool { es, err := pg.LoadScratch(0, 1); return err == nil && len(es) == 0 })
	t.Logf("%d of %d rows were unwritable; the slot caught up during the outage; all drained", len(down), rows)
}

// objectLog records which objects were signalled since the last reset.
type objectLog struct {
	mu sync.Mutex
	v  map[p.ObjectID]p.Version
}

func (l *objectLog) Notify(id p.ObjectID, v p.Version) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.v[id] = max(l.v[id], v, 1)
}

func (l *objectLog) NotifyUnknown(id p.ObjectID) { l.Notify(id, 1) }

func (l *objectLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.v = map[p.ObjectID]p.Version{}
}

func (l *objectLog) signalled(ids []p.ObjectID) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, id := range ids {
		if l.v[id] > 0 {
			n++
		}
	}
	return n
}

func (l *objectLog) RegistrationStatusChanged(p.ObjectID, bool) {}
func (l *objectLog) ReissueRegistrations()                      {}
func (l *objectLog) WriteState([]byte)                          {}

package store

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
)

// openTest opens a Postgres store in a schema of its own, dropped afterwards.
func openTest(t testing.TB) *PG {
	t.Helper()
	dsn := os.Getenv("HERALD_PG")
	if dsn == "" {
		t.Skip("HERALD_PG not set; run `make test-pg`")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("herald_test_state_%d_%d", os.Getpid(), time.Now().UnixNano()%1e9)
	st, err := Open(ctx, db, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		db.Close()
	})
	return st
}

func loadObject(t *testing.T, st server.MatcherStore, id p.ObjectID) server.StoredObject {
	t.Helper()
	objs, err := st.LoadMatcher(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range objs {
		if o.ID == id {
			return o
		}
	}
	return server.StoredObject{}
}

// TestVersionGuard is the guide's store test: v5, v3, v5, v9 -> only v5 and
// v9 advance, and a stale propagator can't clear v9's pending marker.
func TestVersionGuard(t *testing.T) {
	st := openTest(t)
	for _, v := range []p.Version{5, 3, 5} {
		if err := st.SaveVersion("products:1", v, ""); err != nil {
			t.Fatal(err)
		}
	}
	if o := loadObject(t, st, "products:1"); o.Version != 5 || !o.Pending {
		t.Fatalf("after 5,3,5 want v5 pending, got %+v", o)
	}
	st.ClearPending("products:1", 5)
	st.SaveVersion("products:1", 9, "")
	st.ClearPending("products:1", 5) // a propagator that was working on v5
	if o := loadObject(t, st, "products:1"); o.Version != 9 || !o.Pending {
		t.Fatalf("stale ClearPending touched v9: %+v", o)
	}
	st.ClearPending("products:1", 9)
	if o := loadObject(t, st, "products:1"); o.Pending {
		t.Fatalf("ClearPending(9) should clear v9: %+v", o)
	}
}

// conformance runs the same checks against any server.Store, so MemStore
// (used by the simulation) and PG (used in production) can't drift apart.
func conformance(t *testing.T, st server.Store) {
	e1, err := st.Epoch()
	if err != nil || e1 == 0 {
		t.Fatalf("epoch: %d %v", e1, err)
	}
	if e, _ := st.Epoch(); e != e1 {
		t.Fatalf("epoch must be stable until a wipe: %d then %d", e1, e)
	}

	// Matcher view: registrations without a version load as unknown objects.
	st.SaveMatcherReg("a", "c1")
	st.SaveMatcherReg("a", "c1") // duplicate
	st.SaveMatcherReg("a", "c2")
	st.DeleteMatcherReg("a", "c2")
	if o := loadObject(t, st, "a"); o.Known || !slices.Equal(o.Clients, []p.ClientID{"c1"}) {
		t.Fatalf("matcher registrations: %+v", o)
	}

	// Registrar view.
	seen := time.Unix(1_700_000_000, 0).UTC()
	st.SaveClient("c1", 42, "gw-1", seen)
	st.SaveClientReg("c1", "a", true)
	st.SaveClientReg("c1", "b", true)
	st.ClearClientReg("c1", "a", true) // a reached its Matcher
	st.SaveClientReg("c1", "b", false) // b unregistered before reaching it...
	st.ClearClientReg("c1", "b", true) // ...so the stale "add reached" is ignored
	st.SaveNotification("c1", p.Notification{Object: "a", Version: 7})
	st.SaveNotification("c1", p.Notification{Object: "x", Unknown: true, Seq: 9})
	st.DeleteNotification("c1", "x")

	clients, err := st.LoadRegistrar(0, 1)
	if err != nil || len(clients) != 1 {
		t.Fatalf("load: %+v %v", clients, err)
	}
	c := clients[0]
	if c.ID != "c1" || c.Seq != 42 || c.Addr != "gw-1" || !c.LastSeen.Equal(seen) {
		t.Fatalf("client row: %+v", c)
	}
	slices.SortFunc(c.Regs, func(x, y server.StoredReg) int { return compare(x.Object, y.Object) })
	want := []server.StoredReg{{Object: "a", Active: true}, {Object: "b", Active: false, Pending: true}}
	if !slices.Equal(c.Regs, want) {
		t.Fatalf("registrations: got %+v want %+v", c.Regs, want)
	}
	if len(c.Pending) != 1 || c.Pending[0] != (p.Notification{Object: "a", Version: 7}) {
		t.Fatalf("notifications: %+v", c.Pending)
	}
	st.ClearClientReg("c1", "b", false) // the unregistration reached the Matcher: row goes
	if clients, _ := st.LoadRegistrar(0, 1); len(clients[0].Regs) != 1 {
		t.Fatalf("propagated unregistration should be deleted: %+v", clients[0].Regs)
	}

	// Key ranges: each client loads on exactly the Registrar that owns it.
	for i := 0; i < 50; i++ {
		st.SaveClient(p.ClientID(fmt.Sprintf("k%d", i)), 0, "", seen)
	}
	total := 0
	for s := 0; s < 3; s++ {
		cs, _ := st.LoadRegistrar(s, 3)
		for _, c := range cs {
			if p.Owner(string(c.ID), 3) != s {
				t.Fatalf("%s loaded on registrar %d, owned by %d", c.ID, s, p.Owner(string(c.ID), 3))
			}
		}
		total += len(cs)
	}
	if total != 51 {
		t.Fatalf("ranges should partition all 51 clients, got %d", total)
	}

	// Garbage collection removes a client with everything it owns.
	st.DeleteClient("c1")
	cs, _ := st.LoadRegistrar(0, 1)
	for _, c := range cs {
		if c.ID == "c1" {
			t.Fatal("deleted client still loads")
		}
	}

	// Wipe empties everything and moves the epoch forward.
	e2, err := st.Wipe()
	if err != nil || e2 <= e1 {
		t.Fatalf("wipe: epoch %d -> %d (%v)", e1, e2, err)
	}
	if objs, _ := st.LoadMatcher(0, 1); len(objs) != 0 {
		t.Fatalf("wipe left matcher state: %+v", objs)
	}
	if cs, _ := st.LoadRegistrar(0, 1); len(cs) != 0 {
		t.Fatalf("wipe left registrar state: %+v", cs)
	}
}

func compare(a, b p.ObjectID) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func TestConformanceMemStore(t *testing.T) { conformance(t, server.NewMemStore()) }
func TestConformancePostgres(t *testing.T) { conformance(t, openTest(t)) }

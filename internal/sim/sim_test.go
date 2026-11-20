// Package sim is Herald's correctness test: a whole cell plus many clients
// in one process, under message loss, RPC failures, client restarts and
// single-server crashes, total server state loss and (with a store)
// partial storage outages. It checks the paper's reliable-delivery
// property (Sec. 5): every well-behaved client eventually learns the
// latest version of every object it is registered for. With a store it
// also checks that an outage doesn't stall the Bridge's input.
//
// Everything is driven by one seeded RNG and runs on one goroutine, so a
// failing seed reproduces exactly.
package sim

import (
	"context"
	"flag"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kaustubh0117/herald/internal/client"
	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
	pgstore "github.com/Kaustubh0117/herald/internal/store"
)

const (
	numApps    = 30
	numObjects = 20
	wantsEach  = 5
	numRounds  = 400
	loss       = 0.2             // client<->cell requests, replies, and fast-path pushes
	rpcFail    = 0.1             // calls between Matchers, Registrars and the Bridge
	storeFail  = 0.05            // persist mode: store writes that fail
	gcTTL      = 8 * time.Second // idle clients collected after 8 rounds
	outage     = 0.25            // persist mode: fraction of Matcher keys unwritable...
	maxBacklog = 5               // ...without the Bridge's input piling up past this
)

// outages are the rounds [from, to) during which part of the Matcher key
// space is unwritable (persist mode only).
var outages = [][2]int{{60, 130}, {200, 260}}

// store is the fake application database. Herald never sees its values,
// only that versions go up; the global counter mimics a commit LSN.
type store struct {
	lsn p.Version
	v   map[p.ObjectID]p.Version
}

func (s *store) Bump(id p.ObjectID) p.Version {
	s.lsn++
	s.v[id] = s.lsn
	return s.lsn
}

func (s *store) Get(id p.ObjectID) p.Version { return s.v[id] }

// app is a well-behaved application: on every signal it refetches from the
// store. Its cache and persisted token survive restarts (think browser
// localStorage), which is the stricter case: stale data can't hide.
type app struct {
	addr  string
	c     *client.Client
	store *store
	want  map[p.ObjectID]bool
	cache map[p.ObjectID]p.Version
	state []byte
}

func (a *app) Notify(obj p.ObjectID, _ p.Version)         { a.cache[obj] = a.store.Get(obj) }
func (a *app) NotifyUnknown(obj p.ObjectID)               { a.cache[obj] = a.store.Get(obj) }
func (a *app) RegistrationStatusChanged(p.ObjectID, bool) {}
func (a *app) WriteState(s []byte)                        { a.state = s }

func (a *app) ReissueRegistrations() {
	for _, id := range slices.Sorted(maps.Keys(a.want)) {
		a.c.Register(id)
	}
}

type world struct {
	rng    *rand.Rand
	loss   float64
	cell   *server.Cell
	store  *store
	apps   []*app
	byAddr map[string]*app
}

// send is the lossy client<->cell transport. Losing the reply after the
// server applied the request is the nasty case retries must survive.
func (w *world) send(m p.ClientMessage) (p.ServerMessage, error) {
	if w.rng.Float64() < w.loss {
		return p.ServerMessage{}, fmt.Errorf("request lost")
	}
	resp, err := w.cell.Handle(m)
	if err != nil {
		return p.ServerMessage{}, err // a failed store write: the client retries
	}
	if w.rng.Float64() < w.loss {
		return p.ServerMessage{}, fmt.Errorf("reply lost")
	}
	return resp, nil
}

// push is the lossy fast path from Registrars to clients.
func (w *world) push(addr string, _ p.ClientID, ns []p.Notification) {
	if w.rng.Float64() < w.loss {
		return
	}
	if a := w.byAddr[addr]; a != nil {
		a.c.OnPush(ns)
	}
}

// start (re)starts an app's process from its persisted state.
func (w *world) start(a *app) {
	a.c = client.New(client.Config{Send: w.send, Listener: a, State: a.state, Addr: a.addr})
}

// run plays one seeded scenario and returns every property violation.
// run plays one seeded scenario and returns every property violation.
// With a store, servers persist their state, restarted servers reload it,
// and Wipe clears it; a MemStore additionally fails 5% of writes.
func run(seed int64, st server.Store) []string {
	w := &world{
		rng:    rand.New(rand.NewSource(seed)),
		loss:   loss,
		store:  &store{v: map[p.ObjectID]p.Version{}},
		byAddr: map[string]*app{},
	}
	ms, _ := st.(*server.MemStore)
	if ms != nil {
		ms.Fail = func() bool { return w.rng.Float64() < storeFail }
	}
	var fs *server.FaultyStore
	if st != nil {
		if _, err := st.Wipe(); err != nil { // start from empty state
			return []string{"wipe: " + err.Error()}
		}
		fs = &server.FaultyStore{Store: st}
		var err error
		if w.cell, err = server.NewCellWithStore("sim", 3, 3, w.push, fs); err != nil {
			return []string{"start: " + err.Error()}
		}
	} else {
		w.cell = server.NewCell("sim", 3, 3, w.push)
	}
	w.cell.Fail = func() bool { return w.rng.Float64() < rpcFail }
	// A simulated clock, one second per round, so idle clients get garbage
	// collected and come back as zombies with a fresh ID (paper Sec. 4.2.4).
	clock := time.Unix(0, 0)
	w.cell.SetClock(func() time.Time { return clock })

	objs := make([]p.ObjectID, numObjects)
	for i := range objs {
		objs[i] = p.ObjectID(fmt.Sprintf("obj:%d", i))
	}
	for i := 0; i < numApps; i++ {
		a := &app{
			addr:  fmt.Sprintf("app-%d", i),
			store: w.store,
			want:  map[p.ObjectID]bool{},
			cache: map[p.ObjectID]p.Version{},
		}
		for len(a.want) < wantsEach {
			a.want[objs[w.rng.Intn(len(objs))]] = true
		}
		w.apps = append(w.apps, a)
		w.byAddr[a.addr] = a
		w.start(a)
	}

	// The Bridge's durable input log: a version leaves it only once the
	// Matcher accepted it (Phase 5 gets this from the replication slot).
	// Like the real Bridge it is consumed strictly in order, so one failing
	// publish holds up everything behind it.
	var backlog []p.Notification
	publishBacklog := func() {
		for len(backlog) > 0 && w.cell.Publish(backlog[0].Object, backlog[0].Version, "") == nil {
			backlog = backlog[1:]
		}
	}
	var violations []string

	for round := 0; round < numRounds; round++ {
		clock = clock.Add(time.Second)
		for _, o := range outages {
			if fs == nil {
				break
			}
			switch round {
			case o[0]:
				fs.SetOutage(outage)
			case o[1]:
				fs.SetOutage(0)
				// Unwritable versions were parked in scratch, so the input
				// kept flowing (paper Sec. 4.2.6).
				if len(backlog) > maxBacklog {
					violations = append(violations,
						fmt.Sprintf("round %d: Bridge backlog of %d versions after an outage", round, len(backlog)))
				}
			}
		}
		if round%10 == 0 {
			_ = w.cell.GC(gcTTL) // a failed store write just leaves the client for next time
		}
		if w.rng.Float64() < 0.5 { // an application write commits
			id := objs[w.rng.Intn(len(objs))]
			backlog = append(backlog, p.Notification{Object: id, Version: w.store.Bump(id)})
		}
		publishBacklog()

		a := w.apps[w.rng.Intn(len(w.apps))]
		switch r := w.rng.Float64(); {
		case r < 0.05: // app changes its mind
			id := objs[w.rng.Intn(len(objs))]
			if a.want[id] {
				delete(a.want, id)
				a.c.Unregister(id)
			} else {
				a.want[id] = true
				a.c.Register(id)
			}
		case r < 0.07: // app process restarts with its persisted token
			w.start(a)
		}

		for _, a := range w.apps {
			if w.rng.Float64() < 0.3 {
				_ = a.c.Tick()
			}
		}
		// Individual server crashes, as separate processes would have.
		// Both draws happen every round to keep the RNG sequence stable.
		if r, i := w.rng.Float64(), w.rng.Intn(w.cell.NumMatchers()); r < 0.01 {
			w.cell.RestartMatcher(i)
		}
		if r, i := w.rng.Float64(), w.rng.Intn(w.cell.NumRegistrars()); r < 0.01 {
			w.cell.RestartRegistrar(i)
		}
		w.cell.Step()
		if round == 150 || round == 300 {
			w.cell.Wipe() // drop every byte of server state
		}
	}

	// Quiesce: heal the network and let the system converge.
	w.loss = 0
	w.cell.Fail = nil
	if ms != nil {
		ms.Fail = nil
	}
	for i := 0; i < 10; i++ {
		publishBacklog()
		for _, a := range w.apps {
			_ = a.c.Tick()
		}
		w.cell.Step()
	}

	// Reliable delivery property (paper Sec. 5).
	for _, a := range w.apps {
		for _, id := range slices.Sorted(maps.Keys(a.want)) {
			if got, want := a.cache[id], w.store.Get(id); got != want {
				violations = append(violations,
					fmt.Sprintf("%s %s: has v%d, latest is v%d", a.addr, id, got, want))
			}
		}
	}
	return violations
}

var numSeeds = flag.Int("seeds", 200, "number of chaos seeds to run")

func TestChaos(t *testing.T) {
	seeds := *numSeeds
	if testing.Short() {
		seeds = 5
	}
	total, byMode := 0, map[string]int{}
	for _, persist := range []bool{false, true} {
		mode := map[bool]string{false: "memory", true: "store"}[persist]
		for s := int64(1); s <= int64(seeds); s++ {
			var st server.Store
			if persist {
				st = server.NewMemStore()
			}
			v := run(s, st)
			total += len(v)
			byMode[mode] += len(v)
			for i, msg := range v {
				if i == 3 {
					t.Errorf("%s seed %d: ... and %d more", mode, s, len(v)-3)
					break
				}
				t.Errorf("%s seed %d: %s", mode, s, msg)
			}
		}
	}
	// bench/mutate.sh greps this line.
	t.Logf("total violations over %d seeds: %d (memory %d, store %d)",
		seeds, total, byMode["memory"], byMode["store"])
}

func TestChaosIsDeterministic(t *testing.T) {
	if a, b := fmt.Sprint(run(7, server.NewMemStore())), fmt.Sprint(run(7, server.NewMemStore())); a != b {
		t.Fatalf("same seed, different outcome:\n%s\n%s", a, b)
	}
}

// TestChaosPostgres runs a few seeds with Postgres underneath every server:
// restarts reload from real tables, and wipes truncate them.
func TestChaosPostgres(t *testing.T) {
	dsn := os.Getenv("HERALD_PG")
	if dsn == "" {
		t.Skip("HERALD_PG not set; run `make test-pg`")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := fmt.Sprintf("herald_test_sim_%d_%d", os.Getpid(), time.Now().UnixNano()%1e9)
	defer db.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	st, err := pgstore.Open(ctx, db, schema)
	if err != nil {
		t.Fatal(err)
	}
	for s := int64(1); s <= 3; s++ {
		for _, v := range run(s, st) {
			t.Errorf("postgres seed %d: %s", s, v)
		}
	}
}

// Package sim is Herald's correctness test: a whole cell plus many clients
// in one process, under message loss, RPC failures, client restarts and
// total server state loss. It checks the paper's reliable-delivery
// property (Sec. 5): every well-behaved client eventually learns the
// latest version of every object it is registered for.
//
// Everything is driven by one seeded RNG and runs on one goroutine, so a
// failing seed reproduces exactly.
package sim

import (
	"flag"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"testing"

	"github.com/Kaustubh0117/herald/internal/client"
	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
)

const (
	numApps    = 30
	numObjects = 20
	wantsEach  = 5
	numRounds  = 400
	loss       = 0.2 // client<->cell requests, replies, and fast-path pushes
	rpcFail    = 0.1 // calls between Matchers, Registrars and the Bridge
)

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
	resp := w.cell.Handle(m)
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
func run(seed int64) []string {
	w := &world{
		rng:    rand.New(rand.NewSource(seed)),
		loss:   loss,
		store:  &store{v: map[p.ObjectID]p.Version{}},
		byAddr: map[string]*app{},
	}
	w.cell = server.NewCell("sim", 3, 3, w.push)
	w.cell.Fail = func() bool { return w.rng.Float64() < rpcFail }

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
	var backlog []p.Notification
	publishBacklog := func() {
		var keep []p.Notification
		for _, n := range backlog {
			if w.cell.Publish(n.Object, n.Version, "") != nil {
				keep = append(keep, n)
			}
		}
		backlog = keep
	}

	for round := 0; round < numRounds; round++ {
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
		w.cell.Step()
		if round == 150 || round == 300 {
			w.cell.Wipe() // drop every byte of server state
		}
	}

	// Quiesce: heal the network and let the system converge.
	w.loss = 0
	w.cell.Fail = nil
	for i := 0; i < 10; i++ {
		publishBacklog()
		for _, a := range w.apps {
			_ = a.c.Tick()
		}
		w.cell.Step()
	}

	// Reliable delivery property (paper Sec. 5).
	var violations []string
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

var numSeeds = flag.Int("seeds", 20, "number of chaos seeds to run")

func TestChaos(t *testing.T) {
	seeds := *numSeeds
	if testing.Short() {
		seeds = 5
	}
	total := 0
	for s := int64(1); s <= int64(seeds); s++ {
		v := run(s)
		total += len(v)
		for i, msg := range v {
			if i == 3 {
				t.Errorf("seed %d: ... and %d more", s, len(v)-3)
				break
			}
			t.Errorf("seed %d: %s", s, msg)
		}
	}
	// bench/mutate.sh greps this line.
	t.Logf("total violations over %d seeds: %d", seeds, total)
}

func TestChaosIsDeterministic(t *testing.T) {
	if a, b := fmt.Sprint(run(7)), fmt.Sprint(run(7)); a != b {
		t.Fatalf("same seed, different outcome:\n%s\n%s", a, b)
	}
}

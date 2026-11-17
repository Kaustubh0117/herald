package client

import (
	"errors"
	"math/rand"
	"sync"
	"testing"

	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
)

// app is a test application: it records what the library tells it.
type app struct {
	mu       sync.Mutex
	c        *Client
	want     []p.ObjectID
	notified map[p.ObjectID][]p.Version
	unknown  map[p.ObjectID]int
	state    []byte

	// Optional: a fake app database. When set, every signal refetches
	// from it into cache, exactly like a real application would.
	store map[p.ObjectID]p.Version
	cache map[p.ObjectID]p.Version
}

func newApp(want ...p.ObjectID) *app {
	return &app{want: want, notified: map[p.ObjectID][]p.Version{}, unknown: map[p.ObjectID]int{}}
}

func (a *app) Notify(obj p.ObjectID, v p.Version) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.notified[obj] = append(a.notified[obj], v)
	a.refetch(obj)
}

func (a *app) NotifyUnknown(obj p.ObjectID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.unknown[obj]++
	a.refetch(obj)
}

// refetch reads the object's current version from the store. Caller holds a.mu.
func (a *app) refetch(obj p.ObjectID) {
	if a.store != nil {
		a.cache[obj] = a.store[obj]
	}
}

func (a *app) RegistrationStatusChanged(p.ObjectID, bool) {}

func (a *app) ReissueRegistrations() {
	for _, id := range a.want {
		a.c.Register(id)
	}
}

func (a *app) WriteState(s []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state = s
}

func (a *app) last(obj p.ObjectID) p.Version {
	a.mu.Lock()
	defer a.mu.Unlock()
	vs := a.notified[obj]
	if len(vs) == 0 {
		return 0
	}
	return vs[len(vs)-1]
}

// countingSend wraps cell.Handle and counts handshakes and full syncs.
type countingSend struct {
	cell       *server.Cell
	handshakes int
	fullSyncs  int
}

func (s *countingSend) send(m p.ClientMessage) (p.ServerMessage, error) {
	if m.Token == nil {
		s.handshakes++
	}
	if m.FullSync {
		s.fullSyncs++
	}
	return s.cell.Handle(m), nil
}

func rounds(cell *server.Cell, n int, cs ...*Client) {
	for i := 0; i < n; i++ {
		for _, c := range cs {
			_ = c.Tick()
		}
		cell.Step()
	}
}

func TestDeliversAndSurvivesWipe(t *testing.T) {
	cell := server.NewCell("t", 2, 2, nil)
	s := &countingSend{cell: cell}
	a := newApp("products:1")
	a.c = New(Config{Send: s.send, Listener: a})

	rounds(cell, 4, a.c)
	cell.Publish("products:1", 5, "")
	rounds(cell, 3, a.c)
	if got := a.last("products:1"); got != 5 {
		t.Fatalf("want Notify v5, got %v", a.notified)
	}

	cell.Wipe()
	before := a.unknown["products:1"]
	rounds(cell, 4, a.c)
	if a.unknown["products:1"] <= before {
		t.Fatal("after a wipe the app should be told to refetch")
	}
	cell.Publish("products:1", 9, "")
	rounds(cell, 3, a.c)
	if got := a.last("products:1"); got != 9 {
		t.Fatalf("want Notify v9 after wipe, got %v", a.notified)
	}
	if s.handshakes != 1 {
		t.Fatalf("a wipe should reuse the client ID, got %d handshakes", s.handshakes)
	}
}

func TestRestartResumesClientID(t *testing.T) {
	cell := server.NewCell("t", 2, 2, nil)
	s := &countingSend{cell: cell}
	a := newApp("products:1")
	a.c = New(Config{Send: s.send, Listener: a})
	rounds(cell, 4, a.c)
	id := a.c.tok.Client

	// The process restarts: a new Client from the persisted state.
	b := newApp("products:1")
	b.c = New(Config{Send: s.send, Listener: b, State: a.state})
	cell.Publish("products:1", 3, "")
	rounds(cell, 4, b.c)

	if s.handshakes != 1 || b.c.tok.Client != id {
		t.Fatalf("restart should resume %s without a handshake (handshakes=%d, id=%s)",
			id, s.handshakes, b.c.tok.Client)
	}
	if got := b.last("products:1"); got != 3 {
		t.Fatalf("restarted app missed v3: %v", b.notified)
	}
}

func TestDuplicatesAndOldVersionsAreFiltered(t *testing.T) {
	a := newApp()
	a.c = New(Config{Send: func(p.ClientMessage) (p.ServerMessage, error) {
		return p.ServerMessage{}, errors.New("offline")
	}, Listener: a})
	a.c.Register("x")
	a.c.OnPush([]p.Notification{{Object: "x", Version: 4}})
	a.c.OnPush([]p.Notification{{Object: "x", Version: 4}}) // retransmission
	a.c.OnPush([]p.Notification{{Object: "x", Version: 2}}) // reordered
	a.c.OnPush([]p.Notification{{Object: "y", Version: 1}}) // not registered
	if got := a.notified["x"]; len(got) != 1 || got[0] != 4 || len(a.notified["y"]) != 0 {
		t.Fatalf("want exactly one Notify(x, 4), got %v", a.notified)
	}
	// Every notification for a registered object is acked, even filtered
	// ones; y is not registered, so acking it could lose a signal we'd want
	// after a restart restores our intents.
	if len(a.c.acks) != 3 {
		t.Fatalf("want 3 acks (all of x, none of y), got %d", len(a.c.acks))
	}
}

func TestConvergesOverLossyNetwork(t *testing.T) {
	cell := server.NewCell("t", 2, 2, nil)
	rng := rand.New(rand.NewSource(1))
	lossy := func(m p.ClientMessage) (p.ServerMessage, error) {
		if rng.Float64() < 0.3 {
			return p.ServerMessage{}, errors.New("request lost")
		}
		resp := cell.Handle(m)
		if rng.Float64() < 0.3 { // the nasty case: server applied it, reply lost
			return p.ServerMessage{}, errors.New("reply lost")
		}
		return resp, nil
	}
	objs := []p.ObjectID{"a", "b", "c"}
	a := newApp(objs...)
	a.store, a.cache = map[p.ObjectID]p.Version{}, map[p.ObjectID]p.Version{}
	a.c = New(Config{Send: lossy, Listener: a})

	for i := 1; i <= 60; i++ {
		id := objs[rng.Intn(len(objs))]
		a.mu.Lock()
		a.store[id] = p.Version(i) // the write commits...
		a.mu.Unlock()
		cell.Publish(id, p.Version(i), "") // ...then the Bridge publishes it
		if i == 30 {
			cell.Wipe()
		}
		rounds(cell, 1, a.c)
	}
	rounds(cell, 30, a.c) // the network stays lossy; retries must get through
	for _, id := range objs {
		if a.cache[id] != a.store[id] {
			t.Errorf("object %s: app cached v%d, latest is v%d", id, a.cache[id], a.store[id])
		}
	}
}

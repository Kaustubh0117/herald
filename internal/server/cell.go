package server

import (
	"fmt"
	"sync"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// Cell is one complete Herald deployment (the paper's "data center"):
// N Registrars and M Matchers in one process, routed by protocol.Owner.
type Cell struct {
	// Fail, if set, is consulted before every inter-component call and
	// fails the call when it returns true. Set it before starting any
	// goroutine that calls Step; it is not guarded by a lock.
	Fail func() bool

	name         string
	nReg, nMatch int
	push         PushFunc

	mu         sync.Mutex
	epoch      uint64
	nextID     uint64
	registrars []*Registrar
	matchers   []*Matcher
}

func NewCell(name string, registrars, matchers int, push PushFunc) *Cell {
	c := &Cell{name: name, nReg: registrars, nMatch: matchers, push: push}
	c.build(1)
	return c
}

// build replaces every server with an empty one under the given epoch.
// The route closures capture this generation's slices, so a propagator
// still running on an old generation only ever touches old servers.
// Caller holds c.mu (or is the constructor).
func (c *Cell) build(epoch uint64) {
	regs := make([]*Registrar, c.nReg)
	ms := make([]*Matcher, c.nMatch)
	routeR := func(id p.ClientID) RegistrarAPI {
		return faultyRegistrar{c, regs[p.Owner(string(id), len(regs))]}
	}
	routeM := func(id p.ObjectID) MatcherAPI {
		return faultyMatcher{c, ms[p.Owner(string(id), len(ms))]}
	}
	for i := range regs {
		regs[i] = NewRegistrar(c.name, epoch, routeM, c.push)
	}
	for i := range ms {
		ms[i] = NewMatcher(routeR)
	}
	c.epoch, c.nextID = epoch, 0
	c.registrars, c.matchers = regs, ms
}

// Wipe throws away ALL server state, like the paper's brute-force recovery
// (Sec. 4.1.5): every server restarts empty under a new epoch. Correctness
// must survive this -- that is the central claim we test.
func (c *Cell) Wipe() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.build(c.epoch + 1)
}

func (c *Cell) Epoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// Handle routes a client message to the client's Registrar. A handshake
// (nil token) gets a freshly minted client ID; including the epoch keeps
// IDs unique across wipes even though the counter restarts.
func (c *Cell) Handle(m p.ClientMessage) p.ServerMessage {
	c.mu.Lock()
	if m.Token == nil {
		c.nextID++
		id := p.ClientID(fmt.Sprintf("%s.%d.%d", c.name, c.epoch, c.nextID))
		tok := p.Token{Client: id, Cell: c.name, Epoch: c.epoch}
		c.mu.Unlock()
		return p.ServerMessage{NewToken: &tok, Nonce: m.Nonce}
	}
	r := c.registrars[p.Owner(string(m.Token.Client), len(c.registrars))]
	c.mu.Unlock()
	return r.Handle(m)
}

// Publish is the Bridge's entry point. It can fail (fault injection now,
// a network error later); the caller retries until it succeeds.
func (c *Cell) Publish(id p.ObjectID, v p.Version, source p.ClientID) error {
	c.mu.Lock()
	m := c.matchers[p.Owner(string(id), len(c.matchers))]
	c.mu.Unlock()
	return faultyMatcher{c, m}.Publish(id, v, source)
}

// Step runs one pass of every propagator. Tests call it directly for
// deterministic control; servers call it on a timer.
func (c *Cell) Step() {
	c.mu.Lock()
	regs, ms := c.registrars, c.matchers
	c.mu.Unlock()
	for _, r := range regs {
		r.Propagate()
	}
	for _, m := range ms {
		m.Propagate()
	}
}

func (c *Cell) fail() bool { return c.Fail != nil && c.Fail() }

// faultyMatcher and faultyRegistrar wrap every inter-component call in the
// Cell's fault injector, standing in for a lossy network between servers.
type faultyMatcher struct {
	c *Cell
	m *Matcher
}

func (f faultyMatcher) Publish(id p.ObjectID, v p.Version, src p.ClientID) error {
	if f.c.fail() {
		return ErrInjected
	}
	return f.m.Publish(id, v, src)
}

func (f faultyMatcher) AddRegistration(id p.ObjectID, cl p.ClientID) error {
	if f.c.fail() {
		return ErrInjected
	}
	return f.m.AddRegistration(id, cl)
}

func (f faultyMatcher) RemoveRegistration(id p.ObjectID, cl p.ClientID) error {
	if f.c.fail() {
		return ErrInjected
	}
	return f.m.RemoveRegistration(id, cl)
}

func (f faultyMatcher) LatestVersion(id p.ObjectID) (p.Version, bool, error) {
	if f.c.fail() {
		return 0, false, ErrInjected
	}
	return f.m.LatestVersion(id)
}

type faultyRegistrar struct {
	c *Cell
	r *Registrar
}

func (f faultyRegistrar) Handle(m p.ClientMessage) p.ServerMessage { return f.r.Handle(m) }

func (f faultyRegistrar) Deliver(cl p.ClientID, n p.Notification) error {
	if f.c.fail() {
		return ErrInjected
	}
	return f.r.Deliver(cl, n)
}

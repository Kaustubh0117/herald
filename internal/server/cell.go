package server

import (
	"fmt"
	"sync"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// Cell is one complete Herald deployment (the paper's "data center"):
// N Registrars and M Matchers in one process, routed by protocol.Owner.
// Servers reach each other through slot wrappers that look up whichever
// instance currently occupies a slot, so a single server can be restarted
// (RestartMatcher, RestartRegistrar) the way a crashed process would be.
type Cell struct {
	// Fail, if set, is consulted before every inter-component call and
	// fails the call when it returns true. Set it before starting any
	// goroutine that calls Step; it is not guarded by a lock.
	Fail func() bool

	name string
	push PushFunc
	regs []RegistrarAPI // slot wrappers, fixed for the Cell's lifetime
	ms   []MatcherAPI

	mu         sync.Mutex
	lastEpoch  uint64 // every server incarnation gets a fresh epoch
	nextID     uint64
	registrars []*Registrar
	matchers   []*Matcher
}

func NewCell(name string, registrars, matchers int, push PushFunc) *Cell {
	c := &Cell{
		name:       name,
		push:       push,
		registrars: make([]*Registrar, registrars),
		matchers:   make([]*Matcher, matchers),
	}
	for i := range c.registrars {
		c.regs = append(c.regs, registrarSlot{c, i})
	}
	for i := range c.matchers {
		c.ms = append(c.ms, matcherSlot{c, i})
	}
	c.Wipe()
	return c
}

// newRegistrar and newMatcher create an empty server under a new epoch.
// Caller holds c.mu.
func (c *Cell) newRegistrar() *Registrar {
	c.lastEpoch++
	return NewRegistrar(c.name, c.lastEpoch, c.ms, c.push)
}

func (c *Cell) newMatcher() *Matcher {
	c.lastEpoch++
	return NewMatcher(c.lastEpoch, c.regs)
}

// Wipe throws away ALL server state, like the paper's brute-force recovery
// (Sec. 4.1.5): every server restarts empty under a new epoch. Correctness
// must survive this -- that is the central claim we test.
func (c *Cell) Wipe() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.registrars {
		c.registrars[i] = c.newRegistrar()
	}
	for i := range c.matchers {
		c.matchers[i] = c.newMatcher()
	}
}

// RestartMatcher replaces one Matcher with an empty one, like a crashed
// process coming back without its in-memory state.
func (c *Cell) RestartMatcher(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.matchers[i] = c.newMatcher()
}

func (c *Cell) RestartRegistrar(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registrars[i] = c.newRegistrar()
}

func (c *Cell) NumMatchers() int   { return len(c.ms) }
func (c *Cell) NumRegistrars() int { return len(c.regs) }

func (c *Cell) matcher(i int) *Matcher {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.matchers[i]
}

func (c *Cell) registrar(i int) *Registrar {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.registrars[i]
}

// Handle routes a client message to the client's Registrar. A handshake
// (nil token) gets a freshly minted client ID, passed on as a token with no
// cell, which the Registrar treats like any stale token: it grants a real
// one for the same ID under its own epoch.
func (c *Cell) Handle(m p.ClientMessage) p.ServerMessage {
	if m.Token == nil {
		c.mu.Lock()
		c.nextID++
		id := p.ClientID(fmt.Sprintf("%s.%d", c.name, c.nextID))
		c.mu.Unlock()
		m.Token = &p.Token{Client: id}
	}
	return c.registrar(p.Owner(string(m.Token.Client), len(c.regs))).Handle(m)
}

// Publish is the Bridge's entry point. It can fail (fault injection now,
// a network error later); the caller retries until it succeeds.
func (c *Cell) Publish(id p.ObjectID, v p.Version, source p.ClientID) error {
	return c.ms[p.Owner(string(id), len(c.ms))].Publish(id, v, source)
}

// Step runs one pass of every propagator. Tests call it directly for
// deterministic control; servers call it on a timer.
func (c *Cell) Step() {
	c.mu.Lock()
	regs := append([]*Registrar(nil), c.registrars...)
	ms := append([]*Matcher(nil), c.matchers...)
	c.mu.Unlock()
	for _, r := range regs {
		r.Propagate()
	}
	for _, m := range ms {
		m.Propagate()
	}
}

func (c *Cell) fail() bool { return c.Fail != nil && c.Fail() }

// matcherSlot and registrarSlot call whichever server currently occupies
// slot i, through the Cell's fault injector: they stand in for an RPC
// client and a lossy network between servers.
type matcherSlot struct {
	c *Cell
	i int
}

func (s matcherSlot) Publish(id p.ObjectID, v p.Version, src p.ClientID) error {
	if s.c.fail() {
		return ErrInjected
	}
	return s.c.matcher(s.i).Publish(id, v, src)
}

func (s matcherSlot) AddRegistration(id p.ObjectID, cl p.ClientID) error {
	if s.c.fail() {
		return ErrInjected
	}
	return s.c.matcher(s.i).AddRegistration(id, cl)
}

func (s matcherSlot) RemoveRegistration(id p.ObjectID, cl p.ClientID) error {
	if s.c.fail() {
		return ErrInjected
	}
	return s.c.matcher(s.i).RemoveRegistration(id, cl)
}

func (s matcherSlot) LatestVersion(id p.ObjectID) (p.Version, bool, error) {
	if s.c.fail() {
		return 0, false, ErrInjected
	}
	return s.c.matcher(s.i).LatestVersion(id)
}

func (s matcherSlot) Epoch() (uint64, error) {
	if s.c.fail() {
		return 0, ErrInjected
	}
	return s.c.matcher(s.i).Epoch()
}

type registrarSlot struct {
	c *Cell
	i int
}

func (s registrarSlot) Deliver(cl p.ClientID, n p.Notification) error {
	if s.c.fail() {
		return ErrInjected
	}
	return s.c.registrar(s.i).Deliver(cl, n)
}

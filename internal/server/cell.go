package server

import (
	"fmt"
	"sync"
	"time"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// Cell is one complete Herald deployment (the paper's "data center"):
// N Registrars and M Matchers in one process, routed by protocol.Owner.
// Servers reach each other through slot wrappers that look up whichever
// instance currently occupies a slot, so a single server can be restarted
// (RestartMatcher, RestartRegistrar) the way a crashed process would be.
//
// Without a store, a restarted server comes back empty under a new epoch.
// With one, it reloads its key range and keeps the stored state's epoch, so
// nobody has to re-sync; only Wipe changes the epoch.
type Cell struct {
	// Fail, if set, is consulted before every inter-component call and
	// fails the call when it returns true. Set it before starting any
	// goroutine that calls Step; it is not guarded by a lock.
	Fail func() bool

	name  string
	push  PushFunc
	store Store          // nil: memory only
	regs  []RegistrarAPI // slot wrappers, fixed for the Cell's lifetime
	ms    []MatcherAPI

	mu         sync.Mutex
	now        func() time.Time
	lastEpoch  uint64 // memory only: every server incarnation gets a fresh epoch
	nextID     uint64
	registrars []*Registrar
	matchers   []*Matcher
}

func NewCell(name string, registrars, matchers int, push PushFunc) *Cell {
	c, _ := NewCellWithStore(name, registrars, matchers, push, nil) // can't fail without a store
	return c
}

// NewCellWithStore builds a cell whose servers persist their state in st
// and reload it on restart.
func NewCellWithStore(name string, registrars, matchers int, push PushFunc, st Store) (*Cell, error) {
	c := &Cell{
		name:       name,
		push:       push,
		store:      st,
		now:        time.Now,
		registrars: make([]*Registrar, registrars),
		matchers:   make([]*Matcher, matchers),
	}
	for i := range c.registrars {
		c.regs = append(c.regs, registrarSlot{c, i})
	}
	for i := range c.matchers {
		c.ms = append(c.ms, matcherSlot{c, i})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c, c.startAll()
}

// SetClock replaces the clock Registrars use for garbage collection, so a
// simulation can age clients without waiting.
func (c *Cell) SetClock(now func() time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
	for _, r := range c.registrars {
		r.now = now
	}
}

// epochFor picks the epoch for a server about to start. Caller holds c.mu.
func (c *Cell) epochFor() (uint64, error) {
	if c.store != nil {
		return c.store.Epoch()
	}
	c.lastEpoch++
	return c.lastEpoch, nil
}

// newRegistrar and newMatcher start server i: from the store if there is
// one, otherwise empty under a fresh epoch. Caller holds c.mu.
func (c *Cell) newRegistrar(i int) (*Registrar, error) {
	e, err := c.epochFor()
	if err != nil {
		return nil, err
	}
	if c.store != nil {
		return LoadRegistrar(c.name, e, c.ms, c.push, c.store, i, len(c.regs), c.now)
	}
	r := NewRegistrar(c.name, e, c.ms, c.push)
	r.now = c.now
	return r, nil
}

func (c *Cell) newMatcher(i int) (*Matcher, error) {
	e, err := c.epochFor()
	if err != nil {
		return nil, err
	}
	if c.store != nil {
		return LoadMatcher(e, c.regs, c.store, i, len(c.ms))
	}
	return NewMatcher(e, c.regs), nil
}

// startAll (re)starts every server. Caller holds c.mu.
func (c *Cell) startAll() error {
	for i := range c.registrars {
		r, err := c.newRegistrar(i)
		if err != nil {
			return err
		}
		c.registrars[i] = r
	}
	for i := range c.matchers {
		m, err := c.newMatcher(i)
		if err != nil {
			return err
		}
		c.matchers[i] = m
	}
	return nil
}

// Wipe throws away ALL server state, like the paper's brute-force recovery
// (Sec. 4.1.5): every server restarts empty under a new epoch. Correctness
// must survive this -- that is the central claim we test.
func (c *Cell) Wipe() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.store != nil {
		if _, err := c.store.Wipe(); err != nil {
			return err
		}
	}
	return c.startAll()
}

// RestartMatcher replaces one Matcher, like a crashed process coming back:
// empty without a store, reloaded with one.
func (c *Cell) RestartMatcher(i int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, err := c.newMatcher(i)
	if err != nil {
		return err
	}
	c.matchers[i] = m
	return nil
}

func (c *Cell) RestartRegistrar(i int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := c.newRegistrar(i)
	if err != nil {
		return err
	}
	c.registrars[i] = r
	return nil
}

// GC garbage-collects clients idle for longer than ttl on every Registrar.
func (c *Cell) GC(ttl time.Duration) error {
	c.mu.Lock()
	regs := append([]*Registrar(nil), c.registrars...)
	c.mu.Unlock()
	for _, r := range regs {
		if err := r.GC(ttl); err != nil {
			return err
		}
	}
	return nil
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
func (c *Cell) Handle(m p.ClientMessage) (p.ServerMessage, error) {
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

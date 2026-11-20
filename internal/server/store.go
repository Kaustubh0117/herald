package server

import (
	"errors"
	"fmt"
	"sync"
	"time"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// Persistent soft state (paper Sec. 4.2). Servers keep working copies in
// memory and write every change to a store BEFORE applying it in memory or
// acknowledging it. If the write fails, the operation fails and the caller
// retries; applying to memory first would make the retry look like a no-op
// and the change would never reach the store. A restarted server reloads its
// key range instead of making every client re-sync. The state is still soft:
// wiping the store loses nothing that clients can't rebuild.

// StoredObject is one Matcher row: an object's version and registrations.
type StoredObject struct {
	ID      p.ObjectID
	Version p.Version
	Known   bool // false for objects with registrations but no version yet
	Source  p.ClientID
	Pending bool // not yet delivered to every Registrar
	Clients []p.ClientID
}

// StoredReg is one registration in a Registrar's client view.
type StoredReg struct {
	Object  p.ObjectID
	Active  bool // false: an unregistration not yet copied to the Matcher
	Pending bool // not yet copied to the Matcher
}

// StoredClient is everything a Registrar keeps about one client.
type StoredClient struct {
	ID       p.ClientID
	Seq      uint64
	Addr     string
	LastSeen time.Time
	Regs     []StoredReg
	Pending  []p.Notification
}

// ScratchEntry is a version parked in the scratch table because its
// Matcher row was unwritable (paper Sec. 4.2.6).
type ScratchEntry struct {
	Slot    int
	Object  p.ObjectID
	Version p.Version
	Source  p.ClientID
}

type MatcherStore interface {
	SaveVersion(id p.ObjectID, v p.Version, source p.ClientID) error // and mark pending; never moves back
	ClearPending(id p.ObjectID, v p.Version) error                   // only if still at version v
	SaveMatcherReg(id p.ObjectID, c p.ClientID) error
	DeleteMatcherReg(id p.ObjectID, c p.ClientID) error
	LoadMatcher(i, n int) ([]StoredObject, error) // server i of n's key range

	// SaveScratch parks e in slot e.Slot, replacing an older version of the
	// same object. It reports false if the slot holds a different object.
	SaveScratch(e ScratchEntry) (bool, error)
	DeleteScratch(e ScratchEntry) error           // only if the slot still holds e's version
	LoadScratch(i, n int) ([]ScratchEntry, error) // entries for objects in server i of n's range
}

// ScratchSlots is the size of the scratch table. It also bounds what an
// outage can leave behind: one slot per object, however hot the object.
const ScratchSlots = 1024

// scratchAttempts is how many salts bufferToScratch tries before giving up
// (and failing the publish, which the Bridge then retries).
const scratchAttempts = 8

// ScratchSlot is the paper's "salted hash over the object ID, using the
// retry count as the salt". Successive versions of one object land in the
// same slot and overwrite each other, so a hot object uses one slot, not one
// row per update; a different salt usually lands on a different tablet.
func ScratchSlot(id p.ObjectID, attempt int) int {
	return p.Owner(fmt.Sprintf("%s#%d", id, attempt), ScratchSlots)
}

// bufferToScratch parks a version whose Matcher row could not be written,
// trying successive salts past slots that are taken or unwritable.
func bufferToScratch(st MatcherStore, id p.ObjectID, v p.Version, src p.ClientID) error {
	var err error
	for attempt := 0; attempt < scratchAttempts; attempt++ {
		ok, e := st.SaveScratch(ScratchEntry{ScratchSlot(id, attempt), id, v, src})
		if e == nil && ok {
			return nil
		}
		if e != nil {
			err = e
		}
	}
	if err == nil {
		err = errors.New("no free slot")
	}
	return fmt.Errorf("scratch %s: %w", id, err)
}

type RegistrarStore interface {
	SaveClient(c p.ClientID, seq uint64, addr string, lastSeen time.Time) error
	DeleteClient(c p.ClientID) error // with its registrations and notifications
	// SaveClientReg records a registration change and marks it pending.
	SaveClientReg(c p.ClientID, id p.ObjectID, active bool) error
	// ClearClientReg records that the change reached the Matcher: the
	// pending mark goes (or the row, for an unregistration). A no-op if a
	// later change already replaced it.
	ClearClientReg(c p.ClientID, id p.ObjectID, active bool) error
	SaveNotification(c p.ClientID, n p.Notification) error
	DeleteNotification(c p.ClientID, id p.ObjectID) error
	LoadRegistrar(i, n int) ([]StoredClient, error)
}

// Store is one cell's persistent state.
type Store interface {
	MatcherStore
	RegistrarStore
	Epoch() (uint64, error) // the stored state's incarnation, created on first use
	Wipe() (uint64, error)  // drop everything; returns a new, larger epoch
}

// ErrStoreUnavailable is returned by MemStore's injected write failures.
var ErrStoreUnavailable = errors.New("herald: store unavailable")

// MemStore is an in-process Store. It survives server restarts (the
// simulation's stand-in for Postgres) and can fail writes on demand.
type MemStore struct {
	// Fail, if set, is consulted before every write and fails it when it
	// returns true. Not guarded by a lock: set it before concurrent use.
	Fail func() bool

	mu      sync.Mutex
	epoch   uint64
	objects map[p.ObjectID]*StoredObject
	mregs   map[p.ObjectID]map[p.ClientID]bool
	clients map[p.ClientID]*StoredClient
	cregs   map[p.ClientID]map[p.ObjectID]StoredReg
	notifs  map[p.ClientID]map[p.ObjectID]p.Notification
	scratch map[int]ScratchEntry
}

func NewMemStore() *MemStore {
	s := &MemStore{}
	s.reset()
	return s
}

func (s *MemStore) reset() {
	s.objects = map[p.ObjectID]*StoredObject{}
	s.mregs = map[p.ObjectID]map[p.ClientID]bool{}
	s.clients = map[p.ClientID]*StoredClient{}
	s.cregs = map[p.ClientID]map[p.ObjectID]StoredReg{}
	s.notifs = map[p.ClientID]map[p.ObjectID]p.Notification{}
	s.scratch = map[int]ScratchEntry{}
}

// write locks the store and reports whether an injected failure hit.
func (s *MemStore) write() error {
	if s.Fail != nil && s.Fail() {
		return ErrStoreUnavailable
	}
	s.mu.Lock()
	return nil
}

func (s *MemStore) Epoch() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch == 0 {
		s.epoch = 1
	}
	return s.epoch, nil
}

func (s *MemStore) Wipe() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reset()
	s.epoch++
	return s.epoch, nil
}

func (s *MemStore) SaveVersion(id p.ObjectID, v p.Version, src p.ClientID) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if o, ok := s.objects[id]; ok && o.Version >= v {
		return nil
	}
	s.objects[id] = &StoredObject{ID: id, Version: v, Known: true, Source: src, Pending: true}
	return nil
}

func (s *MemStore) ClearPending(id p.ObjectID, v p.Version) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if o, ok := s.objects[id]; ok && o.Version == v {
		o.Pending = false
	}
	return nil
}

func (s *MemStore) SaveMatcherReg(id p.ObjectID, c p.ClientID) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.mregs[id] == nil {
		s.mregs[id] = map[p.ClientID]bool{}
	}
	s.mregs[id][c] = true
	return nil
}

func (s *MemStore) DeleteMatcherReg(id p.ObjectID, c p.ClientID) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	delete(s.mregs[id], c)
	return nil
}

func (s *MemStore) LoadMatcher(i, n int) ([]StoredObject, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byID := map[p.ObjectID]*StoredObject{}
	for id, o := range s.objects {
		if p.Owner(string(id), n) == i {
			cp := *o
			cp.Clients = nil
			byID[id] = &cp
		}
	}
	for id, cs := range s.mregs {
		if p.Owner(string(id), n) != i || len(cs) == 0 {
			continue
		}
		o := byID[id]
		if o == nil {
			o = &StoredObject{ID: id}
			byID[id] = o
		}
		for c := range cs {
			o.Clients = append(o.Clients, c)
		}
	}
	var out []StoredObject
	for _, o := range byID {
		out = append(out, *o)
	}
	return out, nil
}

func (s *MemStore) SaveScratch(e ScratchEntry) (bool, error) {
	if err := s.write(); err != nil {
		return false, err
	}
	defer s.mu.Unlock()
	cur, ok := s.scratch[e.Slot]
	if ok && cur.Object != e.Object {
		return false, nil
	}
	if !ok || cur.Version < e.Version {
		s.scratch[e.Slot] = e
	}
	return true, nil
}

func (s *MemStore) DeleteScratch(e ScratchEntry) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if cur, ok := s.scratch[e.Slot]; ok && cur.Object == e.Object && cur.Version == e.Version {
		delete(s.scratch, e.Slot)
	}
	return nil
}

func (s *MemStore) LoadScratch(i, n int) ([]ScratchEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ScratchEntry
	for _, e := range s.scratch {
		if p.Owner(string(e.Object), n) == i {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *MemStore) SaveClient(c p.ClientID, seq uint64, addr string, lastSeen time.Time) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	s.clients[c] = &StoredClient{ID: c, Seq: seq, Addr: addr, LastSeen: lastSeen}
	return nil
}

func (s *MemStore) DeleteClient(c p.ClientID) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	delete(s.clients, c)
	delete(s.cregs, c)
	delete(s.notifs, c)
	return nil
}

func (s *MemStore) SaveClientReg(c p.ClientID, id p.ObjectID, active bool) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.cregs[c] == nil {
		s.cregs[c] = map[p.ObjectID]StoredReg{}
	}
	s.cregs[c][id] = StoredReg{Object: id, Active: active, Pending: true}
	return nil
}

func (s *MemStore) ClearClientReg(c p.ClientID, id p.ObjectID, active bool) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	r, ok := s.cregs[c][id]
	if !ok || r.Active != active {
		return nil // a later change replaced it
	}
	if active {
		r.Pending = false
		s.cregs[c][id] = r
	} else {
		delete(s.cregs[c], id)
	}
	return nil
}

func (s *MemStore) SaveNotification(c p.ClientID, n p.Notification) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.notifs[c] == nil {
		s.notifs[c] = map[p.ObjectID]p.Notification{}
	}
	s.notifs[c][n.Object] = n
	return nil
}

func (s *MemStore) DeleteNotification(c p.ClientID, id p.ObjectID) error {
	if err := s.write(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	delete(s.notifs[c], id)
	return nil
}

func (s *MemStore) LoadRegistrar(i, n int) ([]StoredClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []StoredClient
	for c, row := range s.clients {
		if p.Owner(string(c), n) != i {
			continue
		}
		sc := *row
		for _, r := range s.cregs[c] {
			sc.Regs = append(sc.Regs, r)
		}
		for _, nt := range s.notifs[c] {
			sc.Pending = append(sc.Pending, nt)
		}
		out = append(out, sc)
	}
	return out, nil
}

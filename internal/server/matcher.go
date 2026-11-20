package server

import (
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/Kaustubh0117/herald/internal/metrics"
	"github.com/Kaustubh0117/herald/internal/mutate"
	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// objState is everything a Matcher knows about one object.
type objState struct {
	version p.Version
	known   bool       // false until the first publish (or after a wipe)
	source  p.ClientID // the writer, which is not notified of its own change
	stamps  p.Stamps   // stage times of the latest version (measurement only)
	// clients maps each registered client to a generation number that
	// changes on every AddRegistration, so dropping a stale registration
	// never removes one that was re-added meanwhile.
	clients map[p.ClientID]uint64
}

// Matcher owns a range of objects: their latest versions and who is
// registered for them (paper Sec. 4.1, the "object view").
type Matcher struct {
	epoch      uint64
	registrars []RegistrarAPI // routed by protocol.Owner(client)
	store      MatcherStore   // nil: memory only
	part       int            // with a store: this is Matcher part of parts,
	parts      int            // whose scratch entries Drain moves into place

	mu      sync.Mutex
	gen     uint64
	objects map[p.ObjectID]*objState
	pending map[p.ObjectID]bool // changed objects not yet pushed to every Registrar
}

func NewMatcher(epoch uint64, registrars []RegistrarAPI) *Matcher {
	return &Matcher{
		epoch:      epoch,
		registrars: registrars,
		objects:    map[p.ObjectID]*objState{},
		pending:    map[p.ObjectID]bool{},
	}
}

// LoadMatcher starts Matcher i of n from its key range in the store and
// resumes propagating whatever was still pending (paper Sec. 4.2.5).
func LoadMatcher(epoch uint64, registrars []RegistrarAPI, st MatcherStore, i, n int) (*Matcher, error) {
	m := NewMatcher(epoch, registrars)
	m.store, m.part, m.parts = st, i, n
	rows, err := st.LoadMatcher(i, n)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		o := m.obj(row.ID)
		o.version, o.known, o.source = row.Version, row.Known, row.Source
		for _, c := range row.Clients {
			m.gen++
			o.clients[c] = m.gen
		}
		if row.Pending {
			m.pending[row.ID] = true
		}
	}
	return m, nil
}

func (m *Matcher) route(c p.ClientID) RegistrarAPI {
	return m.registrars[p.Owner(string(c), len(m.registrars))]
}

func (m *Matcher) Epoch() (uint64, error) { return m.epoch, nil }

// obj returns the state for id, creating it if needed. Caller holds m.mu.
func (m *Matcher) obj(id p.ObjectID) *objState {
	st, ok := m.objects[id]
	if !ok {
		st = &objState{clients: map[p.ClientID]uint64{}}
		m.objects[id] = st
	}
	return st
}

// Publish records a new version. It is idempotent and only moves forward,
// so the Bridge can retry it blindly (paper: "idempotent operations only").
//
// If the object's row is unwritable (a partial storage outage), the version
// is parked in scratch instead and Publish still succeeds: the Bridge reads
// one ordered log, so a single failing object would otherwise stall every
// notification behind it. Drain moves the version into place later, and
// only then is it applied and propagated (paper Sec. 4.2.6).
func (m *Matcher) Publish(id p.ObjectID, v p.Version, source p.ClientID, stamps ...p.Stamps) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s p.Stamps
	if len(stamps) > 0 {
		s = stamps[0]
	}
	s.Stamp(p.StageMatcher, time.Now())
	err := m.apply(id, v, source, s)
	if err == nil || mutate.NoScratch {
		return err
	}
	if err := bufferToScratch(m.store, id, v, source); err != nil {
		return err
	}
	metrics.ScratchBuffered.Inc()
	return nil
}

// apply writes a version to the store, then to memory. Caller holds m.mu.
func (m *Matcher) apply(id p.ObjectID, v p.Version, source p.ClientID, stamps p.Stamps) error {
	st := m.obj(id)
	if st.known && v <= st.version {
		return nil // duplicate or reordered publish: nothing to do
	}
	if m.store != nil {
		if err := m.store.SaveVersion(id, v, source); err != nil && !mutate.MemoryBeforeStore {
			return err
		}
	}
	st.version, st.known, st.source, st.stamps = v, true, source, stamps
	m.pending[id] = true
	return nil
}

// Drain moves versions parked in scratch into place once their rows are
// writable again. An entry leaves scratch only after its version is
// applied; if the slot was overwritten by a newer version meanwhile, the
// newer one stays for the next pass. Run it periodically.
func (m *Matcher) Drain() error {
	if m.store == nil || mutate.NoScratchDrain {
		return nil
	}
	es, err := m.store.LoadScratch(m.part, m.parts)
	if err != nil {
		return err
	}
	slices.SortFunc(es, func(a, b ScratchEntry) int { return a.Slot - b.Slot }) // deterministic for the sim
	for _, e := range es {
		m.mu.Lock()
		err := m.apply(e.Object, e.Version, e.Source, p.Stamps{}) // stamps aren't parked
		m.mu.Unlock()
		if err != nil {
			continue // still unwritable: next pass
		}
		if m.store.DeleteScratch(e) == nil {
			metrics.ScratchDrained.Inc()
		}
	}
	return nil
}

func (m *Matcher) AddRegistration(id p.ObjectID, c p.ClientID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.obj(id)
	if _, ok := st.clients[c]; !ok && m.store != nil {
		if err := m.store.SaveMatcherReg(id, c); err != nil {
			return err
		}
	}
	m.gen++
	st.clients[c] = m.gen
	return nil
}

func (m *Matcher) RemoveRegistration(id p.ObjectID, c p.ClientID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.remove(id, c)
}

// remove drops a registration. Caller holds m.mu.
func (m *Matcher) remove(id p.ObjectID, c p.ClientID) error {
	st, ok := m.objects[id]
	if !ok {
		return nil
	}
	if _, ok := st.clients[c]; ok && m.store != nil {
		if err := m.store.DeleteMatcherReg(id, c); err != nil {
			return err
		}
	}
	delete(st.clients, c)
	return nil
}

func (m *Matcher) LatestVersion(id p.ObjectID) (p.Version, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.objects[id]
	if !ok {
		return 0, false, nil
	}
	return st.version, st.known, nil
}

// Propagate is the Matcher Propagator: one pass over the pending set.
// An object leaves the pending set only after every Registrar accepted it
// (paper Fig. 5, steps 3-5). Run it in a loop in production.
func (m *Matcher) Propagate() {
	type job struct {
		id      p.ObjectID
		v       p.Version
		source  p.ClientID
		stamps  p.Stamps
		clients map[p.ClientID]uint64
	}
	// Snapshot under the lock, then call Registrars without holding it, so a
	// slow Registrar never blocks Publish.
	m.mu.Lock()
	var jobs []job
	for _, id := range slices.Sorted(maps.Keys(m.pending)) {
		st := m.objects[id]
		jobs = append(jobs, job{id, st.version, st.source, st.stamps, maps.Clone(st.clients)})
	}
	m.mu.Unlock()

	for _, j := range jobs {
		ok := true
		for _, c := range slices.Sorted(maps.Keys(j.clients)) {
			if c == j.source {
				continue // the writer already knows about its own change
			}
			n := p.Notification{Object: j.id, Version: j.v, Stamps: j.stamps}
			err := m.route(c).Deliver(c, n)
			if errors.Is(err, ErrNotRegistered) {
				// A stale registration (client collected or gone): drop it,
				// unless the client registered again while we were calling.
				m.mu.Lock()
				if m.objects[j.id].clients[c] == j.clients[c] {
					err = m.remove(j.id, c)
				} else {
					err = nil
				}
				m.mu.Unlock()
			}
			if err != nil {
				ok = false // keep it pending; retry on the next pass
			}
		}
		m.mu.Lock()
		// Only clear if no newer version arrived while we were propagating.
		if (ok || mutate.MatcherDropsFailed) && m.objects[j.id].version == j.v {
			if m.store == nil || m.store.ClearPending(j.id, j.v) == nil {
				delete(m.pending, j.id)
			}
		}
		m.mu.Unlock()
	}
}

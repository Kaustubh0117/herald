package server

import (
	"maps"
	"slices"
	"sync"

	"github.com/Kaustubh0117/herald/internal/mutate"
	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// objState is everything a Matcher knows about one object.
type objState struct {
	version p.Version
	known   bool       // false until the first publish (or after a wipe)
	source  p.ClientID // the writer, which is not notified of its own change
	clients map[p.ClientID]bool
}

// Matcher owns a range of objects: their latest versions and who is
// registered for them (paper Sec. 4.1, the "object view").
type Matcher struct {
	epoch      uint64
	registrars []RegistrarAPI // routed by protocol.Owner(client)

	mu      sync.Mutex
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

func (m *Matcher) route(c p.ClientID) RegistrarAPI {
	return m.registrars[p.Owner(string(c), len(m.registrars))]
}

func (m *Matcher) Epoch() (uint64, error) { return m.epoch, nil }

// obj returns the state for id, creating it if needed. Caller holds m.mu.
func (m *Matcher) obj(id p.ObjectID) *objState {
	st, ok := m.objects[id]
	if !ok {
		st = &objState{clients: map[p.ClientID]bool{}}
		m.objects[id] = st
	}
	return st
}

// Publish records a new version. It is idempotent and only moves forward,
// so the Bridge can retry it blindly (paper: "idempotent operations only").
func (m *Matcher) Publish(id p.ObjectID, v p.Version, source p.ClientID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.obj(id)
	if st.known && v <= st.version {
		return nil // duplicate or reordered publish: nothing to do
	}
	st.version, st.known, st.source = v, true, source
	m.pending[id] = true
	return nil
}

func (m *Matcher) AddRegistration(id p.ObjectID, c p.ClientID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.obj(id).clients[c] = true
	return nil
}

func (m *Matcher) RemoveRegistration(id p.ObjectID, c p.ClientID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st, ok := m.objects[id]; ok {
		delete(st.clients, c)
	}
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
		clients []p.ClientID
	}
	// Snapshot under the lock, then call Registrars without holding it, so a
	// slow Registrar never blocks Publish.
	m.mu.Lock()
	var jobs []job
	for _, id := range slices.Sorted(maps.Keys(m.pending)) {
		st := m.objects[id]
		jobs = append(jobs, job{id, st.version, st.source, slices.Sorted(maps.Keys(st.clients))})
	}
	m.mu.Unlock()

	for _, j := range jobs {
		ok := true
		for _, c := range j.clients {
			if c == j.source {
				continue // the writer already knows about its own change
			}
			n := p.Notification{Object: j.id, Version: j.v}
			if err := m.route(c).Deliver(c, n); err != nil {
				ok = false // keep it pending; retry on the next pass
			}
		}
		m.mu.Lock()
		// Only clear if no newer version arrived while we were propagating.
		if (ok || mutate.MatcherDropsFailed) && m.objects[j.id].version == j.v {
			delete(m.pending, j.id)
		}
		m.mu.Unlock()
	}
}

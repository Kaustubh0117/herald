package server

import (
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/Kaustubh0117/herald/internal/metrics"
	"github.com/Kaustubh0117/herald/internal/mutate"
	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// ErrNotRegistered is a Registrar's answer to a Deliver for a client it
// doesn't know (garbage-collected, or lost with a restart) or that isn't
// registered for the object. The Matcher drops that stale registration.
var ErrNotRegistered = errors.New("herald: client not registered for object")

// clientState is everything a Registrar knows about one client.
type clientState struct {
	regs     map[p.ObjectID]bool
	digest   p.Digest                      // always DigestOf(regs)
	pending  map[p.ObjectID]p.Notification // unacked, at most one per object
	seq      uint64                        // numbers unknown-version notifications
	addr     string                        // presence: gateway holding the socket
	lastSeen time.Time                     // for garbage collection

	// Confirmations for the next reply.
	confirmReg, confirmUnreg []p.ObjectID
}

// regOp is a registration change waiting to be copied to its Matcher.
type regOp struct {
	client p.ClientID
	obj    p.ObjectID
	add    bool
}

// Registrar owns a range of clients: their registrations and unacked
// notifications (paper Sec. 4.1, the "client view").
type Registrar struct {
	cell     string
	epoch    uint64
	matchers []MatcherAPI // routed by protocol.Owner(object)
	push     PushFunc
	store    RegistrarStore // nil: memory only
	now      func() time.Time

	mu           sync.Mutex
	clients      map[p.ClientID]*clientState
	ops          []regOp
	matcherEpoch []uint64 // last epoch seen per Matcher; 0 = not seen yet
}

func NewRegistrar(cell string, epoch uint64, matchers []MatcherAPI, push PushFunc) *Registrar {
	return &Registrar{
		cell:         cell,
		epoch:        epoch,
		matchers:     matchers,
		push:         push,
		now:          time.Now,
		clients:      map[p.ClientID]*clientState{},
		matcherEpoch: make([]uint64, len(matchers)),
	}
}

// LoadRegistrar starts Registrar i of n from its key range in the store:
// one range scan instead of every client re-syncing (paper Sec. 4.2.5).
// Registration changes that hadn't reached their Matcher are queued again.
func LoadRegistrar(cell string, epoch uint64, matchers []MatcherAPI, push PushFunc,
	st RegistrarStore, i, n int, now func() time.Time) (*Registrar, error) {
	r := NewRegistrar(cell, epoch, matchers, push)
	r.store = st
	if now != nil {
		r.now = now
	}
	rows, err := st.LoadRegistrar(i, n)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		cs := &clientState{
			regs:     map[p.ObjectID]bool{},
			pending:  map[p.ObjectID]p.Notification{},
			seq:      row.Seq,
			addr:     row.Addr,
			lastSeen: row.LastSeen,
		}
		for _, reg := range row.Regs {
			if reg.Active {
				cs.regs[reg.Object] = true
				cs.digest.Toggle(reg.Object)
			}
			if reg.Pending && !mutate.NoReplayPending {
				r.ops = append(r.ops, regOp{row.ID, reg.Object, reg.Active})
			}
		}
		for _, nt := range row.Pending {
			cs.pending[nt.Object] = nt
		}
		r.clients[row.ID] = cs
	}
	slices.SortFunc(r.ops, func(a, b regOp) int { // deterministic replay order
		if a.client != b.client {
			return compare(a.client, b.client)
		}
		return compare(a.obj, b.obj)
	})
	return r, nil
}

func compare[T ~string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func (r *Registrar) route(id p.ObjectID) MatcherAPI {
	return r.matchers[p.Owner(string(id), len(r.matchers))]
}

// saveClient writes the client's row. Caller holds r.mu.
func (r *Registrar) saveClient(c p.ClientID, st *clientState) error {
	if r.store == nil {
		return nil
	}
	return r.store.SaveClient(c, st.seq, st.addr, st.lastSeen)
}

// create makes and persists a new client's state. Caller holds r.mu.
//
// seq starts at a hash of (epoch, client), so a new incarnation's
// unknown-version notifications don't reuse an old one's numbers.
// Otherwise a delayed ack for a pre-wipe "unknown, seq 1" would cover (and
// silently clear) a post-wipe "unknown, seq 1" the client never saw. The
// hash is cut to 52 bits so it survives a round trip through a JavaScript
// number.
func (r *Registrar) create(c p.ClientID) (*clientState, error) {
	st := &clientState{
		regs:     map[p.ObjectID]bool{},
		pending:  map[p.ObjectID]p.Notification{},
		seq:      seqBase(r.epoch, c),
		lastSeen: r.now(),
	}
	if err := r.saveClient(c, st); err != nil {
		return nil, err
	}
	r.clients[c] = st
	return st, nil
}

// apply changes one registration and queues it for the Matcher. Repeating
// an operation is a no-op, which keeps duplicated client messages harmless.
// Caller holds r.mu.
func (r *Registrar) apply(c p.ClientID, st *clientState, id p.ObjectID, add bool) error {
	if st.regs[id] == add {
		return nil
	}
	if r.store != nil {
		if err := r.store.SaveClientReg(c, id, add); err != nil {
			return err
		}
		if !add {
			if err := r.store.DeleteNotification(c, id); err != nil {
				return err
			}
		}
	}
	st.digest.Toggle(id)
	if add {
		st.regs[id] = true
	} else {
		delete(st.regs, id)
		delete(st.pending, id) // the client no longer wants this signal
		st.confirmUnreg = append(st.confirmUnreg, id)
	}
	r.ops = append(r.ops, regOp{c, id, add})
	return nil
}

func seqBase(epoch uint64, c p.ClientID) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%d/%s", epoch, c)
	return h.Sum64() >> 12
}

// covers reports whether an ack from the client covers the pending
// notification cur, i.e. the client has seen cur or something newer.
func covers(a, cur p.Notification) bool {
	if cur.Unknown { // seqs aren't ordered across incarnations, so match exactly
		return a.Unknown && a.Seq == cur.Seq
	}
	return !a.Unknown && a.Version >= cur.Version
}

// offer stores n as the client's pending notification for its object unless
// a newer known version is already pending. Signals collapse to one per
// object: reliable *signaling*, not reliable messaging. Caller holds r.mu.
func (r *Registrar) offer(c p.ClientID, st *clientState, n p.Notification) (bool, error) {
	cur, ok := st.pending[n.Object]
	if ok && !cur.Unknown && !n.Unknown && cur.Version >= n.Version {
		return false, nil
	}
	if r.store != nil {
		if err := r.store.SaveNotification(c, n); err != nil && !mutate.MemoryBeforeStore {
			return false, err
		}
	}
	st.pending[n.Object] = n
	return true, nil
}

// Handle processes one client message and builds the reply. An error means
// a store write failed; the client treats that as a lost request and
// retries, which is safe because every step is idempotent.
func (r *Registrar) Handle(m p.ClientMessage) (p.ServerMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// The Cell turns a handshake (nil token) into a freshly minted token
	// before routing here, so a nil token means a confused caller.
	if m.Token == nil {
		return p.ServerMessage{}, errors.New("herald: registrar got a message without a token")
	}
	// A token minted by another cell / an older incarnation of this cell.
	// Reuse the client ID (paper Sec. 4.1.2); the empty digest in the reply
	// makes the client send its full set.
	if m.Token.Cell != r.cell || m.Token.Epoch != r.epoch {
		metrics.Resyncs.WithLabelValues("new_token").Inc()
		tok := p.Token{Client: m.Token.Client, Cell: r.cell, Epoch: r.epoch}
		st, ok := r.clients[tok.Client]
		if !ok {
			var err error
			if st, err = r.create(tok.Client); err != nil {
				return p.ServerMessage{}, err
			}
		}
		return p.ServerMessage{NewToken: &tok, Nonce: m.Nonce, Digest: st.digest}, nil
	}

	c := m.Token.Client
	st, ok := r.clients[c]
	if !ok {
		// A current token for a client we don't know: we garbage-collected
		// it (paper Sec. 4.2.4, a "zombie"). It must start over under a new
		// ID; the token carries no cell, so the new ID's own Registrar treats
		// it like a handshake. The new ID is derived, not random, to keep
		// simulations reproducible.
		metrics.Resyncs.WithLabelValues("zombie").Inc()
		fresh := p.ClientID(fmt.Sprintf("%s.z%d", c, r.epoch))
		return p.ServerMessage{NewToken: &p.Token{Client: fresh}, Nonce: m.Nonce}, nil
	}
	st.lastSeen = r.now()
	if m.Addr != "" {
		st.addr = m.Addr
	}
	if err := r.saveClient(c, st); err != nil {
		return p.ServerMessage{}, err
	}

	if m.FullSync {
		want := map[p.ObjectID]bool{}
		for _, id := range m.Register {
			want[id] = true
		}
		for _, id := range slices.Sorted(maps.Keys(st.regs)) {
			if !want[id] {
				if err := r.apply(c, st, id, false); err != nil {
					return p.ServerMessage{}, err
				}
			}
		}
		for _, id := range slices.Sorted(maps.Keys(want)) {
			if err := r.apply(c, st, id, true); err != nil {
				return p.ServerMessage{}, err
			}
		}
	} else {
		for _, id := range m.Unregister {
			if err := r.apply(c, st, id, false); err != nil {
				return p.ServerMessage{}, err
			}
		}
		for _, id := range m.Register {
			if err := r.apply(c, st, id, true); err != nil {
				return p.ServerMessage{}, err
			}
		}
	}

	for _, a := range m.Acks {
		if cur, ok := st.pending[a.Object]; ok && covers(a, cur) {
			if r.store != nil {
				if err := r.store.DeleteNotification(c, a.Object); err != nil {
					return p.ServerMessage{}, err
				}
			}
			delete(st.pending, a.Object)
		}
	}

	resp := p.ServerMessage{
		Digest:       st.digest,
		RequestSync:  m.Digest != st.digest,
		Registered:   st.confirmReg,
		Unregistered: st.confirmUnreg,
	}
	if resp.RequestSync {
		metrics.Resyncs.WithLabelValues("digest_mismatch").Inc()
	}
	st.confirmReg, st.confirmUnreg = nil, nil
	for _, id := range slices.Sorted(maps.Keys(st.pending)) { // retransmit everything unacked
		resp.Notifications = append(resp.Notifications, st.pending[id])
		if mutate.NoRetransmission {
			if r.store != nil {
				r.store.DeleteNotification(c, id)
			}
			delete(st.pending, id)
		}
	}
	return resp, nil
}

// PendingNotifications counts unacked notifications across all clients.
func (r *Registrar) PendingNotifications() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, st := range r.clients {
		n += len(st.pending)
	}
	return n
}

// Deliver is called by Matchers. It stores the notification as pending
// and tries the unreliable fast path.
func (r *Registrar) Deliver(c p.ClientID, n p.Notification) error {
	r.mu.Lock()
	st, ok := r.clients[c]
	if !ok || !st.regs[n.Object] {
		r.mu.Unlock()
		return ErrNotRegistered
	}
	stored, err := r.offer(c, st, n)
	if err != nil || !stored {
		r.mu.Unlock()
		return err
	}
	addr := st.addr
	r.mu.Unlock()

	if r.push != nil && addr != "" {
		r.push(addr, c, []p.Notification{n})
	}
	return nil
}

// GC deletes clients not heard from for longer than ttl (paper Sec. 4.2.4).
// A collected client that comes back is told to start over as a zombie.
// Its Matcher registrations go away the next time a Matcher delivers to it.
func (r *Registrar) GC(ttl time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := r.now().Add(-ttl)
	for _, c := range slices.Sorted(maps.Keys(r.clients)) {
		if !r.clients[c].lastSeen.Before(cutoff) {
			continue
		}
		if r.store != nil {
			if err := r.store.DeleteClient(c); err != nil {
				return err
			}
		}
		delete(r.clients, c) // its queued ops are skipped as stale
	}
	return nil
}

// checkMatchers re-sends registrations to any Matcher that restarted since
// we last looked: it came back empty and no longer knows who is registered
// for its objects. Post-propagation then gives each client the version the
// new Matcher has, or an unknown-version signal for the ones it lost.
// (With a store, Matchers reload their state and keep their epoch, so this
// only fires after a wipe or in memory-only mode.)
func (r *Registrar) checkMatchers() {
	for i, m := range r.matchers {
		e, err := m.Epoch()
		if err != nil {
			continue // unreachable now; we'll notice a restart next pass
		}
		r.mu.Lock()
		old := r.matcherEpoch[i]
		r.matcherEpoch[i] = e
		if old != 0 && old != e && !mutate.NoMatcherResync {
			for _, c := range slices.Sorted(maps.Keys(r.clients)) {
				for _, id := range slices.Sorted(maps.Keys(r.clients[c].regs)) {
					if p.Owner(string(id), len(r.matchers)) == i {
						r.ops = append(r.ops, regOp{c, id, true})
					}
				}
			}
		}
		r.mu.Unlock()
	}
}

// Propagate is the Registrar Propagator plus "post-propagation"
// (paper Fig. 4, steps 3-5). Failed ops stay queued and are retried.
func (r *Registrar) Propagate() {
	r.checkMatchers()
	r.mu.Lock()
	ops := r.ops
	r.ops = nil
	r.mu.Unlock()

	var retry []regOp
	for _, op := range ops {
		r.mu.Lock()
		st, ok := r.clients[op.client]
		stale := !ok || st.regs[op.obj] != op.add // a later op superseded this one
		r.mu.Unlock()
		if stale {
			continue
		}

		m := r.route(op.obj)
		if !op.add {
			if err := m.RemoveRegistration(op.obj, op.client); err != nil {
				retry = append(retry, op)
			} else if err := r.clearPending(op); err != nil {
				retry = append(retry, op)
			}
			continue
		}
		if err := m.AddRegistration(op.obj, op.client); err != nil {
			retry = append(retry, op)
			continue
		}
		v, known, err := m.LatestVersion(op.obj)
		if err != nil {
			retry = append(retry, op)
			continue
		}

		// Post-propagation: make sure the client learns the version that
		// existed when its registration reached the Matcher. offer (not a
		// plain write) matters: a newer version may have been Delivered
		// between LatestVersion and here, and must not be overwritten.
		if err := r.postPropagate(op, st, v, known); err != nil {
			retry = append(retry, op)
			continue
		}
		if err := r.clearPending(op); err != nil {
			retry = append(retry, op)
		}
	}

	r.mu.Lock()
	r.ops = append(retry, r.ops...)
	r.mu.Unlock()
}

func (r *Registrar) postPropagate(op regOp, st *clientState, v p.Version, known bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !st.regs[op.obj] || r.clients[op.client] != st {
		return nil
	}
	n := p.Notification{Object: op.obj, Version: v}
	if !known {
		// Persist the counter before using it: a seq reused after a crash
		// could let an old ack clear this new signal.
		st.seq++
		if err := r.saveClient(op.client, st); err != nil {
			st.seq--
			return err
		}
		n = p.Notification{Object: op.obj, Unknown: true, Seq: st.seq}
		metrics.UnknownNotifications.Inc()
	}
	if !mutate.NoPostPropagation {
		if _, err := r.offer(op.client, st, n); err != nil {
			return err
		}
	}
	st.confirmReg = append(st.confirmReg, op.obj)
	return nil
}

// clearPending records in the store that op reached its Matcher.
func (r *Registrar) clearPending(op regOp) error {
	if r.store == nil {
		return nil
	}
	return r.store.ClearClientReg(op.client, op.obj, op.add)
}

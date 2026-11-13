package server

import (
	"maps"
	"slices"
	"sync"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// clientState is everything a Registrar knows about one client.
type clientState struct {
	regs    map[p.ObjectID]bool
	digest  p.Digest                      // always DigestOf(regs)
	pending map[p.ObjectID]p.Notification // unacked, at most one per object
	seq     uint64                        // numbers unknown-version notifications
	addr    string                        // presence: gateway holding the socket

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
	cell  string
	epoch uint64
	route func(p.ObjectID) MatcherAPI
	push  PushFunc

	mu      sync.Mutex
	clients map[p.ClientID]*clientState
	ops     []regOp
}

func NewRegistrar(cell string, epoch uint64, route func(p.ObjectID) MatcherAPI, push PushFunc) *Registrar {
	return &Registrar{
		cell:    cell,
		epoch:   epoch,
		route:   route,
		push:    push,
		clients: map[p.ClientID]*clientState{},
	}
}

// state returns the client's state, creating it if needed. Caller holds r.mu.
func (r *Registrar) state(c p.ClientID) *clientState {
	st, ok := r.clients[c]
	if !ok {
		st = &clientState{
			regs:    map[p.ObjectID]bool{},
			pending: map[p.ObjectID]p.Notification{},
		}
		r.clients[c] = st
	}
	return st
}

// apply changes one registration and queues it for the Matcher. Repeating
// an operation is a no-op, which keeps duplicated client messages harmless.
// Caller holds r.mu.
func (r *Registrar) apply(c p.ClientID, st *clientState, id p.ObjectID, add bool) {
	if st.regs[id] == add {
		return
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
}

// covers reports whether an ack from the client covers the pending
// notification cur, i.e. the client has seen cur or something newer.
func covers(a, cur p.Notification) bool {
	if cur.Unknown {
		return a.Unknown && a.Seq >= cur.Seq
	}
	return !a.Unknown && a.Version >= cur.Version
}

// offer stores n as the client's pending notification for its object unless
// a newer known version is already pending. Signals collapse to one per
// object: reliable *signaling*, not reliable messaging. Caller holds r.mu.
func offer(st *clientState, n p.Notification) bool {
	cur, ok := st.pending[n.Object]
	if ok && !cur.Unknown && !n.Unknown && cur.Version >= n.Version {
		return false
	}
	st.pending[n.Object] = n
	return true
}

// Handle processes one client message and builds the reply.
func (r *Registrar) Handle(m p.ClientMessage) p.ServerMessage {
	r.mu.Lock()
	defer r.mu.Unlock()

	// The Cell turns a handshake (nil token) into a freshly minted token
	// before routing here, so a nil token means a confused caller.
	if m.Token == nil {
		return p.ServerMessage{}
	}
	// A token minted by another cell / an older incarnation of this cell.
	// Reuse the client ID (paper Sec. 4.1.2); the empty digest in the reply
	// makes the client send its full set.
	if m.Token.Cell != r.cell || m.Token.Epoch != r.epoch {
		tok := p.Token{Client: m.Token.Client, Cell: r.cell, Epoch: r.epoch}
		st := r.state(tok.Client)
		return p.ServerMessage{NewToken: &tok, Nonce: m.Nonce, Digest: st.digest}
	}

	c := m.Token.Client
	st := r.state(c)
	if m.Addr != "" {
		st.addr = m.Addr
	}

	if m.FullSync {
		want := map[p.ObjectID]bool{}
		for _, id := range m.Register {
			want[id] = true
		}
		for _, id := range slices.Sorted(maps.Keys(st.regs)) {
			if !want[id] {
				r.apply(c, st, id, false)
			}
		}
		for _, id := range slices.Sorted(maps.Keys(want)) {
			r.apply(c, st, id, true)
		}
	} else {
		for _, id := range m.Unregister {
			r.apply(c, st, id, false)
		}
		for _, id := range m.Register {
			r.apply(c, st, id, true)
		}
	}

	for _, a := range m.Acks {
		if cur, ok := st.pending[a.Object]; ok && covers(a, cur) {
			delete(st.pending, a.Object)
		}
	}

	resp := p.ServerMessage{
		Digest:       st.digest,
		RequestSync:  m.Digest != st.digest,
		Registered:   st.confirmReg,
		Unregistered: st.confirmUnreg,
	}
	st.confirmReg, st.confirmUnreg = nil, nil
	for _, id := range slices.Sorted(maps.Keys(st.pending)) { // retransmit everything unacked
		resp.Notifications = append(resp.Notifications, st.pending[id])
	}
	return resp
}

// Deliver is called by Matchers. It stores the notification as pending
// and tries the unreliable fast path.
func (r *Registrar) Deliver(c p.ClientID, n p.Notification) error {
	r.mu.Lock()
	st, ok := r.clients[c]
	if !ok || !st.regs[n.Object] {
		r.mu.Unlock()
		return nil // stale Matcher registration; the client will re-sync if needed
	}
	if !offer(st, n) {
		r.mu.Unlock()
		return nil
	}
	addr := st.addr
	r.mu.Unlock()

	if r.push != nil && addr != "" {
		r.push(addr, c, []p.Notification{n})
	}
	return nil
}

// Propagate is the Registrar Propagator plus "post-propagation"
// (paper Fig. 4, steps 3-5). Failed ops stay queued and are retried.
func (r *Registrar) Propagate() {
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
		r.mu.Lock()
		if st.regs[op.obj] {
			n := p.Notification{Object: op.obj, Version: v}
			if !known {
				st.seq++
				n = p.Notification{Object: op.obj, Unknown: true, Seq: st.seq}
			}
			offer(st, n)
			st.confirmReg = append(st.confirmReg, op.obj)
		}
		r.mu.Unlock()
	}

	r.mu.Lock()
	r.ops = append(retry, r.ops...)
	r.mu.Unlock()
}

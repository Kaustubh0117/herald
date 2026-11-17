// Package client is the Go client library (paper Fig. 2). Applications see
// only Register/Unregister and Notify/NotifyUnknown callbacks; tokens,
// retries, digests and failover stay inside. Following the paper's lessons
// (Sec. 8), it keeps the app's intents and turns every kind of failure into
// "refetch this object".
package client

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/Kaustubh0117/herald/internal/mutate"
	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// Listener is implemented by the application (paper: NotificationListener).
// Callbacks run on the goroutine that called Tick or OnPush, never while the
// client holds its lock, so they may call back into the Client.
type Listener interface {
	Notify(obj p.ObjectID, v p.Version)
	NotifyUnknown(obj p.ObjectID) // refetch regardless of cached version
	RegistrationStatusChanged(obj p.ObjectID, registered bool)
	ReissueRegistrations() // call Register() for everything you care about
	WriteState(state []byte)
}

// SendFunc delivers one message to the cell and returns its reply. Any
// error (lost request, lost reply, timeout) is treated the same: retry.
type SendFunc func(p.ClientMessage) (p.ServerMessage, error)

type Config struct {
	Send     SendFunc
	Listener Listener
	State    []byte // from a previous WriteState; nil for a brand-new client
	Addr     string // optional: where fast-path pushes reach us (gateways overwrite it)
}

type Client struct {
	send SendFunc
	l    Listener
	addr string

	mu       sync.Mutex
	tok      *p.Token
	nonce    uint64
	intents  map[p.ObjectID]bool // the source of truth
	digest   p.Digest            // always DigestOf(intents)
	toReg    map[p.ObjectID]bool // queued, not yet confirmed by a reply
	toUnreg  map[p.ObjectID]bool
	needFull bool
	reissue  bool
	acks     []p.Notification
	seen     map[p.ObjectID]p.Version // highest version delivered to the app
}

func New(cfg Config) *Client {
	c := &Client{
		send:     cfg.Send,
		l:        cfg.Listener,
		addr:     cfg.Addr,
		intents:  map[p.ObjectID]bool{},
		toReg:    map[p.ObjectID]bool{},
		toUnreg:  map[p.ObjectID]bool{},
		seen:     map[p.ObjectID]p.Version{},
		needFull: !mutate.NoRegistrationSync, // whatever the server has, our intents start empty
		reissue:  true,
	}
	if cfg.State != nil {
		var tok p.Token
		if json.Unmarshal(cfg.State, &tok) == nil && tok.Client != "" {
			c.tok = &tok
		}
	}
	return c
}

// Register records the intent immediately and queues it for the server.
func (c *Client) Register(id p.ObjectID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.intents[id] {
		return
	}
	c.intents[id] = true
	c.digest.Toggle(id)
	delete(c.toUnreg, id)
	c.toReg[id] = true
}

func (c *Client) Unregister(id p.ObjectID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.intents[id] {
		return
	}
	delete(c.intents, id)
	c.digest.Toggle(id)
	delete(c.toReg, id)
	c.toUnreg[id] = true
}

// Tick sends pending operations and acks; with nothing pending it is a
// heartbeat. Call it on a timer (the paper used 20 minutes; demos use
// seconds). The returned error is informational only: nothing was
// cleared, so everything is retried on the next Tick.
func (c *Client) Tick() error {
	c.mu.Lock()
	reissue := c.reissue
	c.reissue = false
	c.mu.Unlock()
	if reissue {
		c.l.ReissueRegistrations()
	}

	c.mu.Lock()
	m := p.ClientMessage{Token: c.tok, Digest: c.digest, Addr: c.addr}
	if c.tok == nil {
		c.nonce++
		m.Nonce = c.nonce
	} else if c.needFull {
		m.FullSync = true
		m.Register = slices.Sorted(maps.Keys(c.intents))
	} else {
		m.Register = slices.Sorted(maps.Keys(c.toReg))
		m.Unregister = slices.Sorted(maps.Keys(c.toUnreg))
	}
	m.Acks = slices.Clone(c.acks)
	c.mu.Unlock()

	resp, err := c.send(m)
	if err != nil {
		return err
	}
	c.handle(m, resp)
	return nil
}

// Run calls Tick every interval until ctx is cancelled.
func (c *Client) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		_ = c.Tick()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *Client) handle(sent p.ClientMessage, r p.ServerMessage) {
	c.mu.Lock()
	if r.NewToken != nil {
		if sent.Token == nil && r.Nonce != c.nonce {
			c.mu.Unlock()
			return // grant for an older handshake attempt: ignore
		}
		c.tok = r.NewToken
		c.needFull = !mutate.NoRegistrationSync // new server state: always reconcile
		state, _ := json.Marshal(c.tok)
		c.mu.Unlock()
		c.l.WriteState(state)
		return
	}

	// The server processed what we sent: clear exactly those items.
	if sent.FullSync {
		// The server now holds exactly sent.Register. Intents may have
		// changed since we built the message, so recompute what's left.
		c.needFull = false
		had := map[p.ObjectID]bool{}
		for _, id := range sent.Register {
			had[id] = true
		}
		c.toReg, c.toUnreg = map[p.ObjectID]bool{}, map[p.ObjectID]bool{}
		for id := range c.intents {
			if !had[id] {
				c.toReg[id] = true
			}
		}
		for id := range had {
			if !c.intents[id] {
				c.toUnreg[id] = true
			}
		}
	} else {
		for _, id := range sent.Register {
			delete(c.toReg, id)
		}
		for _, id := range sent.Unregister {
			delete(c.toUnreg, id)
		}
	}
	c.acks = c.acks[len(sent.Acks):]

	// Registration Sync: the server applied exactly what we sent, so its
	// digest must equal the one we sent (later local changes are still
	// queued and don't count as drift).
	if (r.RequestSync || r.Digest != sent.Digest) && !mutate.NoRegistrationSync {
		c.needFull = true
	}
	c.mu.Unlock()

	for _, id := range r.Registered {
		c.l.RegistrationStatusChanged(id, true)
	}
	for _, id := range r.Unregistered {
		c.l.RegistrationStatusChanged(id, false)
	}
	c.OnPush(r.Notifications)
}

// OnPush handles notifications from a reply or the fast path. Ack every
// notification for a registered object, deliver selectively: skip versions
// no newer than one already delivered. Unknown-version signals are
// always delivered; a duplicate only costs one extra refetch.
func (c *Client) OnPush(ns []p.Notification) {
	var deliver []p.Notification
	c.mu.Lock()
	for _, n := range ns {
		if mutate.AckUnregistered {
			c.acks = append(c.acks, n)
		}
		if !c.intents[n.Object] {
			// Don't ack what we can't deliver. A restarted process gets
			// pushes before ReissueRegistrations restores its intents; acking
			// those would clear the server's only copy of the signal. If we
			// truly don't want the object, unregistering drops it server-side.
			continue
		}
		if !mutate.AckUnregistered {
			c.acks = append(c.acks, n)
		}
		if !n.Unknown {
			if v, ok := c.seen[n.Object]; ok && n.Version <= v {
				continue
			}
			c.seen[n.Object] = n.Version
		}
		deliver = append(deliver, n)
	}
	c.mu.Unlock()

	for _, n := range deliver {
		if n.Unknown {
			c.l.NotifyUnknown(n.Object)
		} else {
			c.l.Notify(n.Object, n.Version)
		}
	}
}

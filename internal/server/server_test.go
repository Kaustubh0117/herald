package server

import (
	"maps"
	"slices"
	"testing"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// testClient speaks the protocol by hand, so these tests exercise the
// servers without depending on the client library (Phase 3).
type testClient struct {
	t    *testing.T
	cell *Cell
	tok  *p.Token
	regs map[p.ObjectID]bool
	addr string
}

func newTestClient(t *testing.T, c *Cell) *testClient {
	t.Helper()
	resp := c.Handle(p.ClientMessage{Nonce: 1})
	if resp.NewToken == nil {
		t.Fatal("handshake returned no token")
	}
	return &testClient{t: t, cell: c, tok: resp.NewToken, regs: map[p.ObjectID]bool{}}
}

func (tc *testClient) send(m p.ClientMessage) p.ServerMessage {
	m.Token, m.Digest, m.Addr = tc.tok, p.DigestOf(tc.regs), tc.addr
	return tc.cell.Handle(m)
}

func (tc *testClient) register(ids ...p.ObjectID) p.ServerMessage {
	for _, id := range ids {
		tc.regs[id] = true
	}
	return tc.send(p.ClientMessage{Register: ids})
}

func (tc *testClient) unregister(ids ...p.ObjectID) p.ServerMessage {
	for _, id := range ids {
		delete(tc.regs, id)
	}
	return tc.send(p.ClientMessage{Unregister: ids})
}

func (tc *testClient) fullSync() p.ServerMessage {
	return tc.send(p.ClientMessage{FullSync: true, Register: slices.Sorted(maps.Keys(tc.regs))})
}

func (tc *testClient) heartbeat() p.ServerMessage { return tc.send(p.ClientMessage{}) }

func (tc *testClient) ack(ns ...p.Notification) p.ServerMessage {
	return tc.send(p.ClientMessage{Acks: ns})
}

// settle runs the propagators enough times for everything to land.
func settle(c *Cell) {
	for i := 0; i < 5; i++ {
		c.Step()
	}
}

func pendingFor(resp p.ServerMessage, id p.ObjectID) (p.Notification, bool) {
	for _, n := range resp.Notifications {
		if n.Object == id {
			return n, true
		}
	}
	return p.Notification{}, false
}

func wantVersion(t *testing.T, resp p.ServerMessage, id p.ObjectID, v p.Version) p.Notification {
	t.Helper()
	n, ok := pendingFor(resp, id)
	if !ok || n.Unknown || n.Version != v {
		t.Fatalf("want pending %s@v%d, got %+v (ok=%v)", id, v, n, ok)
	}
	return n
}

func TestRegisterThenPublish(t *testing.T) {
	c := NewCell("t", 2, 2, nil)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	if err := c.Publish("products:1", 7, ""); err != nil {
		t.Fatal(err)
	}
	settle(c)
	wantVersion(t, tc.heartbeat(), "products:1", 7)
}

func TestPublishThenRegister(t *testing.T) {
	c := NewCell("t", 2, 2, nil)
	if err := c.Publish("products:1", 7, ""); err != nil {
		t.Fatal(err)
	}
	settle(c)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c) // post-propagation writes the pending notification
	resp := tc.heartbeat()
	wantVersion(t, resp, "products:1", 7)
	if !slices.Contains(resp.Registered, "products:1") {
		t.Fatalf("registration not confirmed: %+v", resp)
	}
}

func TestRegisterBeforeAnyPublishIsUnknown(t *testing.T) {
	c := NewCell("t", 2, 2, nil)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	n, ok := pendingFor(tc.heartbeat(), "products:1")
	if !ok || !n.Unknown {
		t.Fatalf("want unknown-version notification, got %+v (ok=%v)", n, ok)
	}
}

func TestDuplicatesChangeNothing(t *testing.T) {
	c := NewCell("t", 2, 2, nil)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	c.Publish("products:1", 7, "")
	settle(c)
	n := wantVersion(t, tc.heartbeat(), "products:1", 7)

	// Replayed and reordered publishes don't change the pending version.
	c.Publish("products:1", 7, "")
	c.Publish("products:1", 5, "")
	settle(c)
	wantVersion(t, tc.heartbeat(), "products:1", 7)

	// Ack it, twice. Then replay the publish: nothing new becomes pending.
	tc.ack(n)
	tc.ack(n)
	c.Publish("products:1", 7, "")
	settle(c)
	if resp := tc.heartbeat(); len(resp.Notifications) != 0 {
		t.Fatalf("acked notification came back: %+v", resp.Notifications)
	}

	// A duplicated registration is a no-op: same digest, no new signal.
	resp := tc.register("products:1")
	settle(c)
	if resp.RequestSync || resp.Digest != p.DigestOf(tc.regs) {
		t.Fatalf("duplicate registration disturbed the digest: %+v", resp)
	}
	if resp := tc.heartbeat(); len(resp.Notifications) != 0 {
		t.Fatalf("duplicate registration produced %+v", resp.Notifications)
	}
}

func TestStaleAckDoesNotClearNewerVersion(t *testing.T) {
	c := NewCell("t", 2, 2, nil)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	c.Publish("products:1", 7, "")
	settle(c)
	old := wantVersion(t, tc.heartbeat(), "products:1", 7)
	c.Publish("products:1", 9, "")
	settle(c)
	tc.ack(old) // the client only saw v7
	wantVersion(t, tc.heartbeat(), "products:1", 9)
}

func TestWriterIsNotNotifiedOfOwnChange(t *testing.T) {
	c := NewCell("t", 2, 2, nil)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	n, _ := pendingFor(tc.heartbeat(), "products:1")
	tc.ack(n) // clear the initial unknown-version signal
	c.Publish("products:1", 3, tc.tok.Client)
	settle(c)
	if resp := tc.heartbeat(); len(resp.Notifications) != 0 {
		t.Fatalf("writer got its own change: %+v", resp.Notifications)
	}
}

func TestDigestMismatchRequestsSync(t *testing.T) {
	c := NewCell("t", 2, 2, nil)
	tc := newTestClient(t, c)
	tc.regs["products:1"] = true // client thinks it registered; server never heard
	if resp := tc.heartbeat(); !resp.RequestSync {
		t.Fatalf("want RequestSync, got %+v", resp)
	}
	if resp := tc.fullSync(); resp.RequestSync {
		t.Fatalf("full sync should reconcile digests, got %+v", resp)
	}
}

func TestWipeRecoversThroughClient(t *testing.T) {
	c := NewCell("t", 2, 2, nil)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	c.Publish("products:1", 4, "")
	settle(c)
	tc.ack(wantVersion(t, tc.heartbeat(), "products:1", 4))

	c.Wipe()

	// The old token is stale: same client ID, new epoch, empty digest.
	resp := tc.heartbeat()
	if resp.NewToken == nil || resp.NewToken.Client != tc.tok.Client || resp.NewToken.Epoch == tc.tok.Epoch {
		t.Fatalf("want a fresh token for the same client, got %+v", resp)
	}
	if resp.Digest == p.DigestOf(tc.regs) {
		t.Fatal("wiped server should not know the client's registrations")
	}
	tc.tok = resp.NewToken
	tc.fullSync()
	settle(c)

	// The version was lost with the wipe, so the client must refetch.
	n, ok := pendingFor(tc.heartbeat(), "products:1")
	if !ok || !n.Unknown {
		t.Fatalf("want unknown-version notification after wipe, got %+v", n)
	}
	c.Publish("products:1", 5, "")
	settle(c)
	wantVersion(t, tc.heartbeat(), "products:1", 5)
}

func TestPreWipeAckDoesNotClearPostWipeUnknown(t *testing.T) {
	c := NewCell("t", 1, 1, nil)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	old, _ := pendingFor(tc.heartbeat(), "products:1") // unknown, from epoch 1

	c.Wipe()
	tc.tok = tc.heartbeat().NewToken
	tc.fullSync()
	settle(c)
	tc.ack(old) // delayed ack from before the wipe
	if n, ok := pendingFor(tc.heartbeat(), "products:1"); !ok || !n.Unknown {
		t.Fatalf("stale ack cleared the post-wipe unknown signal: %+v ok=%v", n, ok)
	}
}

func TestMatcherRestartTriggersResync(t *testing.T) {
	c := NewCell("t", 1, 1, nil)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	c.Publish("products:1", 4, "")
	settle(c)
	tc.ack(wantVersion(t, tc.heartbeat(), "products:1", 4))

	// The Matcher crashes and comes back empty: it no longer knows the
	// client is registered, and it lost v4.
	c.RestartMatcher(0)
	settle(c)
	n, ok := pendingFor(tc.heartbeat(), "products:1")
	if !ok || !n.Unknown {
		t.Fatalf("want an unknown-version signal after the Matcher lost state, got %+v ok=%v", n, ok)
	}
	tc.ack(n)
	c.Publish("products:1", 6, "")
	settle(c)
	wantVersion(t, tc.heartbeat(), "products:1", 6) // the registration was re-sent
}

func TestRegistrarRestartIsAStaleToken(t *testing.T) {
	c := NewCell("t", 1, 1, nil)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	c.RestartRegistrar(0)
	resp := tc.heartbeat()
	if resp.NewToken == nil || resp.NewToken.Client != tc.tok.Client {
		t.Fatalf("want a fresh token for the same client, got %+v", resp)
	}
	tc.tok = resp.NewToken
	tc.fullSync()
	settle(c)
	c.Publish("products:1", 2, "")
	settle(c)
	wantVersion(t, tc.heartbeat(), "products:1", 2)
}

func TestFastPathPush(t *testing.T) {
	var pushed []p.Notification
	c := NewCell("t", 2, 2, func(addr string, _ p.ClientID, ns []p.Notification) {
		if addr == "gw-1" {
			pushed = append(pushed, ns...)
		}
	})
	tc := newTestClient(t, c)
	tc.addr = "gw-1"
	tc.register("products:1")
	settle(c)
	c.Publish("products:1", 7, "")
	settle(c)
	if len(pushed) != 1 || pushed[0].Version != 7 {
		t.Fatalf("want one fast-path push of v7, got %+v", pushed)
	}
}

func TestFailedCallsAreRetried(t *testing.T) {
	c := NewCell("t", 2, 2, nil)
	c.Fail = func() bool { return true }
	if err := c.Publish("products:1", 7, ""); err == nil {
		t.Fatal("publish should fail while faults are injected")
	}
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	if resp := tc.heartbeat(); len(resp.Registered) != 0 || len(resp.Notifications) != 0 {
		t.Fatalf("nothing should propagate while faulty: %+v", resp)
	}

	c.Fail = nil
	if err := c.Publish("products:1", 7, ""); err != nil {
		t.Fatal(err)
	}
	settle(c)
	resp := tc.heartbeat()
	wantVersion(t, resp, "products:1", 7)
	if !slices.Contains(resp.Registered, "products:1") {
		t.Fatalf("registration not confirmed after recovery: %+v", resp)
	}
}

func TestOrderingTrap(t *testing.T) {
	// Register fails to propagate, then the client unregisters. When the
	// network heals, the stale register must not reach the Matcher.
	c := NewCell("t", 1, 1, nil)
	tc := newTestClient(t, c)
	c.Fail = func() bool { return true }
	tc.register("products:1")
	settle(c)
	tc.unregister("products:1")
	c.Fail = nil
	settle(c)
	if st := c.matcher(0).objects["products:1"]; st != nil && st.clients[tc.tok.Client] {
		t.Fatal("Matcher kept a registration the client withdrew")
	}
}

package server

import (
	"testing"
	"time"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

func storeCell(t *testing.T) (*Cell, *MemStore) {
	t.Helper()
	st := NewMemStore()
	c, err := NewCellWithStore("t", 2, 2, nil, st)
	if err != nil {
		t.Fatal(err)
	}
	return c, st
}

func TestRestartWithStoreNeedsNoResync(t *testing.T) {
	c, _ := storeCell(t)
	tc := newTestClient(t, c)
	tc.register("products:1", "products:2")
	settle(c)
	c.Publish("products:1", 4, "")
	settle(c)
	resp := tc.heartbeat()
	for _, n := range resp.Notifications {
		tc.ack(n)
	}

	// Every server crashes and reloads its key range from the store.
	for i := 0; i < c.NumRegistrars(); i++ {
		if err := c.RestartRegistrar(i); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < c.NumMatchers(); i++ {
		if err := c.RestartMatcher(i); err != nil {
			t.Fatal(err)
		}
	}
	resp = tc.heartbeat()
	if resp.NewToken != nil || resp.RequestSync {
		t.Fatalf("a restart with a store should be invisible to the client, got %+v", resp)
	}
	if len(resp.Notifications) != 0 {
		t.Fatalf("acked notifications came back after restart: %+v", resp.Notifications)
	}
	c.Publish("products:2", 9, "") // the reloaded Matcher still knows who is registered
	settle(c)
	wantVersion(t, tc.heartbeat(), "products:2", 9)
}

func TestUnackedNotificationSurvivesRestart(t *testing.T) {
	c, _ := storeCell(t)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	c.Publish("products:1", 4, "")
	settle(c)
	for i := 0; i < c.NumRegistrars(); i++ {
		c.RestartRegistrar(i)
	}
	wantVersion(t, tc.heartbeat(), "products:1", 4) // still retransmitted
}

func TestPendingPublishSurvivesMatcherRestart(t *testing.T) {
	c, _ := storeCell(t)
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	c.Publish("products:1", 4, "") // accepted, but not yet propagated
	for i := 0; i < c.NumMatchers(); i++ {
		c.RestartMatcher(i)
	}
	settle(c) // the reloaded Matcher resumes propagating its pending set
	wantVersion(t, tc.heartbeat(), "products:1", 4)
}

func TestFailedStoreWriteFailsTheOperation(t *testing.T) {
	c, st := storeCell(t)
	st.Fail = func() bool { return true }
	if err := c.Publish("products:1", 4, ""); err == nil {
		t.Fatal("publish must fail when its version can't be stored")
	}
	if _, err := c.Handle(p.ClientMessage{Nonce: 1}); err == nil {
		t.Fatal("handshake must fail when the client can't be stored")
	}
	st.Fail = nil
	if err := c.Publish("products:1", 4, ""); err != nil {
		t.Fatal(err) // the retry goes through: memory wasn't updated by the failed attempt
	}
	tc := newTestClient(t, c)
	tc.register("products:1")
	settle(c)
	wantVersion(t, tc.heartbeat(), "products:1", 4)
}

func TestGarbageCollectedClientComesBackAsZombie(t *testing.T) {
	for _, persist := range []bool{false, true} {
		var c *Cell
		if persist {
			c, _ = storeCell(t)
		} else {
			c = NewCell("t", 2, 2, nil)
		}
		now := time.Unix(1000, 0)
		c.SetClock(func() time.Time { return now })

		tc := newTestClient(t, c)
		tc.register("products:1")
		settle(c)
		old := tc.tok.Client

		now = now.Add(48 * time.Hour) // the client goes quiet for two days
		if err := c.GC(24 * time.Hour); err != nil {
			t.Fatal(err)
		}

		// It comes back with a current token, but we forgot it: start over
		// under a new ID, via a token with no cell (a handshake in disguise).
		resp := tc.heartbeat()
		if resp.NewToken == nil || resp.NewToken.Client == old || resp.NewToken.Cell != "" {
			t.Fatalf("persist=%v: want a fresh client ID for a zombie, got %+v", persist, resp)
		}
		tc.tok = resp.NewToken
		resp = tc.heartbeat() // the new ID's Registrar grants a real token
		if resp.NewToken == nil || resp.NewToken.Client != tc.tok.Client || resp.NewToken.Cell != "t" {
			t.Fatalf("persist=%v: want a real token for the new ID, got %+v", persist, resp)
		}
		tc.tok = resp.NewToken
		tc.fullSync()
		settle(c)
		c.Publish("products:1", 7, "")
		settle(c)
		wantVersion(t, tc.heartbeat(), "products:1", 7)

		// The Matcher's registration for the old ID was dropped when the
		// Registrar answered "not registered".
		m := c.matcher(p.Owner("products:1", c.NumMatchers()))
		if _, ok := m.objects["products:1"].clients[old]; ok {
			t.Fatalf("persist=%v: Matcher kept the collected client's registration", persist)
		}
	}
}

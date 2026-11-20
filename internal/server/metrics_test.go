package server

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Kaustubh0117/herald/internal/metrics"
)

// TestResyncAndUnknownMetrics: a wipe shows up on the dashboard as one
// new_token resync per returning client, and registering for an object
// with no known version as an unknown-version notification.
func TestResyncAndUnknownMetrics(t *testing.T) {
	newToken := metrics.Resyncs.WithLabelValues("new_token")
	c := NewCell("t", 2, 2, nil)
	tc := newTestClient(t, c)

	unknown := testutil.ToFloat64(metrics.UnknownNotifications)
	tc.register("products:1")
	settle(c)
	if got := testutil.ToFloat64(metrics.UnknownNotifications) - unknown; got != 1 {
		t.Fatalf("unknown-version notifications grew by %v, want 1", got)
	}
	if n := c.PendingNotifications(); n != 1 {
		t.Fatalf("pending notifications = %d, want 1", n)
	}

	before := testutil.ToFloat64(newToken)
	if err := c.Wipe(); err != nil {
		t.Fatal(err)
	}
	if resp := tc.heartbeat(); resp.NewToken == nil {
		t.Fatal("expected a new token after the wipe")
	}
	if got := testutil.ToFloat64(newToken) - before; got != 1 {
		t.Fatalf("new_token resyncs grew by %v, want 1", got)
	}
}

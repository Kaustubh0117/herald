package server

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Kaustubh0117/herald/internal/metrics"
	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// TestPartialOutageBuffersAndDrains is Phase 8's DONE WHEN at unit scale:
// with 25% of the Matcher key space unwritable, every publish is still
// accepted (so the Bridge can ack its log), the other 75% notify right
// away, and the parked versions arrive once the outage ends.
func TestPartialOutageBuffersAndDrains(t *testing.T) {
	fs := &FaultyStore{Store: NewMemStore()}
	c, err := NewCellWithStore("t", 2, 4, nil, fs)
	if err != nil {
		t.Fatal(err)
	}
	var objs []p.ObjectID
	for i := 0; i < 40; i++ {
		objs = append(objs, p.ObjectID(fmt.Sprintf("products:%d", i)))
	}
	tc := newTestClient(t, c)
	tc.register(objs...)
	settle(c)
	tc.ack(tc.heartbeat().Notifications...) // unknown-version signals for the new registrations

	fs.SetOutage(0.25)
	before := testutil.ToFloat64(metrics.ScratchBuffered)
	down := map[p.ObjectID]bool{} // remembered: after the outage nothing is down
	bad := 0
	for _, id := range objs {
		if err := c.Publish(id, 10, ""); err != nil {
			t.Fatalf("publish %s during outage: %v", id, err)
		}
		if fs.Down(string(id)) {
			down[id] = true
			bad++
		}
	}
	if bad == 0 || bad == len(objs) {
		t.Fatalf("outage should hit some but not all objects, hit %d of %d", bad, len(objs))
	}
	// A hot object overwrites its own scratch slot instead of piling up.
	var hot p.ObjectID
	for _, id := range objs {
		if down[id] {
			hot = id
			break
		}
	}
	for v := p.Version(11); v <= 50; v++ {
		if err := c.Publish(hot, v, ""); err != nil {
			t.Fatal(err)
		}
	}
	if got := testutil.ToFloat64(metrics.ScratchBuffered) - before; got != float64(bad+40) {
		t.Fatalf("herald_scratch_buffered_total grew by %v, want %d", got, bad+40)
	}
	if n := scratchLen(t, fs); n != bad {
		t.Fatalf("scratch holds %d entries, want one per unwritable object (%d)", n, bad)
	}

	settle(c)
	resp := tc.heartbeat()
	for _, id := range objs {
		if _, got := pendingFor(resp, id); got == down[id] {
			t.Fatalf("%s (unwritable=%v): notified=%v during the outage", id, down[id], got)
		}
	}
	tc.ack(resp.Notifications...)

	fs.SetOutage(0)
	settle(c)
	resp = tc.heartbeat()
	for _, id := range objs {
		if !down[id] {
			if n, got := pendingFor(resp, id); got {
				t.Fatalf("%s notified twice: %+v", id, n)
			}
			continue
		}
		want := p.Version(10)
		if id == hot {
			want = 50
		}
		wantVersion(t, resp, id, want)
	}
	if n := scratchLen(t, fs); n != 0 {
		t.Fatalf("scratch not drained: %d entries left", n)
	}
}

// TestScratchSaltSkipsUnwritableSlots: when the object's first scratch slot
// is itself on a bad tablet, the next salt is tried.
func TestScratchSaltSkipsUnwritableSlots(t *testing.T) {
	fs := &FaultyStore{Store: NewMemStore()}
	fs.SetOutage(0.5)
	var id p.ObjectID
	for i := 0; ; i++ { // an object that is down AND whose first slot is down
		id = p.ObjectID(fmt.Sprintf("o%d", i))
		if fs.Down(string(id)) && fs.Down(scratchKey(ScratchSlot(id, 0))) {
			break
		}
	}
	if err := bufferToScratch(fs, id, 3, ""); err != nil {
		t.Fatal(err)
	}
	es, _ := fs.LoadScratch(0, 1)
	if len(es) != 1 || es[0].Slot == ScratchSlot(id, 0) {
		t.Fatalf("want one entry in a later slot, got %+v", es)
	}

	fs.SetOutage(1) // every tablet down: the publish fails and the Bridge retries
	if err := bufferToScratch(fs, "other", 1, ""); err == nil {
		t.Fatal("buffering with every slot unwritable should fail")
	}
}

func scratchLen(t *testing.T, st Store) int {
	t.Helper()
	es, err := st.LoadScratch(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	return len(es)
}

// TestScratchSurvivesCrowding: a long outage parks many objects, and each
// one occupies a slot until the outage ends. Every publish must still find
// room, or the Bridge stalls (the bench/brownout.sh regression: 1024 slots
// and 8 salts ran out).
func TestScratchSurvivesCrowding(t *testing.T) {
	fs := &FaultyStore{Store: NewMemStore()}
	fs.SetOutage(0.25)
	for i := 0; i < 2000; i++ {
		if err := bufferToScratch(fs, p.ObjectID(fmt.Sprintf("products:%d", i)), 1, ""); err != nil {
			t.Fatalf("object %d of 2000: %v", i, err)
		}
	}
}

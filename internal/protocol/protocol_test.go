package protocol

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestDigestOrderIndependent(t *testing.T) {
	var a, b Digest
	for _, id := range []ObjectID{"products:1", "products:2", "orders:9"} {
		a.Toggle(id)
	}
	for _, id := range []ObjectID{"orders:9", "products:1", "products:2"} {
		b.Toggle(id)
	}
	if a != b {
		t.Fatalf("digest depends on order: %v vs %v", a, b)
	}
	want := DigestOf(map[ObjectID]bool{"products:1": true, "products:2": true, "orders:9": true})
	if a != want {
		t.Fatalf("Toggle and DigestOf disagree: %v vs %v", a, want)
	}
}

func TestDigestToggleTwiceIsIdentity(t *testing.T) {
	var d Digest
	d.Toggle("products:1")
	before := d
	d.Toggle("products:2")
	d.Toggle("products:2")
	if d != before {
		t.Fatalf("toggling twice changed the digest: %v vs %v", d, before)
	}
	d.Toggle("products:1")
	if d != (Digest{}) {
		t.Fatalf("removing the last object should give the empty digest, got %v", d)
	}
}

func TestDigestOfIgnoresFalseEntries(t *testing.T) {
	got := DigestOf(map[ObjectID]bool{"a": true, "b": false})
	want := DigestOf(map[ObjectID]bool{"a": true})
	if got != want {
		t.Fatalf("false entries must not count: %v vs %v", got, want)
	}
}

func TestOwnerSpread(t *testing.T) {
	const ids, servers = 40000, 4
	counts := make([]int, servers)
	for i := 0; i < ids; i++ {
		counts[Owner(fmt.Sprintf("client-%d", i), servers)]++
	}
	ideal := ids / servers
	for s, c := range counts {
		if c < ideal*9/10 || c > ideal*11/10 {
			t.Errorf("server %d owns %d IDs, want %d ±10%% (all: %v)", s, c, ideal, counts)
		}
	}
}

func TestOwnerIsContiguous(t *testing.T) {
	// Range partitioning: sorting IDs by hash must sort them by owner too,
	// which is what lets a server recover with one range scan (Sec. 4.2.5).
	prev := -1
	for _, h := range []uint32{0, 1 << 30, 1 << 31, 3 << 30, 1<<32 - 1} {
		o := int(uint64(h) * 4 >> 32)
		if o < prev {
			t.Fatalf("owner went backwards at hash %#x", h)
		}
		prev = o
	}
	if prev != 3 {
		t.Fatalf("max hash should map to the last server, got %d", prev)
	}
}

func TestPartitionKey(t *testing.T) {
	k := PartitionKey("products:42")
	if !strings.HasSuffix(k, ":products:42") || len(k) != len("deadbeef:products:42") {
		t.Fatalf("unexpected partition key %q", k)
	}
	if PartitionKey("products:42") != k {
		t.Fatal("partition key must be deterministic")
	}
}

func TestKeyRangeAgreesWithOwner(t *testing.T) {
	in := func(k, lo, hi string) bool { return k >= lo && (hi == "" || k < hi) }
	for _, n := range []int{1, 2, 3, 4, 7} {
		for i := 0; i < 20000; i++ {
			id := fmt.Sprintf("client-%d", i)
			k := PartitionKey(id)
			for s := 0; s < n; s++ {
				lo, hi := KeyRange(s, n)
				if in(k, lo, hi) != (Owner(id, n) == s) {
					t.Fatalf("n=%d server %d: %s in [%s,%s) disagrees with Owner=%d", n, s, k, lo, hi, Owner(id, n))
				}
			}
		}
	}
	// Exact boundaries, where an off-by-one would hide.
	for _, n := range []int{3, 7} {
		for s := 1; s < n; s++ {
			lo, _ := KeyRange(s, n)
			var h uint64
			fmt.Sscanf(lo, "%x", &h)
			if int(h*uint64(n)>>32) != s || int((h-1)*uint64(n)>>32) != s-1 {
				t.Fatalf("n=%d: boundary %s is off", n, lo)
			}
		}
	}
}

// TestOwnerSpreadsSequentialKeys: real IDs differ only at the end
// ("products:41", "products:42"), and Owner must still spread them evenly.
func TestOwnerSpreadsSequentialKeys(t *testing.T) {
	for _, n := range []int{2, 3, 4, 7} {
		counts := make([]int, n)
		const keys = 10000
		for i := 0; i < keys; i++ {
			counts[Owner(fmt.Sprintf("products:%d", i), n)]++
		}
		for s, c := range counts {
			if want := keys / n; c < want*9/10 || c > want*11/10 {
				t.Errorf("n=%d: server %d owns %d of %d keys, want about %d", n, s, c, keys, want)
			}
		}
	}
}

func TestStampDeltas(t *testing.T) {
	base := time.UnixMicro(1_000_000)
	var s Stamps
	s.Stamp(StageCommit, base)
	s.Stamp(StageBridge, base.Add(5*time.Millisecond))
	// matcher not stamped: its time folds into the registrar stage
	s.Stamp(StageRegistrar, base.Add(25*time.Millisecond))
	s.Stamp(StageGateway, base.Add(26*time.Millisecond))
	got := s.Deltas(base.Add(30 * time.Millisecond))
	want := []StageDelta{{"bridge", 5 * time.Millisecond}, {"registrar", 20 * time.Millisecond},
		{"gateway", time.Millisecond}, {"client", 4 * time.Millisecond}}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
	if (Stamps{}).Deltas(base) != nil {
		t.Fatal("no commit stamp should give no deltas")
	}
	for st := StageCommit; st < NumStages; st++ {
		if back, ok := StageByName(st.String()); !ok || back != st {
			t.Fatalf("StageByName(%q) = %v, %v", st, back, ok)
		}
	}
}

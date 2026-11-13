package protocol

import (
	"fmt"
	"strings"
	"testing"
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

// Package protocol holds the small set of types every Herald component
// shares: object and client names, versions, tokens, digests, and the
// partitioning functions that decide which server owns what.
package protocol

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
)

// ObjectID names one piece of application data, e.g. "products:42".
type ObjectID string

// ClientID names one client (a browser tab, a service replica).
type ClientID string

// Version increases monotonically per object. Herald uses the commit LSN of
// the transaction that changed the row, so it can never go backwards
// (paper Sec. 3.1 warns about wall-clock versions and clock skew).
type Version int64

// Notification tells a client "Object is now at Version; go refetch it".
// Unknown is the paper's NotifyUnknown: the server lost track of the
// version, so the client must refetch regardless of what it has cached.
type Notification struct {
	Object  ObjectID
	Version Version
	Unknown bool
	Seq     uint64 // orders unknown-version notifications for one client
}

// Token identifies a client to one incarnation of one cell. When a cell is
// wiped its Epoch changes, every old token becomes stale, and clients
// re-handshake (the paper's "data center identifier is changed", Sec. 4.1.5).
type Token struct {
	Client ClientID
	Cell   string
	Epoch  uint64
}

// Digest is an order-independent fingerprint of a set of registrations.
// XOR of SHA-256 hashes lets us add or remove one object in O(1),
// instead of rehashing the whole set (the paper uses HMAC-SHA1 over the set).
// It detects drift, not tampering: registrations are still authorized
// server-side, so a crafted collision gains an attacker nothing.
type Digest [32]byte

// Toggle adds id if absent or removes it if present (XOR is self-inverse).
// Callers must only toggle when set membership actually changes.
func (d *Digest) Toggle(id ObjectID) {
	h := sha256.Sum256([]byte(id))
	for i := range d {
		d[i] ^= h[i]
	}
}

func (d Digest) String() string { return hex.EncodeToString(d[:6]) }

// DigestOf computes the digest of a whole set (used in tests and full syncs).
func DigestOf(ids map[ObjectID]bool) Digest {
	var d Digest
	for id, ok := range ids {
		if ok {
			d.Toggle(id)
		}
	}
	return d
}

// hash32 is the partitioning hash: FNV-1a, which is fast and stable across
// runs and machines, followed by MurmurHash3's finalizer. The finalizer
// matters because Owner uses the TOP bits, and on its own FNV-1a barely
// changes them when keys differ only at the end: "obj:0".."obj:19" all
// landed on one of three servers. Not cryptographic, and needn't be.
//
// Changing this moves every key, so stored state must be wiped (see
// LayoutVersion).
func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	x := h.Sum32()
	x ^= x >> 16
	x *= 0x85ebca6b
	x ^= x >> 13
	x *= 0xc2b2ae35
	x ^= x >> 16
	return x
}

// LayoutVersion identifies hash32. A store written under a different
// layout has its keys in the wrong ranges and must be wiped before use.
const LayoutVersion = 2

// PartitionKey prepends a hash of the ID to the ID itself, so keys spread
// uniformly while staying readable (paper Sec. 4.1.1). It is also the
// primary key of the persistent tables.
func PartitionKey(id string) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], hash32(id))
	return hex.EncodeToString(b[:]) + ":" + id
}

// Owner maps an ID to one of n servers by splitting the 32-bit hash space
// into n contiguous ranges -- the paper's static range partitioning.
func Owner(id string, n int) int {
	return int(uint64(hash32(id)) * uint64(n) >> 32)
}

// KeyRange returns the PartitionKey range owned by server i of n, for a
// recovery scan: keys k with lo <= k < hi, where hi == "" means no upper
// bound. It agrees exactly with Owner: Owner(id, n) == i iff
// PartitionKey(id) falls in KeyRange(i, n).
func KeyRange(i, n int) (lo, hi string) {
	// Owner(h) = floor(h*n / 2^32) == i  <=>  ceil(i*2^32/n) <= h < ceil((i+1)*2^32/n)
	bound := func(j int) uint64 { return (uint64(j)<<32 + uint64(n) - 1) / uint64(n) }
	lo = fmt.Sprintf("%08x", bound(i))
	if i+1 < n {
		hi = fmt.Sprintf("%08x", bound(i+1))
	}
	return lo, hi
}

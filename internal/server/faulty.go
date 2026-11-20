package server

import (
	"fmt"
	"sync"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// FaultyStore fails Matcher writes for keys whose partition hash falls in
// the bottom fraction of the hash space, the way a few unavailable storage
// tablets would (paper Sec. 4.2.6: real storage rarely dies completely).
// The scratch table lives on the same tablets, so a scratch slot whose key
// falls in the bad range fails too; that is what the salted retry is for.
// Registrar tables are left alone: the Bridge's ordered input is what an
// outage blocks, and clients retry their own operations anyway.
type FaultyStore struct {
	Store

	mu  sync.Mutex
	bad float64
}

// SetOutage makes the bottom fraction of the key space unwritable; 0 ends
// the outage.
func (f *FaultyStore) SetOutage(fraction float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bad = fraction
}

// Down reports whether writes for key currently fail.
func (f *FaultyStore) Down(key string) bool {
	const buckets = 1 << 20
	f.mu.Lock()
	defer f.mu.Unlock()
	return float64(p.Owner(key, buckets)) < f.bad*buckets
}

func scratchKey(slot int) string { return fmt.Sprintf("scratch/%d", slot) }

func (f *FaultyStore) SaveVersion(id p.ObjectID, v p.Version, src p.ClientID) error {
	if f.Down(string(id)) {
		return ErrStoreUnavailable
	}
	return f.Store.SaveVersion(id, v, src)
}

func (f *FaultyStore) ClearPending(id p.ObjectID, v p.Version) error {
	if f.Down(string(id)) {
		return ErrStoreUnavailable
	}
	return f.Store.ClearPending(id, v)
}

func (f *FaultyStore) SaveMatcherReg(id p.ObjectID, c p.ClientID) error {
	if f.Down(string(id)) {
		return ErrStoreUnavailable
	}
	return f.Store.SaveMatcherReg(id, c)
}

func (f *FaultyStore) DeleteMatcherReg(id p.ObjectID, c p.ClientID) error {
	if f.Down(string(id)) {
		return ErrStoreUnavailable
	}
	return f.Store.DeleteMatcherReg(id, c)
}

func (f *FaultyStore) SaveScratch(e ScratchEntry) (bool, error) {
	if f.Down(scratchKey(e.Slot)) {
		return false, ErrStoreUnavailable
	}
	return f.Store.SaveScratch(e)
}

func (f *FaultyStore) DeleteScratch(e ScratchEntry) error {
	if f.Down(scratchKey(e.Slot)) {
		return ErrStoreUnavailable
	}
	return f.Store.DeleteScratch(e)
}

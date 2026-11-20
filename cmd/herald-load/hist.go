package main

import (
	"math"
	"sync"
	"time"
)

// hist is a log-bucketed latency histogram: each bucket is 2% wider than
// the last, from 10 µs up, so quantiles come out within ~1% without
// keeping every sample.
type hist struct {
	mu      sync.Mutex
	buckets []uint64
	n       uint64
	sum     time.Duration
}

const (
	histMin    = 10 * time.Microsecond
	histGrowth = 1.02
)

func bucketOf(d time.Duration) int {
	if d <= histMin {
		return 0
	}
	return int(math.Log(float64(d)/float64(histMin))/math.Log(histGrowth)) + 1
}

// upper is the largest duration that falls in bucket b.
func upper(b int) time.Duration {
	return time.Duration(float64(histMin) * math.Pow(histGrowth, float64(b)))
}

func (h *hist) add(d time.Duration) {
	b := bucketOf(d)
	h.mu.Lock()
	defer h.mu.Unlock()
	for len(h.buckets) <= b {
		h.buckets = append(h.buckets, 0)
	}
	h.buckets[b]++
	h.n++
	h.sum += d
}

// quantile returns the q-th quantile (0 < q <= 1), or 0 with no samples.
func (h *hist) quantile(q float64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.n == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.n)))
	var seen uint64
	for b, c := range h.buckets {
		seen += c
		if seen >= rank {
			return upper(b)
		}
	}
	return upper(len(h.buckets) - 1)
}

func (h *hist) mean() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.n == 0 {
		return 0
	}
	return h.sum / time.Duration(h.n)
}

func (h *hist) count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

package main

import (
	"testing"
	"time"
)

func TestParseCPUTime(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"0:01.50":    1500 * time.Millisecond,
		"2:03.25":    123250 * time.Millisecond,
		"01:02:03":   3723 * time.Second,
		"1-00:00:01": 86401 * time.Second,
	} {
		if got := parseCPUTime(in); got != want {
			t.Errorf("parseCPUTime(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestHistQuantiles(t *testing.T) {
	h := &hist{}
	for i := 1; i <= 1000; i++ {
		h.add(time.Duration(i) * time.Millisecond)
	}
	for q, want := range map[float64]time.Duration{0.5: 500 * time.Millisecond, 0.99: 990 * time.Millisecond} {
		got := h.quantile(q)
		if d := float64(got-want) / float64(want); d < 0 || d > 0.03 { // buckets are 2% wide, upper bound
			t.Errorf("q%.2f = %v, want %v (+3%%)", q, got, want)
		}
	}
}

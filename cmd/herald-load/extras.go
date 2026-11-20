package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// procStats reads a process's total CPU time and resident memory (bytes)
// with ps, which works on macOS and Linux without cgo. pid 0: zeros.
func procStats(pid int) (cpu time.Duration, rss float64) {
	if pid == 0 {
		return 0, 0
	}
	out, err := exec.Command("ps", "-o", "time=,rss=", "-p", strconv.Itoa(pid)).Output()
	f := strings.Fields(string(out))
	if err != nil || len(f) != 2 {
		log.Printf("ps %d: %v %q", pid, err, out)
		return 0, 0
	}
	kb, _ := strconv.ParseFloat(f[1], 64)
	return parseCPUTime(f[0]), kb * 1024
}

// parseCPUTime parses ps's "[[dd-]hh:]mm:ss[.ss]".
func parseCPUTime(s string) time.Duration {
	var days float64
	if d, rest, ok := strings.Cut(s, "-"); ok {
		days, _ = strconv.ParseFloat(d, 64)
		s = rest
	}
	secs := 0.0
	for _, part := range strings.Split(s, ":") {
		v, _ := strconv.ParseFloat(part, 64)
		secs = secs*60 + v
	}
	return time.Duration((days*86400 + secs) * float64(time.Second))
}

// perSecond collects one second of the timeline at a time.
type perSecond struct {
	delivered atomic.Int64
	mu        sync.Mutex
	h         *hist // commit-to-client delays of stamped notifications
}

func (s *perSecond) add(d time.Duration) {
	s.mu.Lock()
	s.h.add(d)
	s.mu.Unlock()
}

// take returns this second's numbers and starts the next second.
func (s *perSecond) take() (delivered int64, h *hist) {
	s.mu.Lock()
	h, s.h = s.h, &hist{}
	s.mu.Unlock()
	return s.delivered.Swap(0), h
}

// writeSeries writes one row per second of the window: updates sent,
// notifications delivered, delay quantiles, and (with scrape) the cell's
// scratch counters and slot lag.
func writeSeries(ctx context.Context, path string, start time.Time, d time.Duration,
	sec *perSecond, updates *atomic.Int64, scrape string) {
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"t_s", "updates", "delivered", "p50_ms", "p99_ms",
		"scratch_buffered", "scratch_drained", "slot_lag_bytes"})
	t := time.NewTicker(time.Second)
	defer t.Stop()
	lastUpdates := updates.Load()
	for i := 1; time.Duration(i)*time.Second <= d; i++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n := updates.Load()
		delivered, h := sec.take()
		m := scrapeMetrics(scrape)
		w.Write([]string{strconv.Itoa(i), strconv.FormatInt(n-lastUpdates, 10), strconv.FormatInt(delivered, 10),
			msNum(h.quantile(0.5)), msNum(h.quantile(0.99)),
			m["herald_scratch_buffered_total"], m["herald_scratch_drained_total"], m["herald_bridge_slot_lag_bytes"]})
		w.Flush()
		lastUpdates = n
	}
}

// scrapeMetrics fetches a Prometheus text page into name -> value (only
// unlabelled samples, which is all the timeline needs).
func scrapeMetrics(url string) map[string]string {
	out := map[string]string{}
	if url == "" {
		return out
	}
	resp, err := http.Get(url)
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		if name, val, ok := strings.Cut(line, " "); ok && !strings.Contains(name, "{") {
			out[name] = val
		}
	}
	return out
}

// outage breaks part of the cell's store at `at` into the window and heals
// it `length` later, through herald-dev's /admin/outage.
func outage(ctx context.Context, url string, at, length time.Duration, fraction float64) {
	set := func(f float64) {
		resp, err := http.Post(fmt.Sprintf("%s?fraction=%g", url, f), "", nil)
		if err != nil {
			log.Printf("outage: %v", err)
			return
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("outage: %s", resp.Status)
		}
	}
	sleep(ctx, at)
	set(fraction)
	log.Printf("outage: %.0f%% of the Matcher key space unwritable", fraction*100)
	sleep(ctx, length)
	set(0)
	log.Printf("outage over")
}

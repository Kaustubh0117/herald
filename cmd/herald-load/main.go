// herald-load is the load generator for the Phase 10 experiments. It opens
// many simulated clients against a gateway, each registered for a few
// objects, drives UPDATEs into Postgres at a fixed rate, and measures how
// long each notification took, per stage, from the stamps it carries.
//
//	herald-load -url ws://127.0.0.1:8080/ws -pg URL -clients 1000 -rate 100 -duration 60s
//
// Results go to stdout as a table and, with -out, are appended as CSV rows
// (one per stage) so bench scripts can chart them. With -metrics, the same
// numbers are exported as herald_delivery_seconds{stage}.
package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Kaustubh0117/herald/internal/client"
	"github.com/Kaustubh0117/herald/internal/metrics"
	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/wsclient"
)

var stages = []string{"bridge", "matcher", "registrar", "gateway", "client", "total"}

func main() {
	url := flag.String("url", "ws://127.0.0.1:8080/ws", "gateway WebSocket URL")
	pg := flag.String("pg", os.Getenv("HERALD_PG"), "Postgres URL for the UPDATE driver (empty: no updates)")
	nClients := flag.Int("clients", 1000, "simulated clients")
	nObjects := flag.Int("objects", 200, "distinct rows in the products table to spread clients over")
	perClient := flag.Int("per-client", 5, "objects each client registers for")
	heartbeat := flag.Duration("heartbeat", 30*time.Second, "client heartbeat interval (reliable path)")
	rate := flag.Float64("rate", 100, "UPDATEs per second")
	warmup := flag.Duration("warmup", 5*time.Second, "settle time after connecting, before measuring")
	duration := flag.Duration("duration", 30*time.Second, "measurement window")
	label := flag.String("label", "", "free-form tag written to every CSV row (e.g. step=20ms)")
	out := flag.String("out", "", "append CSV results to this file")
	metricsAddr := flag.String("metrics", "", "serve Prometheus metrics on this address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *metricsAddr != "" {
		go func() { log.Fatal(http.ListenAndServe(*metricsAddr, promhttp.Handler())) }()
	}

	var db *pgxpool.Pool
	if *pg != "" {
		var err error
		if db, err = pgxpool.New(ctx, *pg); err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		// Rows 1..objects must exist for UPDATEs to produce notifications.
		if _, err := db.Exec(ctx, `INSERT INTO products (id, name, price)
			SELECT g, 'item ' || g, 1 FROM generate_series(1, $1) g ON CONFLICT (id) DO NOTHING`, *nObjects); err != nil {
			log.Fatalf("seeding products: %v", err)
		}
	}

	// Measurement: stage deltas from every delivered notification that was
	// caused by a commit, recorded only inside the measurement window.
	var measuring atomic.Bool
	hists := map[string]*hist{}
	for _, s := range stages {
		hists[s] = &hist{}
	}
	observe := func(n p.Notification, received time.Time) {
		if !measuring.Load() || n.Stamps[p.StageCommit] == 0 {
			return
		}
		for _, d := range n.Stamps.Deltas(received) {
			hists[d.Stage].add(d.D)
			metrics.DeliverySeconds.WithLabelValues(d.Stage).Observe(d.D.Seconds())
		}
		total := received.Sub(time.UnixMicro(n.Stamps[p.StageCommit]))
		hists["total"].add(total)
		metrics.DeliverySeconds.WithLabelValues("total").Observe(total.Seconds())
	}

	log.Printf("connecting %d clients to %s ...", *nClients, *url)
	t0 := time.Now()
	var connected, failed atomic.Int64
	sem := make(chan struct{}, 200) // concurrent dials
	var wg sync.WaitGroup
	for i := 0; i < *nClients; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := runClient(ctx, *url, i, *nObjects, *perClient, *heartbeat, observe); err != nil {
				if failed.Add(1) <= 5 {
					log.Printf("client %d: %v", i, err)
				}
				return
			}
			connected.Add(1)
		}(i)
	}
	wg.Wait()
	log.Printf("%d clients registered in %v (%d failed)", connected.Load(), time.Since(t0).Round(time.Millisecond), failed.Load())
	if connected.Load() == 0 {
		log.Fatal("no clients connected")
	}

	sleep(ctx, *warmup)
	var updates atomic.Int64
	if db != nil {
		go drive(ctx, db, *rate, *nObjects, &updates)
	}
	measuring.Store(true)
	start := time.Now()
	sleep(ctx, *duration)
	measuring.Store(false)
	elapsed := time.Since(start)

	fmt.Printf("\n%d clients, %.0f updates/s requested, %.0f/s sent, %d notifications in %v\n",
		connected.Load(), *rate, float64(updates.Load())/elapsed.Seconds(), hists["total"].count(), elapsed.Round(time.Second))
	fmt.Printf("%-10s %10s %10s %10s %10s\n", "stage", "count", "p50", "p99", "mean")
	for _, s := range stages {
		h := hists[s]
		fmt.Printf("%-10s %10d %10s %10s %10s\n", s, h.count(), ms(h.quantile(0.5)), ms(h.quantile(0.99)), ms(h.mean()))
	}
	if *out != "" {
		if err := writeCSV(*out, *label, int(connected.Load()), *rate, hists); err != nil {
			log.Fatal(err)
		}
	}
}

// runClient connects one simulated client, registers it, and leaves it
// heartbeating in the background until ctx ends.
func runClient(ctx context.Context, url string, i, nObjects, perClient int, heartbeat time.Duration,
	observe func(p.Notification, time.Time)) error {
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := wsclient.Dial(dctx, url)
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewPCG(uint64(i), 42)) // each client's objects are reproducible
	want := map[p.ObjectID]bool{}
	for len(want) < min(perClient, nObjects) {
		want[p.ObjectID(fmt.Sprintf("products:%d", 1+rng.IntN(nObjects)))] = true
	}
	c := client.New(client.Config{Send: conn.Send, Listener: listener{}, Observe: observe})
	conn.OnPush(c.OnPush)
	for id := range want {
		c.Register(id)
	}
	// Handshake, full sync, confirmation: a few round trips, then the
	// regular heartbeat takes over.
	for k := 0; k < 3; k++ {
		if err := c.Tick(); err != nil {
			conn.Close()
			return err
		}
	}
	go func() {
		c.Run(ctx, heartbeat)
		conn.Close()
	}()
	return nil
}

// drive issues UPDATEs at the given rate, spread over the objects, from a
// pool of workers so a slow statement doesn't throttle the rate.
func drive(ctx context.Context, db *pgxpool.Pool, rate float64, nObjects int, sent *atomic.Int64) {
	ids := make(chan int, 1024)
	for w := 0; w < 16; w++ {
		go func() {
			for id := range ids {
				if _, err := db.Exec(ctx, `UPDATE products SET price = price + 1 WHERE id = $1`, id); err == nil {
					sent.Add(1)
				} else if !errors.Is(err, context.Canceled) {
					log.Printf("update: %v", err)
				}
			}
		}()
	}
	defer close(ids)
	const tick = 10 * time.Millisecond
	t := time.NewTicker(tick)
	defer t.Stop()
	owed := 0.0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		owed += rate * tick.Seconds()
		for ; owed >= 1; owed-- {
			select {
			case ids <- 1 + rand.IntN(nObjects):
			default: // workers can't keep up: drop rather than queue unboundedly
			}
		}
	}
}

func writeCSV(path, label string, clients int, rate float64, hists map[string]*hist) error {
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if os.IsNotExist(statErr) {
		w.Write([]string{"label", "clients", "rate", "stage", "count", "p50_ms", "p99_ms", "mean_ms"})
	}
	for _, s := range stages {
		h := hists[s]
		w.Write([]string{label, strconv.Itoa(clients), strconv.FormatFloat(rate, 'f', -1, 64), s,
			strconv.FormatUint(h.count(), 10), msNum(h.quantile(0.5)), msNum(h.quantile(0.99)), msNum(h.mean())})
	}
	w.Flush()
	return w.Error()
}

func ms(d time.Duration) string    { return msNum(d) + "ms" }
func msNum(d time.Duration) string { return strconv.FormatFloat(float64(d)/1e6, 'f', 2, 64) }

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// listener is a do-nothing application: the load generator only measures.
type listener struct{}

func (listener) Notify(p.ObjectID, p.Version)               {}
func (listener) NotifyUnknown(p.ObjectID)                   {}
func (listener) RegistrationStatusChanged(p.ObjectID, bool) {}
func (listener) ReissueRegistrations()                      {}
func (listener) WriteState([]byte)                          {}

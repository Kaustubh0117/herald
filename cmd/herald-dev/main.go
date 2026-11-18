// herald-dev runs a whole Herald cell in one process: Matchers, Registrars,
// a WebSocket gateway and (with -pg) the Postgres Bridge, plus a tiny demo
// "application backend" serving the products table. It is what you demo from.
//
//	go run ./cmd/herald-dev -pg postgres://postgres:herald@127.0.0.1:5432/postgres
//	open http://localhost:8080, then in psql: UPDATE products SET price = 12 WHERE id = 1;
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kaustubh0117/herald/internal/bridge"
	"github.com/Kaustubh0117/herald/internal/demo"
	"github.com/Kaustubh0117/herald/internal/gateway"
	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
	"github.com/Kaustubh0117/herald/web"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	pg := flag.String("pg", os.Getenv("HERALD_PG"), "Postgres URL; enables the Bridge and the demo API")
	slot := flag.String("slot", "herald_dev", "replication slot (created if missing)")
	pub := flag.String("pub", "herald_pub", "publication (created for the demo table if missing)")
	step := flag.Duration("step", 50*time.Millisecond, "propagator interval (the paper batches with ~200-500 ms)")
	flag.Parse()
	ctx := context.Background()

	var gw *gateway.Gateway
	push := func(_ string, c p.ClientID, ns []p.Notification) { gw.Push(c, ns) }
	cell := server.NewCell("dev", 2, 2, push)
	gw = gateway.New("gw-dev", gateway.Local{Cell: cell})

	go func() { // propagator loops
		for range time.Tick(*step) {
			cell.Step()
		}
	}()

	http.Handle("/ws", gw)

	// Manual publish, handy without Postgres: POST /publish?obj=products:1&v=7
	http.HandleFunc("POST /publish", func(w http.ResponseWriter, r *http.Request) {
		v, err := strconv.ParseInt(r.URL.Query().Get("v"), 10, 64)
		if err != nil {
			http.Error(w, "bad v", http.StatusBadRequest)
			return
		}
		if err := cell.Publish(p.ObjectID(r.URL.Query().Get("obj")), p.Version(v), ""); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		}
	})

	// The demo button: throw away every byte of server state.
	http.HandleFunc("POST /admin/wipe", func(w http.ResponseWriter, r *http.Request) {
		cell.Wipe()
		log.Printf("cell wiped: all server state discarded")
	})

	if *pg != "" {
		db, err := pgxpool.New(ctx, *pg)
		if err != nil {
			log.Fatalf("postgres: %v", err)
		}
		if err := demo.Setup(ctx, db, *slot, *pub); err != nil {
			log.Fatalf("demo setup: %v", err)
		}
		http.Handle("GET /api/products", demo.Products(db))
		go runBridge(ctx, &bridge.Bridge{DSN: *pg, Slot: *slot, Publication: *pub, Out: cell})
	} else {
		log.Printf("no -pg given: Bridge disabled; use POST /publish to send versions")
	}

	http.Handle("/", http.FileServerFS(web.FS)) // /herald-client.js, /demo/
	http.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/demo/", http.StatusFound)
	})

	log.Printf("herald-dev listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

// runBridge keeps the Bridge running; the replication slot makes every
// restart lossless.
func runBridge(ctx context.Context, b *bridge.Bridge) {
	for {
		err := b.Run(ctx)
		log.Printf("bridge stopped: %v (restarting in 1s)", err)
		time.Sleep(time.Second)
	}
}

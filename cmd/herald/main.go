// herald runs one Herald server process. Every process reads the same
// partition map (deploy/partitions.yaml) to find its peers.
//
//	herald matcher   -config partitions.yaml -i 0
//	herald registrar -config partitions.yaml -i 0
//	herald gateway   -config partitions.yaml -listen :8080 -grpc 127.0.0.1:7100 [-pg URL]
//	herald bridge    -config partitions.yaml -pg URL [-demo]
//	herald wipe      -config partitions.yaml -pg URL
//
// With -store, matchers and registrars keep their soft state in Postgres
// (-pg) and reload it when restarted, instead of making clients re-sync.
// -metrics ADDR serves Prometheus metrics at ADDR/metrics.
//
// For a single process running everything, use herald-dev instead.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	pb "github.com/Kaustubh0117/herald/gen/heraldv1"
	"github.com/Kaustubh0117/herald/internal/bridge"
	"github.com/Kaustubh0117/herald/internal/demo"
	"github.com/Kaustubh0117/herald/internal/gateway"
	"github.com/Kaustubh0117/herald/internal/metrics"
	"github.com/Kaustubh0117/herald/internal/rpc"
	"github.com/Kaustubh0117/herald/internal/server"
	"github.com/Kaustubh0117/herald/internal/store"
	"github.com/Kaustubh0117/herald/web"
)

// Config is the static partition map (the paper used Chubby). Servers own
// equal contiguous slices of the hash space, in list order, matching
// protocol.Owner. Changing the lists means restarting every process.
type Config struct {
	Cell       string        `yaml:"cell"`
	Registrars []string      `yaml:"registrars"`
	Matchers   []string      `yaml:"matchers"`
	Step       time.Duration `yaml:"step"` // propagator interval
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: herald matcher|registrar|gateway|bridge|wipe [flags]")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	configPath := fs.String("config", "deploy/partitions.yaml", "partition map")
	index := fs.Int("i", 0, "matcher/registrar: which entry in the partition map this process is")
	listen := fs.String("listen", ":8080", "gateway: HTTP/WebSocket listen address")
	grpcAddr := fs.String("grpc", "127.0.0.1:7100", "gateway: gRPC address Registrars push to")
	pg := fs.String("pg", os.Getenv("HERALD_PG"), "bridge: Postgres URL; gateway: serve the demo API from it")
	slot := fs.String("slot", "herald_cell", "bridge: replication slot")
	pub := fs.String("pub", "herald_pub", "bridge: publication")
	demoSetup := fs.Bool("demo", false, "bridge: create the demo table, publication and slot if missing")
	persist := fs.Bool("store", false, "matcher/registrar: keep soft state in Postgres (-pg) and reload it on restart")
	gcTTL := fs.Duration("gc-ttl", 24*time.Hour, "registrar: garbage-collect clients idle this long")
	metricsAddr := fs.String("metrics", "", "serve Prometheus metrics on this address (e.g. :9102)")
	fs.Parse(args)

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.SetPrefix(fmt.Sprintf("[%s] ", cmd))
	if *metricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("GET /metrics", promhttp.Handler())
			log.Fatal(http.ListenAndServe(*metricsAddr, mux))
		}()
	}

	var st server.Store
	if *persist || cmd == "wipe" {
		if *pg == "" {
			log.Fatal("-store and wipe need -pg or HERALD_PG")
		}
		st = openStore(ctx, *pg, cfg.Cell)
	}

	switch cmd {
	case "matcher":
		runMatcher(ctx, cfg, *index, st)
	case "registrar":
		runRegistrar(ctx, cfg, *index, st, *gcTTL)
	case "wipe":
		e, err := st.Wipe()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("wiped cell %s; new epoch %d. Restart its matchers and registrars.", cfg.Cell, e)
	case "gateway":
		runGateway(ctx, cfg, *listen, *grpcAddr, *pg)
	case "bridge":
		runBridge(ctx, cfg, *pg, *slot, *pub, *demoSetup)
	default:
		log.Fatalf("unknown command %q", cmd)
	}
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{Step: 50 * time.Millisecond}
	if err := yaml.Unmarshal(b, cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(cfg.Registrars) == 0 || len(cfg.Matchers) == 0 {
		return nil, fmt.Errorf("%s: need at least one registrar and one matcher", path)
	}
	return cfg, nil
}

func matcherClients(cfg *Config) rpc.Matchers {
	var out rpc.Matchers
	for _, a := range cfg.Matchers {
		conn, err := rpc.Dial(a)
		if err != nil {
			log.Fatal(err)
		}
		out = append(out, rpc.NewMatcherClient(conn))
	}
	return out
}

func registrarClients(cfg *Config) []*rpc.RegistrarClient {
	var out []*rpc.RegistrarClient
	for _, a := range cfg.Registrars {
		conn, err := rpc.Dial(a)
		if err != nil {
			log.Fatal(err)
		}
		out = append(out, rpc.NewRegistrarClient(conn))
	}
	return out
}

// epoch identifies the state a memory-only process starts with: empty, so
// it must differ from any previous run, which millisecond start time does.
// (With -store the epoch comes from the store and survives restarts.)
func epoch() uint64 { return uint64(time.Now().UnixMilli()) }

// serveGRPC listens on the port of addr on all interfaces: addr is how
// peers reach us (e.g. "matcher-0:7002" in docker-compose).
func serveGRPC(ctx context.Context, addr string, register func(*grpc.Server)) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatal(err)
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	register(s)
	go func() { <-ctx.Done(); s.Stop() }()
	log.Printf("gRPC on %s", addr)
	if err := s.Serve(lis); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func every(ctx context.Context, d time.Duration, f func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}

// openStore opens the cell's state schema ("herald_<cell>").
func openStore(ctx context.Context, pg, cell string) server.Store {
	db, err := pgxpool.New(ctx, pg)
	if err != nil {
		log.Fatal(err)
	}
	st, err := store.Open(ctx, db, "herald_"+cell)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	return st
}

func runMatcher(ctx context.Context, cfg *Config, i int, st server.Store) {
	var regs []server.RegistrarAPI
	for _, r := range registrarClients(cfg) {
		regs = append(regs, r)
	}
	m := server.NewMatcher(epoch(), regs)
	if st != nil {
		e, err := st.Epoch()
		if err != nil {
			log.Fatal(err)
		}
		if m, err = server.LoadMatcher(e, regs, st, i, len(cfg.Matchers)); err != nil {
			log.Fatal(err)
		}
		log.Printf("loaded state from Postgres (epoch %d)", e)
	}
	go every(ctx, cfg.Step, m.Propagate)
	go every(ctx, time.Second, func() { // move versions parked during an outage into place
		if err := m.Drain(); err != nil {
			log.Printf("drain: %v", err)
		}
	})
	serveGRPC(ctx, cfg.Matchers[i], func(s *grpc.Server) { pb.RegisterMatcherServer(s, &rpc.MatcherServer{M: m}) })
}

func runRegistrar(ctx context.Context, cfg *Config, i int, st server.Store, gcTTL time.Duration) {
	push := rpc.NewPusher().Push
	r := server.NewRegistrar(cfg.Cell, epoch(), matcherClients(cfg), push)
	if st != nil {
		e, err := st.Epoch()
		if err != nil {
			log.Fatal(err)
		}
		if r, err = server.LoadRegistrar(cfg.Cell, e, matcherClients(cfg), push, st, i, len(cfg.Registrars), nil); err != nil {
			log.Fatal(err)
		}
		log.Printf("loaded state from Postgres (epoch %d)", e)
	}
	metrics.PendingNotifications(func() float64 { return float64(r.PendingNotifications()) })
	go every(ctx, cfg.Step, r.Propagate)
	go every(ctx, time.Minute, func() {
		if err := r.GC(gcTTL); err != nil {
			log.Printf("gc: %v", err) // retried next minute
		}
	})
	serveGRPC(ctx, cfg.Registrars[i], func(s *grpc.Server) { pb.RegisterRegistrarServer(s, &rpc.RegistrarServer{R: r}) })
}

func runGateway(ctx context.Context, cfg *Config, listen, grpcAddr, pg string) {
	// The gateway's gRPC address is what it stamps on client messages, so
	// Registrars know where to push.
	gw := gateway.New(grpcAddr, rpc.Router(registrarClients(cfg)))
	go serveGRPC(ctx, grpcAddr, func(s *grpc.Server) { pb.RegisterGatewayServer(s, &rpc.GatewayServer{G: gw}) })

	mux := http.NewServeMux()
	mux.Handle("/ws", gw)
	mux.Handle("/", http.FileServerFS(web.FS))
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/demo/", http.StatusFound)
	})
	if pg != "" { // demo convenience: the "application backend" the page refetches from
		db, err := pgxpool.New(ctx, pg)
		if err != nil {
			log.Fatal(err)
		}
		mux.Handle("GET /api/products", demo.Products(db))
	}
	srv := &http.Server{Addr: listen, Handler: mux}
	go func() { <-ctx.Done(); srv.Close() }()
	log.Printf("WebSocket on %s", listen)
	if err := srv.ListenAndServe(); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func runBridge(ctx context.Context, cfg *Config, pg, slot, pub string, demoSetup bool) {
	if pg == "" {
		log.Fatal("bridge needs -pg or HERALD_PG")
	}
	if demoSetup {
		db, err := pgxpool.New(ctx, pg)
		if err != nil {
			log.Fatal(err)
		}
		if err := demo.Setup(ctx, db, slot, pub); err != nil {
			log.Fatalf("demo setup: %v", err)
		}
		db.Close()
	}
	b := &bridge.Bridge{DSN: pg, Slot: slot, Publication: pub, Out: matcherClients(cfg)}
	go bridge.WatchLag(ctx, pg, slot, 5*time.Second)
	for ctx.Err() == nil { // the slot makes every restart lossless
		err := b.Run(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Printf("bridge stopped: %v (restarting in 1s)", err)
		time.Sleep(time.Second)
	}
}

package rpc

import (
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "github.com/Kaustubh0117/herald/gen/heraldv1"
	"github.com/Kaustubh0117/herald/internal/client"
	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
)

// cluster runs Matchers and Registrars as gRPC servers on localhost ports,
// talking to each other only through the network adapters. Restarting one
// stops its server and starts an empty replacement on the same address,
// like a crashed process coming back.
type cluster struct {
	t      *testing.T
	mAddrs []string
	rAddrs []string
	mAPI   Matchers
	rAPI   []server.RegistrarAPI
	router Router

	mu    sync.Mutex
	epoch uint64
	ms    []*server.Matcher
	rs    []*server.Registrar
	srvs  map[string]*grpc.Server
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func newCluster(t *testing.T, nMatchers, nRegistrars int) *cluster {
	c := &cluster{t: t, srvs: map[string]*grpc.Server{}}
	for i := 0; i < nMatchers; i++ {
		c.mAddrs = append(c.mAddrs, freeAddr(t))
		conn, _ := Dial(c.mAddrs[i])
		c.mAPI = append(c.mAPI, NewMatcherClient(conn))
	}
	for i := 0; i < nRegistrars; i++ {
		c.rAddrs = append(c.rAddrs, freeAddr(t))
		conn, _ := Dial(c.rAddrs[i])
		rc := NewRegistrarClient(conn)
		c.rAPI = append(c.rAPI, rc)
		c.router = append(c.router, rc)
	}
	c.ms = make([]*server.Matcher, nMatchers)
	c.rs = make([]*server.Registrar, nRegistrars)
	for i := range c.ms {
		c.startMatcher(i)
	}
	for i := range c.rs {
		c.startRegistrar(i)
	}

	done := make(chan struct{})
	go func() { // every process runs its propagator on a timer
		for {
			select {
			case <-done:
				return
			case <-time.After(10 * time.Millisecond):
			}
			c.mu.Lock()
			ms, rs := append([]*server.Matcher(nil), c.ms...), append([]*server.Registrar(nil), c.rs...)
			c.mu.Unlock()
			for _, r := range rs {
				r.Propagate()
			}
			for _, m := range ms {
				m.Propagate()
			}
		}
	}()
	t.Cleanup(func() {
		close(done)
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, s := range c.srvs {
			s.Stop()
		}
	})
	return c
}

func (c *cluster) serve(addr string, register func(*grpc.Server)) {
	c.mu.Lock()
	if old := c.srvs[addr]; old != nil {
		old.Stop() // the process dies, taking its memory with it
	}
	c.mu.Unlock()
	var lis net.Listener
	var err error
	for i := 0; i < 50; i++ { // the port can take a moment to free up
		if lis, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		c.t.Fatal(err)
	}
	s := grpc.NewServer()
	register(s)
	go s.Serve(lis)
	c.mu.Lock()
	c.srvs[addr] = s
	c.mu.Unlock()
}

func (c *cluster) nextEpoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch++
	return c.epoch
}

func (c *cluster) startMatcher(i int) {
	m := server.NewMatcher(c.nextEpoch(), c.rAPI)
	c.serve(c.mAddrs[i], func(s *grpc.Server) { pb.RegisterMatcherServer(s, &MatcherServer{M: m}) })
	c.mu.Lock()
	c.ms[i] = m
	c.mu.Unlock()
}

func (c *cluster) startRegistrar(i int) {
	r := server.NewRegistrar("rpc-test", c.nextEpoch(), c.mAPI, nil)
	c.serve(c.rAddrs[i], func(s *grpc.Server) { pb.RegisterRegistrarServer(s, &RegistrarServer{R: r}) })
	c.mu.Lock()
	c.rs[i] = r
	c.mu.Unlock()
}

// publish retries like the Bridge does: the Matcher may be restarting.
func (c *cluster) publish(id p.ObjectID, v p.Version) {
	deadline := time.Now().Add(10 * time.Second)
	for c.mAPI.Publish(id, v, "") != nil {
		if time.Now().After(deadline) {
			c.t.Fatalf("publish %s@%d never succeeded", id, v)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type events struct {
	mu      sync.Mutex
	latest  p.Version
	unknown int
}

func (e *events) Notify(_ p.ObjectID, v p.Version) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.latest = v
}

func (e *events) NotifyUnknown(p.ObjectID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.unknown++
}

func (e *events) RegistrationStatusChanged(p.ObjectID, bool) {}
func (e *events) ReissueRegistrations()                      {}
func (e *events) WriteState([]byte)                          {}

func (e *events) get() (p.Version, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.latest, e.unknown
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSeparateProcessesSurviveRestarts(t *testing.T) {
	c := newCluster(t, 1, 2)
	ev := &events{}
	cl := client.New(client.Config{Send: c.router.Handle, Listener: ev})
	cl.Register("products:1")
	stopTicks := make(chan struct{})
	defer close(stopTicks)
	go func() {
		for {
			select {
			case <-stopTicks:
				return
			case <-time.After(10 * time.Millisecond):
				cl.Tick()
			}
		}
	}()

	waitFor(t, "initial unknown-version signal", func() bool { _, u := ev.get(); return u == 1 })
	c.publish("products:1", 5)
	waitFor(t, "v5", func() bool { v, _ := ev.get(); return v == 5 })

	// The Matcher crashes and comes back empty on the same address. The
	// Registrar notices the new epoch and re-sends the registration;
	// post-propagation turns the lost version into a refetch.
	c.startMatcher(0)
	waitFor(t, "refetch after Matcher restart", func() bool { _, u := ev.get(); return u == 2 })
	c.publish("products:1", 7)
	waitFor(t, "v7 after Matcher restart", func() bool { v, _ := ev.get(); return v == 7 })

	// Both Registrars crash: the client's token goes stale, it re-sends its
	// full set, and keeps receiving updates.
	c.startRegistrar(0)
	c.startRegistrar(1)
	c.publish("products:1", 9)
	waitFor(t, "v9 after Registrar restart", func() bool { v, _ := ev.get(); return v == 9 })
}

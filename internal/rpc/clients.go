package rpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Kaustubh0117/herald/gen/heraldv1"
	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
)

// callTimeout bounds every RPC. A timeout is just a failed call: every
// caller already retries, and every operation is idempotent.
const callTimeout = 2 * time.Second

// Dial connects lazily (no blocking, no failure if the peer is down yet),
// so processes can start in any order. Reconnects back off from 100 ms up
// to 2 s, so a restarted peer is found again quickly.
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TLS: future work
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 2 * time.Second},
			MinConnectTimeout: time.Second,
		}))
}

func call() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), callTimeout)
}

// MatcherClient implements server.MatcherAPI over gRPC.
type MatcherClient struct{ c pb.MatcherClient }

func NewMatcherClient(conn *grpc.ClientConn) *MatcherClient {
	return &MatcherClient{pb.NewMatcherClient(conn)}
}

func (m *MatcherClient) Publish(id p.ObjectID, v p.Version, src p.ClientID) error {
	ctx, cancel := call()
	defer cancel()
	_, err := m.c.Publish(ctx, &pb.PublishRequest{Items: []*pb.PublishRequest_Item{
		{ObjectId: string(id), Version: int64(v), SourceClient: string(src)},
	}})
	return err
}

func (m *MatcherClient) AddRegistration(id p.ObjectID, c p.ClientID) error {
	ctx, cancel := call()
	defer cancel()
	_, err := m.c.AddRegistration(ctx, &pb.RegistrationRequest{ObjectId: string(id), ClientId: string(c)})
	return err
}

func (m *MatcherClient) RemoveRegistration(id p.ObjectID, c p.ClientID) error {
	ctx, cancel := call()
	defer cancel()
	_, err := m.c.RemoveRegistration(ctx, &pb.RegistrationRequest{ObjectId: string(id), ClientId: string(c)})
	return err
}

func (m *MatcherClient) LatestVersion(id p.ObjectID) (p.Version, bool, error) {
	ctx, cancel := call()
	defer cancel()
	r, err := m.c.LatestVersion(ctx, &pb.LatestVersionRequest{ObjectId: string(id)})
	if err != nil {
		return 0, false, err
	}
	return p.Version(r.Version), r.Known, nil
}

func (m *MatcherClient) Epoch() (uint64, error) {
	ctx, cancel := call()
	defer cancel()
	r, err := m.c.Epoch(ctx, &pb.EpochRequest{})
	if err != nil {
		return 0, err
	}
	return r.Epoch, nil
}

// RegistrarClient implements server.RegistrarAPI over gRPC, plus Handle
// for gateways.
type RegistrarClient struct{ c pb.RegistrarClient }

func NewRegistrarClient(conn *grpc.ClientConn) *RegistrarClient {
	return &RegistrarClient{pb.NewRegistrarClient(conn)}
}

func (r *RegistrarClient) Deliver(c p.ClientID, n p.Notification) error {
	ctx, cancel := call()
	defer cancel()
	_, err := r.c.Deliver(ctx, &pb.DeliverRequest{Items: []*pb.DeliverRequest_Item{
		{ClientId: string(c), Notification: notifToPB(n)},
	}})
	return err
}

func (r *RegistrarClient) Handle(m p.ClientMessage) (p.ServerMessage, error) {
	ctx, cancel := call()
	defer cancel()
	resp, err := r.c.Handle(ctx, clientMessageToPB(m))
	if err != nil {
		return p.ServerMessage{}, err
	}
	return serverMessageFromPB(resp), nil
}

// Matchers and Registrars route by protocol.Owner, like the in-process Cell.
type Matchers []server.MatcherAPI

// Publish is the Bridge's entry point.
func (ms Matchers) Publish(id p.ObjectID, v p.Version, src p.ClientID) error {
	return ms[p.Owner(string(id), len(ms))].Publish(id, v, src)
}

// Router is a gateway's Handler: it sends each client message to the
// client's Registrar. A handshake gets a freshly minted random ID, passed
// on as a token with no cell; the Registrar grants a real token for it.
type Router []*RegistrarClient

func (rs Router) Handle(m p.ClientMessage) (p.ServerMessage, error) {
	if m.Token == nil {
		var b [12]byte
		rand.Read(b[:])
		m.Token = &p.Token{Client: p.ClientID("c-" + hex.EncodeToString(b[:]))}
	}
	return rs[p.Owner(string(m.Token.Client), len(rs))].Handle(m)
}

// Pusher is a Registrar's fast path to gateways (a server.PushFunc). It
// dials each gateway address once and never blocks the caller: a lost push
// is covered by retransmission.
type Pusher struct {
	mu    sync.Mutex
	conns map[string]pb.GatewayClient
}

func NewPusher() *Pusher { return &Pusher{conns: map[string]pb.GatewayClient{}} }

func (ps *Pusher) Push(addr string, c p.ClientID, ns []p.Notification) {
	ps.mu.Lock()
	gc, ok := ps.conns[addr]
	if !ok {
		conn, err := Dial(addr)
		if err != nil {
			ps.mu.Unlock()
			return
		}
		gc = pb.NewGatewayClient(conn)
		ps.conns[addr] = gc
	}
	ps.mu.Unlock()
	go func() {
		ctx, cancel := call()
		defer cancel()
		gc.Push(ctx, &pb.PushRequest{ClientId: string(c), Notifications: notifsToPB(ns)})
	}()
}

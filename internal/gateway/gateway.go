// Package gateway terminates client WebSockets. Gateways are stateless apart
// from open sockets: each forwards client messages to the cell, stamping its
// own address on them; the Registrar stores that address as the client's
// presence, which is how a fast-path push finds the right socket
// (paper Sec. 4.1.1, the addr column).
package gateway

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/Kaustubh0117/herald/internal/metrics"
	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// Frame is one WebSocket message in either direction (JSON). Clients send
// Msg; the gateway replies with Resp, or pushes Push on the fast path.
type Frame struct {
	Msg  *p.ClientMessage `json:",omitempty"`
	Resp *p.ServerMessage `json:",omitempty"`
	Push []p.Notification `json:",omitempty"`
}

// Handler is the gateway's view of a cell: in-process (server.Cell) or the
// client's Registrar over gRPC (rpc.Router). On error the gateway closes the
// socket rather than reply: an empty reply would look like success and make
// the client drop its queued operations. A closed socket is a lost request,
// which the client simply retries.
type Handler interface {
	Handle(m p.ClientMessage) (p.ServerMessage, error)
}

type Gateway struct {
	Addr string
	Cell Handler

	mu    sync.Mutex
	conns map[p.ClientID]*websocket.Conn
}

func New(addr string, cell Handler) *Gateway {
	return &Gateway{Addr: addr, Cell: cell, conns: map[p.ClientID]*websocket.Conn{}}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true}) // dev only
	if err != nil {
		return
	}
	defer c.CloseNow()
	metrics.ClientsOnline.Inc()
	defer metrics.ClientsOnline.Dec()
	ctx := r.Context()

	var me p.ClientID
	defer func() {
		g.mu.Lock()
		if g.conns[me] == c {
			delete(g.conns, me)
		}
		g.mu.Unlock()
	}()

	for {
		var f Frame
		if err := wsjson.Read(ctx, c, &f); err != nil || f.Msg == nil {
			return
		}
		f.Msg.Addr = g.Addr
		resp, err := g.Cell.Handle(*f.Msg)
		if err != nil {
			return
		}
		if t := resp.NewToken; t != nil {
			me = t.Client
		} else if f.Msg.Token != nil {
			me = f.Msg.Token.Client
		}
		if me != "" {
			g.mu.Lock()
			g.conns[me] = c
			g.mu.Unlock()
		}
		stampGateway(resp.Notifications)
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = wsjson.Write(wctx, c, Frame{Resp: &resp})
		cancel()
		if err != nil {
			return
		}
	}
}

// Push is the fast path. It never blocks the caller (a Matcher propagating
// to a Registrar): the write happens in the background, and if it fails or
// the client isn't connected here, retransmission covers it.
func (g *Gateway) Push(client p.ClientID, ns []p.Notification) {
	g.mu.Lock()
	c := g.conns[client]
	g.mu.Unlock()
	if c == nil {
		return
	}
	ns = slices.Clone(ns) // the Registrar may reuse its slice
	stampGateway(ns)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = wsjson.Write(ctx, c, Frame{Push: ns}) // Conn writes are safe concurrently
	}()
}

// stampGateway records when notifications left for the client's socket.
func stampGateway(ns []p.Notification) {
	now := time.Now()
	for i := range ns {
		if ns[i].Stamps[p.StageCommit] != 0 {
			ns[i].Stamps.Stamp(p.StageGateway, now)
		}
	}
}

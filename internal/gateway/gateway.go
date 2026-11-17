// Package gateway terminates client WebSockets. Gateways are stateless apart
// from open sockets: each forwards client messages to the cell, stamping its
// own address on them; the Registrar stores that address as the client's
// presence, which is how a fast-path push finds the right socket
// (paper Sec. 4.1.1, the addr column).
package gateway

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// Frame is one WebSocket message in either direction (JSON). Clients send
// Msg; the gateway replies with Resp, or pushes Push on the fast path.
type Frame struct {
	Msg  *p.ClientMessage `json:",omitempty"`
	Resp *p.ServerMessage `json:",omitempty"`
	Push []p.Notification `json:",omitempty"`
}

// Handler is the gateway's view of a cell (server.Cell satisfies it).
type Handler interface {
	Handle(m p.ClientMessage) p.ServerMessage
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
		resp := g.Cell.Handle(*f.Msg)
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
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := wsjson.Write(wctx, c, Frame{Resp: &resp})
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
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = wsjson.Write(ctx, c, Frame{Push: ns}) // Conn writes are safe concurrently
	}()
}

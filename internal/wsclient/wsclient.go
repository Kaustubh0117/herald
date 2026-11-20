// Package wsclient connects the Go client library to a gateway over a
// WebSocket, speaking the same frames as web/herald-client.js. The load
// generator uses it to stand in for thousands of browser tabs.
package wsclient

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/Kaustubh0117/herald/internal/gateway"
	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// Conn is one client's socket. Send is a client.SendFunc; pushes go to the
// function given to OnPush.
type Conn struct {
	ws    *websocket.Conn
	resps chan p.ServerMessage
	push  atomic.Pointer[func([]p.Notification)]
	done  chan struct{}

	sendMu sync.Mutex // the protocol is one request at a time per client
}

// Dial opens a socket to a gateway (e.g. ws://127.0.0.1:8080/ws).
func Dial(ctx context.Context, url string) (*Conn, error) {
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(1 << 20)
	c := &Conn{ws: ws, resps: make(chan p.ServerMessage, 1), done: make(chan struct{})}
	go c.read()
	return c, nil
}

// OnPush sets where fast-path pushes go, typically client.Client.OnPush.
// Pushes that arrive before it is set are dropped (retransmission covers
// them).
func (c *Conn) OnPush(f func([]p.Notification)) { c.push.Store(&f) }

func (c *Conn) read() {
	defer close(c.done)
	for {
		var f gateway.Frame
		if err := wsjson.Read(context.Background(), c.ws, &f); err != nil {
			return
		}
		switch {
		case f.Resp != nil:
			select {
			case c.resps <- *f.Resp:
			default: // nobody waiting (the request timed out): drop it
			}
		case f.Push != nil:
			if fp := c.push.Load(); fp != nil {
				(*fp)(f.Push)
			}
		}
	}
}

var errClosed = errors.New("wsclient: connection closed")

// Send writes one client message and waits for the gateway's reply.
func (c *Conn) Send(m p.ClientMessage) (p.ServerMessage, error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select { // a late reply to an earlier, timed-out request
	case <-c.resps:
	default:
	}
	if err := wsjson.Write(ctx, c.ws, gateway.Frame{Msg: &m}); err != nil {
		return p.ServerMessage{}, err
	}
	select {
	case r := <-c.resps:
		return r, nil
	case <-c.done:
		return p.ServerMessage{}, errClosed
	case <-ctx.Done():
		return p.ServerMessage{}, ctx.Err()
	}
}

// Close closes the socket.
func (c *Conn) Close() error { return c.ws.Close(websocket.StatusNormalClosure, "") }

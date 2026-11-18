package gateway

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
)

func TestHandshakeRegisterAndFastPath(t *testing.T) {
	var gw *Gateway
	cell := server.NewCell("gw", 1, 1, func(_ string, c p.ClientID, ns []p.Notification) { gw.Push(c, ns) })
	gw = New("gw-test", Local{cell})
	srv := httptest.NewServer(gw)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	roundTrip := func(m p.ClientMessage) p.ServerMessage {
		t.Helper()
		if err := wsjson.Write(ctx, conn, Frame{Msg: &m}); err != nil {
			t.Fatal(err)
		}
		var f Frame
		if err := wsjson.Read(ctx, conn, &f); err != nil || f.Resp == nil {
			t.Fatalf("want a reply frame, got %+v (err %v)", f, err)
		}
		return *f.Resp
	}

	tok := roundTrip(p.ClientMessage{Nonce: 1}).NewToken
	if tok == nil {
		t.Fatal("handshake gave no token")
	}
	regs := map[p.ObjectID]bool{"products:1": true}
	roundTrip(p.ClientMessage{Token: tok, Digest: p.DigestOf(regs), Register: []p.ObjectID{"products:1"}})
	for i := 0; i < 3; i++ {
		cell.Step()
	}

	// The registration recorded this gateway as the client's presence, so a
	// publish should arrive as an unsolicited push frame.
	cell.Publish("products:1", 42, "")
	for i := 0; i < 3; i++ {
		cell.Step()
	}
	var f Frame
	if err := wsjson.Read(ctx, conn, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Push) != 1 || f.Push[0].Version != 42 {
		t.Fatalf("want a push of v42, got %+v", f)
	}
}

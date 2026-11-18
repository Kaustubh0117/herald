package store

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
)

// TestRegistrarRestartCausesNoResyncs is Phase 7's goal: restarting a
// Registrar reloads its clients from Postgres, so none of them notices.
// HERALD_CLIENTS=10000 reproduces the guide's number (default 1000, to keep
// `make test-pg` quick).
func TestRegistrarRestartCausesNoResyncs(t *testing.T) {
	st := openTest(t)
	n := 1000
	if s := os.Getenv("HERALD_CLIENTS"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	cell, err := server.NewCellWithStore("pg", 1, 1, nil, st)
	if err != nil {
		t.Fatal(err)
	}
	handle := func(m p.ClientMessage) p.ServerMessage {
		t.Helper()
		resp, err := cell.Handle(m)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	type client struct {
		tok    *p.Token
		digest p.Digest
	}
	clients := make([]client, n)
	start := time.Now()
	for i := range clients {
		obj := p.ObjectID(fmt.Sprintf("products:%d", i%100))
		tok := handle(p.ClientMessage{Nonce: 1}).NewToken
		clients[i] = client{tok: tok, digest: p.DigestOf(map[p.ObjectID]bool{obj: true})}
		handle(p.ClientMessage{Token: tok, Digest: clients[i].digest, Register: []p.ObjectID{obj}})
	}
	for i := 0; i < 3; i++ {
		cell.Step()
	}
	t.Logf("registered %d clients in %v", n, time.Since(start).Round(time.Millisecond))

	start = time.Now()
	if err := cell.RestartRegistrar(0); err != nil {
		t.Fatal(err)
	}
	t.Logf("restarted the Registrar (one range scan) in %v", time.Since(start).Round(time.Millisecond))

	resyncs := 0
	for _, c := range clients {
		resp := handle(p.ClientMessage{Token: c.tok, Digest: c.digest})
		if resp.NewToken != nil || resp.RequestSync {
			resyncs++
		}
	}
	if resyncs != 0 {
		t.Fatalf("%d of %d clients had to re-sync after a Registrar restart", resyncs, n)
	}
}

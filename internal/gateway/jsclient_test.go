package gateway

import (
	"bufio"
	"context"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	p "github.com/Kaustubh0117/herald/internal/protocol"
	"github.com/Kaustubh0117/herald/internal/server"
	"net/http/httptest"
)

// countingCell counts full syncs so the test can check the browser client
// re-syncs exactly once per new server state.
type countingCell struct {
	*server.Cell
	fullSyncs atomic.Int32
}

func (c *countingCell) Handle(m p.ClientMessage) p.ServerMessage {
	if m.FullSync {
		c.fullSyncs.Add(1)
	}
	return c.Cell.Handle(m)
}

// TestBrowserClientInterop runs web/herald-client.js in Node against the Go
// gateway: it must decode Go's JSON, compute the same digest, receive
// versions, and recover from a full wipe.
func TestBrowserClientInterop(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	var gw *Gateway
	cell := &countingCell{Cell: server.NewCell("js", 2, 2, func(_ string, c p.ClientID, ns []p.Notification) {
		gw.Push(c, ns)
	})}
	gw = New("gw-js", cell)
	srv := httptest.NewServer(gw)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			cell.Step()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	cmd := exec.CommandContext(ctx, node, "../../web/test/driver.mjs",
		"ws"+strings.TrimPrefix(srv.URL, "http"), "products:1")
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = testWriter{t}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { stdin.Close(); cmd.Wait() }()

	lines := make(chan string)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	expect := func(want string) {
		t.Helper()
		for {
			select {
			case l, ok := <-lines:
				if !ok {
					t.Fatalf("node exited before %q", want)
				}
				if l == want {
					return
				}
				t.Logf("node: %s", l)
			case <-ctx.Done():
				t.Fatalf("timed out waiting for %q", want)
			}
		}
	}

	expect("unknown products:1") // registered before any publish
	cell.Publish("products:1", 5, "")
	expect("notify products:1 5")
	if n := cell.fullSyncs.Load(); n != 1 {
		t.Fatalf("want 1 full sync after the handshake, got %d", n)
	}

	cell.Wipe()
	expect("unknown products:1") // lost versions turn into a refetch
	cell.Publish("products:1", 9, "")
	expect("notify products:1 9")
	if n := cell.fullSyncs.Load(); n != 2 {
		t.Fatalf("want exactly one more full sync after the wipe, got %d total", n)
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(b []byte) (int, error) {
	w.t.Logf("node stderr: %s", b)
	return len(b), nil
}

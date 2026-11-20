package rpc

import (
	"testing"
	"time"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// Stamps must survive the trip between processes, or multi-process latency
// measurements lose every stage after the first hop.
func TestStampsSurviveGRPC(t *testing.T) {
	n := p.Notification{Object: "products:1", Version: 9}
	n.Stamps.Stamp(p.StageCommit, time.UnixMicro(100))
	n.Stamps.Stamp(p.StageMatcher, time.UnixMicro(300))
	if back := notifFromPB(notifToPB(n)); back != n {
		t.Fatalf("round trip: got %+v want %+v", back, n)
	}
	if got := len(notifToPB(n).Stamps); got != 2 {
		t.Fatalf("only stamped stages go on the wire: got %d", got)
	}
}

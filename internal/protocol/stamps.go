package protocol

import "time"

// Stage names a point a notification passes on its way to a client. Each
// component stamps the time it handled the notification (paper Sec. 6.3),
// so a client can tell where the delay went (Fig. 9).
type Stage int

const (
	StageCommit    Stage = iota // the application's transaction committed
	StageBridge                 // the Bridge read it from the WAL and published it
	StageMatcher                // a Matcher accepted the version
	StageRegistrar              // a Registrar received it for one client
	StageGateway                // a gateway sent it down the client's socket
	NumStages
)

var stageNames = [NumStages]string{"commit", "bridge", "matcher", "registrar", "gateway"}

func (s Stage) String() string { return stageNames[s] }

// StageByName is the inverse of String; ok is false for unknown names.
func StageByName(name string) (Stage, bool) {
	for i, n := range stageNames {
		if n == name {
			return Stage(i), true
		}
	}
	return 0, false
}

// Stamps holds a unix-microsecond time per stage, 0 meaning not stamped. It
// is an array rather than a slice so Notification stays comparable with ==.
// Stamps are measurement only: they aren't persisted, and a notification
// that wasn't caused by a commit (post-propagation) has none.
type Stamps [NumStages]int64

// Stamp records that stage s happened at t.
func (s *Stamps) Stamp(st Stage, t time.Time) { s[st] = t.UnixMicro() }

// StageDelta is the time a notification took to reach a stage from the
// previous stamped one. The last delta is "client": gateway to app.
type StageDelta struct {
	Stage string
	D     time.Duration
}

// Deltas splits commit-to-received into per-stage times. It returns nil if
// the notification carries no commit stamp. Stages that weren't stamped are
// folded into the next one that was.
func (s Stamps) Deltas(received time.Time) []StageDelta {
	if s[StageCommit] == 0 {
		return nil
	}
	var out []StageDelta
	prev := s[StageCommit]
	for st := StageBridge; st < NumStages; st++ {
		if s[st] == 0 {
			continue
		}
		out = append(out, StageDelta{st.String(), time.Duration(s[st]-prev) * time.Microsecond})
		prev = s[st]
	}
	return append(out, StageDelta{"client", time.Duration(received.UnixMicro()-prev) * time.Microsecond})
}

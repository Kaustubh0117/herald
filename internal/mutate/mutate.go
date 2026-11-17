// Package mutate holds switches that each disable one reliability
// mechanism. bench/mutate.sh turns them on one at a time (via the
// HERALD_MUTATE environment variable) to show that the chaos simulation
// notices: a test that can't fail proves nothing. All are off by default.
package mutate

import (
	"os"
	"slices"
	"strings"
)

var (
	// NoPostPropagation: a registration reaching the Matcher does not
	// produce a notification for the object's current version.
	NoPostPropagation = on("no-post-propagation")

	// NoRegistrationSync: the client never resends its full registration
	// set, neither on a new token nor on a digest mismatch.
	NoRegistrationSync = on("no-registration-sync")

	// NoRetransmission: a pending notification is sent in one reply and
	// then forgotten, without waiting for the client's ack.
	NoRetransmission = on("no-retransmission")

	// MatcherDropsFailed: the Matcher clears a pending object even when
	// delivering it to some Registrar failed.
	MatcherDropsFailed = on("matcher-drops-failed")

	// AckUnregistered: the client acks notifications for objects it is not
	// (yet) registered for. This was a real bug the simulation found: a
	// restarted client acked pushes before restoring its intents.
	AckUnregistered = on("ack-unregistered")
)

func on(name string) bool {
	return slices.Contains(strings.Split(os.Getenv("HERALD_MUTATE"), ","), name)
}

// Package server implements a Herald cell: Matchers (sharded by object),
// Registrars (sharded by client) and the Cell that wires them together.
// Components only talk through MatcherAPI and RegistrarAPI, so Phase 6 can
// swap these direct calls for gRPC without touching the logic.
package server

import (
	"errors"

	p "github.com/Kaustubh0117/herald/internal/protocol"
)

// MatcherAPI is what Registrars and the Bridge call on a Matcher.
// Every method is idempotent, so callers handle failure by retrying.
type MatcherAPI interface {
	Publish(id p.ObjectID, v p.Version, source p.ClientID, stamps ...p.Stamps) error
	AddRegistration(id p.ObjectID, c p.ClientID) error
	RemoveRegistration(id p.ObjectID, c p.ClientID) error
	LatestVersion(id p.ObjectID) (v p.Version, known bool, err error)
	// Epoch identifies this incarnation; it changes when the Matcher
	// restarts empty, which tells Registrars to re-send registrations.
	Epoch() (uint64, error)
}

// RegistrarAPI is what Matchers call on a Registrar. (Clients reach a
// Registrar's Handle through a gateway, not through this interface.)
type RegistrarAPI interface {
	Deliver(c p.ClientID, n p.Notification) error
}

// PushFunc is the unreliable fast path: send notifications to the gateway
// at addr that holds client c's connection. It may silently drop them;
// retransmission in Registrar.Handle covers the loss (paper Sec. 4.2.3).
type PushFunc func(addr string, c p.ClientID, ns []p.Notification)

// ErrInjected is returned by calls the Cell's fault injector chose to fail.
var ErrInjected = errors.New("herald: injected fault")

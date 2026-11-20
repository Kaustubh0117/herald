// Package metrics holds Herald's Prometheus metrics (build guide Phase 10).
// They register with the default registry, so any process can serve them
// with promhttp.Handler(); each process only moves the ones it owns.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ScratchBuffered counts versions parked in the scratch table because
	// their Matcher row was unwritable: Herald's version of the paper's
	// Fig. 11 (a month of buffering events in production).
	ScratchBuffered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "herald_scratch_buffered_total",
		Help: "Versions parked in scratch during a partial storage outage.",
	})

	// ScratchDrained counts parked versions moved into the Matcher table.
	// buffered - drained is (roughly) the current scratch backlog.
	ScratchDrained = promauto.NewCounter(prometheus.CounterOpts{
		Name: "herald_scratch_drained_total",
		Help: "Parked versions moved from scratch into the Matcher table.",
	})

	// SlotLagBytes is how much WAL Postgres keeps for the Bridge's slot:
	// the early-warning signal for everything downstream.
	SlotLagBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "herald_bridge_slot_lag_bytes",
		Help: "WAL bytes retained for the Bridge's replication slot.",
	})

	PublishTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "herald_publish_total",
		Help: "Versions the Bridge published to a Matcher.",
	})

	PublishRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "herald_publish_retries_total",
		Help: "Failed publish attempts the Bridge retried.",
	})

	// Resyncs counts clients told to resend their registrations, by why.
	Resyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "herald_resyncs_total",
		Help: "Registration resyncs requested, by reason (new_token, digest_mismatch, zombie).",
	}, []string{"reason"})

	// UnknownNotifications spike after wipes and migrations: the price of
	// soft state, paid in refetches instead of lost updates.
	UnknownNotifications = promauto.NewCounter(prometheus.CounterOpts{
		Name: "herald_unknown_notifications_total",
		Help: "Unknown-version notifications created by post-propagation.",
	})

	ClientsOnline = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "herald_clients_online",
		Help: "Open client WebSocket connections on this gateway.",
	})
)

// PendingNotifications exports the unacked backlog, computed by f when
// Prometheus scrapes (not on every change). Call it once per process.
func PendingNotifications(f func() float64) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "herald_registrar_pending_notifications",
		Help: "Unacked notifications held by this process's Registrars.",
	}, f)
}

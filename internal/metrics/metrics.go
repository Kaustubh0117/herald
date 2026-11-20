// Package metrics holds Herald's Prometheus metrics. They register with the
// default registry, so any process can serve them with promhttp.Handler().
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
)

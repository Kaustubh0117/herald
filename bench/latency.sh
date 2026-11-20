#!/usr/bin/env bash
# Latency budget (paper Fig. 9): where does the time go between a commit
# and the client? Sweeps the propagator batching window and records the
# per-stage delay from the notifications' stage stamps.
#
#   bench/latency.sh            # ~3 minutes; writes docs/charts/latency-budget.png
set -euo pipefail
cd "$(dirname "$0")/.."
. bench/lib.sh
CLIENTS=${CLIENTS:-1000}
RATE=${RATE:-100}
DURATION=${DURATION:-30s}
WARMUP=${WARMUP:-10s}
OUT=bench/results/latency.csv

build
rm -f "$OUT"
for step in 1ms 20ms 100ms 500ms; do
  echo "== batching window $step"
  drop_slot
  start_dev -step "$step"
  bin/herald-load -url "ws://127.0.0.1:$PORT/ws" -pg "$PG" -clients "$CLIENTS" -rate "$RATE" \
    -warmup "$WARMUP" -duration "$DURATION" -label "$step" -out "$OUT"
  stop_dev
done
plot latency "$OUT" docs/charts/latency-budget.png \
  "$CLIENTS clients x 5 objects, $RATE updates/s, $DURATION per run; $(hardware)"

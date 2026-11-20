#!/usr/bin/env bash
# Storage brown-out (paper Fig. 11): a quarter of the Matcher key space
# becomes unwritable for a minute while updates keep flowing. Unwritable
# versions are parked in scratch, so the slot keeps moving and the other
# objects keep notifying; the parked versions drain when the outage ends.
#
#   bench/brownout.sh           # ~3 minutes; writes docs/charts/brownout.png
set -euo pipefail
cd "$(dirname "$0")/.."
. bench/lib.sh
CLIENTS=${CLIENTS:-2000}
OBJECTS=${OBJECTS:-1000}
RATE=${RATE:-200}
OUT=bench/results/brownout.csv

build
drop_slot
start_dev -store
bin/herald-load -url "ws://127.0.0.1:$PORT/ws" -pg "$PG" -clients "$CLIENTS" -objects "$OBJECTS" \
  -rate "$RATE" -warmup 10s -duration 150s -series "$OUT" -scrape "http://127.0.0.1:$PORT/metrics" \
  -outage-url "http://127.0.0.1:$PORT/admin/outage" -outage-at 30s -outage-for 60s -outage-fraction 0.25
curl -sf -XPOST "localhost:$PORT/admin/outage?fraction=0" >/dev/null || true
plot brownout "$OUT" docs/charts/brownout.png \
  "25% of Matcher keys unwritable from 30 s to 90 s; $CLIENTS clients x 5 objects over $OBJECTS rows, $RATE updates/s; $(hardware)"

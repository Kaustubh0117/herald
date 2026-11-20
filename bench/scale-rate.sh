#!/usr/bin/env bash
# Scale with notification rate (paper Fig. 7): fixed clients, ramp the
# UPDATE rate; record the cell's CPU and the delay.
#
#   bench/scale-rate.sh         # ~7 minutes; writes docs/charts/scale-rate.png
set -euo pipefail
cd "$(dirname "$0")/.."
. bench/lib.sh
CLIENTS=${CLIENTS:-10000}
OBJECTS=${OBJECTS:-10000}
RATES=${RATES:-"100 500 1000 2000 5000"}
OUT=bench/results/scale-rate.csv

build
rm -f "$OUT"
for r in $RATES; do
  sleep 35 # let earlier runs' sockets leave TIME_WAIT (also a previous script's)
  echo "== $r updates/s"
  drop_slot
  start_dev
  bin/herald-load -url "ws://127.0.0.1:$PORT/ws" -pg "$PG" -clients "$CLIENTS" -objects "$OBJECTS" \
    -rate "$r" -warmup 10s -duration 30s -cpu-pid "$DEV_PID" -label "$r" -out "$OUT"
  stop_dev
done
plot rate "$OUT" docs/charts/scale-rate.png \
  "$CLIENTS clients x 5 objects over $OBJECTS rows (~5 notifications per update), 30 s per point; $(hardware)"

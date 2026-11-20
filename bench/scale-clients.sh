#!/usr/bin/env bash
# Scale with clients (paper Fig. 6): ramp the number of connected clients at
# a fixed update rate; record the cell's CPU and memory and the delay.
#
#   bench/scale-clients.sh      # ~7 minutes; writes docs/charts/scale-clients.png
#
# One process can open at most ~16k sockets to one address (macOS's ephemeral
# port range is 49152-65535), so the ramp stops at 15k. Runs are spaced 35 s
# apart so earlier sockets leave TIME_WAIT.
set -euo pipefail
cd "$(dirname "$0")/.."
. bench/lib.sh
COUNTS=${COUNTS:-"1000 2000 5000 10000 15000"}
RATE=${RATE:-100}
OBJECTS=${OBJECTS:-1000}
OUT=bench/results/scale-clients.csv

build
rm -f "$OUT"
for n in $COUNTS; do
  sleep 35 # let earlier runs' sockets leave TIME_WAIT (also a previous script's)
  echo "== $n clients"
  drop_slot
  start_dev
  bin/herald-load -url "ws://127.0.0.1:$PORT/ws" -pg "$PG" -clients "$n" -objects "$OBJECTS" \
    -rate "$RATE" -heartbeat 30s -warmup 10s -duration 30s -cpu-pid "$DEV_PID" -label "$n" -out "$OUT"
  stop_dev
done
plot clients "$OUT" docs/charts/scale-clients.png \
  "5 objects each over $OBJECTS rows, $RATE updates/s, heartbeat 30 s, 30 s per point; $(hardware)"

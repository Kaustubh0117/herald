#!/usr/bin/env bash
# Runs a Herald cell as separate local processes (bridge, 2 matchers,
# 2 registrars, gateway), connects the browser client through Node, then
# kills and restarts each kind of process in turn. After every restart, a
# real SQL UPDATE must still reach the client. Exits non-zero if one doesn't.
#
#   PG=postgres://... PSQL=psql bench/cluster.sh
#   STORE=1 bench/cluster.sh   # matchers and registrars persist their state:
#                              # restarts must also cause no client refetches
set -euo pipefail
cd "$(dirname "$0")/.."
PG=${PG:-postgres://postgres:herald@127.0.0.1:5432/postgres}
PSQL=${PSQL:-psql}
CFG=deploy/partitions.local.yaml
SLOT=herald_cluster
STORE=${STORE:-}
STATE=()
if [ -n "$STORE" ]; then STATE=(-store -pg "$PG"); fi
LOGS=$(mktemp -d)

go build -o bin/herald ./cmd/herald

# One pid file per process (macOS ships bash 3.2: no associative arrays).
start() { # start NAME ARGS... : run one process, logging to $LOGS/NAME.log
  local name=$1; shift
  bin/herald "$@" -config "$CFG" >>"$LOGS/$name.log" 2>&1 &
  echo $! >"$LOGS/$name.pid"
}
crash() { # crash NAME: kill -9, like a machine dying
  local pid; pid=$(cat "$LOGS/$1.pid")
  kill -9 "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
}
restart() { crash "$1"; start "$@"; }

cleanup() {
  kill $(cat "$LOGS"/*.pid 2>/dev/null) 2>/dev/null || true
  pkill -P $$ 2>/dev/null || true # the client and the sleep feeding its stdin
  wait 2>/dev/null || true
  "$PSQL" "$PG" -qAtc "SELECT pg_drop_replication_slot('$SLOT') FROM pg_replication_slots WHERE slot_name = '$SLOT'" >/dev/null || true
  if [ -n "$STORE" ]; then "$PSQL" "$PG" -qc 'DROP SCHEMA IF EXISTS "herald_cell-a" CASCADE' >/dev/null 2>&1 || true; fi
  echo "logs: $LOGS"
}
trap cleanup EXIT

if [ -n "$STORE" ]; then bin/herald wipe -config "$CFG" -pg "$PG" 2>/dev/null; fi # start from empty state
start bridge     bridge -pg "$PG" -slot "$SLOT" -demo
start matcher0   matcher -i 0 ${STATE[@]+"${STATE[@]}"}
start matcher1   matcher -i 1 ${STATE[@]+"${STATE[@]}"}
start registrar0 registrar -i 0 ${STATE[@]+"${STATE[@]}"}
start registrar1 registrar -i 1 ${STATE[@]+"${STATE[@]}"}
start gateway    gateway -listen :8090 -grpc 127.0.0.1:7100 -pg "$PG"
sleep 2

(sleep 600) | node web/test/driver.mjs ws://127.0.0.1:8090/ws 'products:1' >"$LOGS/client.log" 2>&1 &
sleep 1

notifies() { grep -c '^notify products:1 ' "$LOGS/client.log" || true; }
check() { # check LABEL: an UPDATE must reach the client within 10 s
  local before; before=$(notifies)
  "$PSQL" "$PG" -qc "UPDATE products SET price = price + 1 WHERE id = 1"
  for _ in $(seq 100); do
    if [ "$(notifies)" -gt "$before" ]; then printf '  ok   %s\n' "$1"; return; fi
    sleep 0.1
  done
  printf '  FAIL %s\n' "$1"; tail -5 "$LOGS/client.log"; exit 1
}

unknowns() { grep -c '^unknown products:1' "$LOGS/client.log" || true; }

check "steady state"
before=$(unknowns)
restart matcher0 matcher -i 0 ${STATE[@]+"${STATE[@]}"}; restart matcher1 matcher -i 1 ${STATE[@]+"${STATE[@]}"}; sleep 1
check "after killing both matchers"
restart registrar0 registrar -i 0 ${STATE[@]+"${STATE[@]}"}; restart registrar1 registrar -i 1 ${STATE[@]+"${STATE[@]}"}; sleep 1
check "after killing both registrars"
if [ -n "$STORE" ]; then # with persistent state, those restarts were invisible
  if [ "$(unknowns)" -ne "$before" ]; then
    printf '  FAIL restarts with -store caused %d refetch signals\n' "$(( $(unknowns) - before ))"; exit 1
  fi
  printf '  ok   restarts with -store caused no refetch signals\n'
fi
crash bridge
"$PSQL" "$PG" -qc "UPDATE products SET price = price + 1 WHERE id = 1"   # written while the bridge is down
start bridge bridge -pg "$PG" -slot "$SLOT"; sleep 1
check "after killing the bridge"
restart gateway gateway -listen :8090 -grpc 127.0.0.1:7100 -pg "$PG"; sleep 2
check "after killing the gateway"
echo "all checks passed; client saw $(notifies) updates"

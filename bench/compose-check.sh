#!/usr/bin/env bash
# Crash test for the docker-compose cell: connects the browser client (via
# Node) to the gateway, then restarts each Herald service with no grace
# period. After every restart, a SQL UPDATE must still reach the client.
#
#   docker compose -f deploy/docker-compose.yml up --build -d
#   PSQL=psql bench/compose-check.sh
set -euo pipefail
cd "$(dirname "$0")/.."
PG=${PG:-postgres://postgres:herald@127.0.0.1:5433/postgres}
PSQL=${PSQL:-psql}
DC="docker compose -f deploy/docker-compose.yml"
LOG=$(mktemp)

cleanup() { pkill -P $$ 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT

(sleep 600) | node web/test/driver.mjs ws://127.0.0.1:8080/ws 'products:1' >"$LOG" 2>&1 &
sleep 2

notifies() { grep -c '^notify products:1 ' "$LOG" || true; }
check() { # check LABEL: an UPDATE must reach the client within 15 s
  local before; before=$(notifies)
  "$PSQL" "$PG" -qc "UPDATE products SET price = price + 1 WHERE id = 1"
  for _ in $(seq 150); do
    if [ "$(notifies)" -gt "$before" ]; then printf '  ok   %s\n' "$1"; return; fi
    sleep 0.1
  done
  printf '  FAIL %s\n' "$1"; tail -5 "$LOG"; exit 1
}
crash() { $DC restart -t 0 "$@" >/dev/null 2>&1; sleep 2; } # -t 0: SIGKILL, no clean shutdown

check "steady state"
crash matcher-0;                 check "after crashing matcher-0"
crash registrar-0 registrar-1;   check "after crashing both registrars"
crash bridge;                    check "after crashing the bridge"
crash gateway;                   check "after crashing the gateway"
echo "all checks passed; client saw $(notifies) updates"

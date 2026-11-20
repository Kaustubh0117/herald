# Shared helpers for the experiment scripts (bash 3.2 compatible).
PG=${PG:-${HERALD_PG:-postgres://postgres:herald@127.0.0.1:5432/postgres}}
PSQL=${PSQL:-psql}
PORT=${PORT:-8099}
SLOT=${SLOT:-herald_bench}

hardware() { # one line for chart subtitles and the README
  printf '%s, %s cores, %s GB RAM, macOS %s' \
    "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)" \
    "$(sysctl -n hw.ncpu 2>/dev/null || nproc)" \
    "$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 ))" \
    "$(sw_vers -productVersion 2>/dev/null || uname -r)"
}

build() { go build -o bin/herald-dev ./cmd/herald-dev && go build -o bin/herald-load ./cmd/herald-load; }

start_dev() { # start_dev [herald-dev flags...]: one cell on $PORT, Bridge on $SLOT
  bin/herald-dev -addr ":$PORT" -pg "$PG" -slot "$SLOT" "$@" >"$WORK/dev.log" 2>&1 &
  DEV_PID=$!
  for _ in $(seq 50); do curl -sf "localhost:$PORT/metrics" >/dev/null && return; sleep 0.2; done
  echo "herald-dev did not start:"; cat "$WORK/dev.log"; exit 1
}

stop_dev() { kill "$DEV_PID" 2>/dev/null || true; wait "$DEV_PID" 2>/dev/null || true; }

drop_slot() { # a leftover slot makes Postgres keep WAL forever
  "$PSQL" "$PG" -qAtc "SELECT pg_drop_replication_slot('$SLOT') FROM pg_replication_slots WHERE slot_name = '$SLOT'" >/dev/null || true
}

WORK=$(mktemp -d)
cleanup() { stop_dev; pkill -P $$ 2>/dev/null || true; drop_slot; }
trap cleanup EXIT

plot() { # plot ARGS...: runs bench/plot.py with a Python that has matplotlib
  local py
  for py in ${PYTHON:-} python3 /usr/bin/python3; do
    if "$py" -c 'import matplotlib' 2>/dev/null; then "$py" bench/plot.py "$@"; return; fi
  done
  echo "no Python with matplotlib found: pip3 install matplotlib (or set PYTHON=...)"; return 1
}

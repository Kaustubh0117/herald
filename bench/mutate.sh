#!/usr/bin/env bash
# Mutation testing: disable one reliability mechanism at a time and show the
# chaos simulation catches it. A row with 0 violations would mean the test
# can't tell that mechanism is missing.
set -euo pipefail
cd "$(dirname "$0")/.."
SEEDS=${SEEDS:-200}

count() {
  HERALD_MUTATE="$1" go test -count=1 -v -run '^TestChaos$' ./internal/sim/ -seeds="$SEEDS" 2>&1 |
    sed -n 's/.*total violations over [0-9]* seeds: \([0-9]*\).*/\1/p'
}

printf '%-24s %s\n' "mutation" "violations ($SEEDS seeds)"
for m in none no-post-propagation no-registration-sync no-retransmission matcher-drops-failed no-matcher-resync ack-unregistered; do
  printf '%-24s %s\n' "$m" "$(count "$m")"
done

# Herald

Herald is a self-healing cache-invalidation service for Postgres-backed applications.
When a row changes, every browser tab and backend service that cached it hears about it
within a second. The application needs no `publish()` calls, because Herald reads changes
straight from Postgres's write-ahead log. You can also delete all of Herald's own state
while it's running without losing an update. It is a re-implementation of Google's
[Thialfi](https://research.google/pubs/thialfi-a-client-notification-service-for-internet-scale-applications/)
(Adya, Cooper, Myers, Piatek, SOSP 2011).

**Status:** under construction.

## Correctness

**Property** (paper §5): if a well-behaved client registers for an object, it always
eventually learns that object's latest version. Duplicates, reordering and skipped
intermediate versions are allowed.

**Chaos simulation** ([internal/sim](internal/sim/sim_test.go)): one cell with 3 Registrars,
3 Matchers and 30 clients watching 20 objects, for 400 rounds. It drops 20% of client
requests, client replies and fast-path pushes, and fails 10% of internal RPCs. Clients
restart at random, and **all server state is wiped twice**. After the network heals,
every client must hold the latest version of everything it wants. 0 violations over
500 seeds; the default 20 seeds run in under a second and give the same result every
time for a given seed.

**Mutation testing** ([bench/mutate.sh](bench/mutate.sh)) shows the test can fail: each
row turns off one mechanism.

| Mechanism removed | Violations (20 seeds) | What it shows |
|---|---:|---|
| none | 0 | baseline |
| Registrar post-propagation | 99 | clients that register after the last change never learn it |
| Registration sync (full resend on new token / digest mismatch) | 2,903 | after a wipe, servers never relearn registrations |
| Retransmission of unacked notifications | 101 | the fast path alone loses updates |
| Matcher keeps failed deliveries pending | 239 | internal RPC failures silently drop updates |
| Client acks only registered objects | 4 | a real bug the simulation found: a restarted client acked pushes before restoring its registrations |

## Develop

```sh
make test      # unit tests, no database needed
make test-pg   # also runs Postgres tests (needs wal_level=logical; see deploy/docker-compose.yml)
bench/mutate.sh  # mutation table above
```

## Postgres setup

```sql
-- once per database: which tables Herald watches
CREATE PUBLICATION herald_pub FOR TABLE products, orders;   -- or FOR ALL TABLES
-- once per Herald cell: its durable cursor into the WAL
SELECT pg_create_logical_replication_slot('herald_cell_a', 'pgoutput');
-- watch how much WAL a stalled Bridge is forcing Postgres to keep
SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots;
```

Tables need a primary key so updates and deletes carry it. Each row change becomes
`table:pk` and `table:*` notifications versioned by the transaction's commit LSN.

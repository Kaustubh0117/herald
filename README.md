# Herald

Herald is a self-healing cache-invalidation service for Postgres-backed applications.
When a row changes, every browser tab and backend service that cached it hears about it
within a second. The application needs no `publish()` calls, because Herald reads changes
straight from Postgres's write-ahead log. You can also delete all of Herald's own state
while it's running without losing an update. It is a re-implementation of Google's
[Thialfi](https://research.google/pubs/thialfi-a-client-notification-service-for-internet-scale-applications/)
(Adya, Cooper, Myers, Piatek, SOSP 2011).

**Status:** under construction.

## Quick start

```sh
make dev                  # one-process cell + Bridge + demo store on :8080
open http://localhost:8080
psql -d postgres -c "UPDATE products SET price = 12 WHERE id = 1"   # the page updates live
```

`make dev` creates a demo `products` table, a publication and a replication slot
(`herald_dev`) in the database named by `PG`. The slot keeps WAL while herald-dev is
stopped; drop it when you're done: `SELECT pg_drop_replication_slot('herald_dev');`.

### As separate processes

```sh
bench/cluster.sh   # bridge, 2 matchers, 2 registrars, gateway as local processes;
                   # kills and restarts each kind, checking a SQL UPDATE still arrives
docker compose -f deploy/docker-compose.yml up --build -d # the same cell in containers:
                   # demo on :8080, its Postgres on host port 5433
bench/compose-check.sh   # crashes each container in turn, checking an UPDATE still arrives
```

Processes talk over gRPC ([proto/herald.proto](proto/herald.proto)) and find each other
through a static partition map ([deploy/partitions.yaml](deploy/partitions.yaml)).

## Correctness

**Property** (paper §5): if a well-behaved client registers for an object, it always
eventually learns that object's latest version. Duplicates, reordering and skipped
intermediate versions are allowed.

**Chaos simulation** ([internal/sim](internal/sim/sim_test.go)): one cell with 3 Registrars,
3 Matchers and 30 clients watching 20 objects, for 400 rounds. It drops 20% of client
requests, client replies and fast-path pushes, and fails 10% of internal RPCs. Clients
restart at random, individual Matchers and Registrars crash and come back empty, and
**all server state is wiped twice**. After the network heals, every client must hold
the latest version of everything it wants. The default 200 seeds run in under a second
with 0 violations, and a given seed always gives the same result.

**Mutation testing** ([bench/mutate.sh](bench/mutate.sh)) shows the test can fail: each
row turns off one mechanism.

| Mechanism removed | Violations (200 seeds) | What it shows |
|---|---:|---|
| none | 0 | baseline |
| Registrar post-propagation | 1,874 | clients that register after the last change never learn it |
| Registration sync (full resend on new token / digest mismatch) | 29,114 | after a wipe, servers never relearn registrations |
| Retransmission of unacked notifications | 890 | the fast path alone loses updates |
| Matcher keeps failed deliveries pending | 2,149 | internal RPC failures silently drop updates |
| Registrars re-send registrations to a restarted Matcher | 4,869 | a Matcher that crashes forgets who is registered |
| Client acks only registered objects | 15 | a real bug the simulation found: a restarted client acked pushes before restoring its registrations |

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

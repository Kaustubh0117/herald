#!/bin/sh
# The Bridge connects for logical replication from another container; the
# image's default pg_hba.conf only allows that from inside this one.
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"

// Package pgtest checks that the test database is set up the way Herald
// needs it. Tests that need Postgres skip unless HERALD_PG is set, so plain
// `make test` works on a machine without a database.
package pgtest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestLogicalReplicationEnabled(t *testing.T) {
	dsn := os.Getenv("HERALD_PG")
	if dsn == "" {
		t.Skip("HERALD_PG not set; run `make test-pg`")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	var level string
	if err := conn.QueryRow(ctx, "SHOW wal_level").Scan(&level); err != nil {
		t.Fatal(err)
	}
	if level != "logical" {
		t.Fatalf("wal_level = %q, want logical (Phase 5 needs it)", level)
	}
}

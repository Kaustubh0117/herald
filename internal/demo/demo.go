// Package demo is a stand-in for "your application": a products table and
// a tiny read API. Herald itself never touches this data; it only reads the
// table's changes from the WAL and tells clients to refetch from here.
package demo

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kaustubh0117/herald/internal/bridge"
)

// Setup creates the demo table, publication and replication slot if
// missing. Run it from one process only: CREATE PUBLICATION isn't
// idempotent under concurrency.
func Setup(ctx context.Context, db *pgxpool.Pool, slot, pub string) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS products (id int PRIMARY KEY, name text NOT NULL, price int NOT NULL)`,
		`INSERT INTO products VALUES (1, 'Lamp', 10), (2, 'Desk', 90), (3, 'Chair', 45), (4, 'Shelf', 60)
		 ON CONFLICT DO NOTHING`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			return err
		}
	}
	var exists bool
	if err := db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, pub).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		// Identifiers can't be bound as parameters; pub comes from our own flag.
		if _, err := db.Exec(ctx, `CREATE PUBLICATION `+pub+` FOR TABLE products`); err != nil {
			return err
		}
	}
	conn, err := db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return bridge.EnsureSlot(ctx, conn.Conn(), slot)
}

type product struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Price int    `json:"price"`
}

// Products serves GET /api/products: the "refetch" target for the demo page.
func Products(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.Query(r.Context(), `SELECT id, name, price FROM products ORDER BY id`)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		out := []product{}
		for rows.Next() {
			var pr product
			if err := rows.Scan(&pr.ID, &pr.Name, &pr.Price); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			out = append(out, pr)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	}
}

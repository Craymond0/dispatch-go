// Package testdb gives each test package its own throwaway database so
// `go test ./...` can run packages in parallel without them truncating each
// other's tables. It needs TEST_DATABASE_URL pointing at a server where that
// role may CREATE DATABASE; tests skip when it is unset.
package testdb

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Open returns a pool on a database named after the base database plus
// suffix, creating it on first use. The pool is closed when the test ends.
func Open(t *testing.T, suffix string) *pgxpool.Pool {
	t.Helper()
	raw := os.Getenv("TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("TEST_DATABASE_URL not set; integration tests need a disposable Postgres")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	base := strings.TrimPrefix(u.Path, "/")
	name := base + "_" + suffix
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create %s: %v", name, err)
	}
	u.Path = "/" + name
	db, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db
}

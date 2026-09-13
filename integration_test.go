package main

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestPostgresReliability(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("isolated test database required")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	// Dedicated test database only; do not point this variable at user data.
	if _, err = db.Exec(ctx, "TRUNCATE events,jobs RESTART IDENTITY"); err != nil {
		t.Fatal(err)
	}
	a := App{db}
	t.Setenv("DEMO_MODE", "1")
	submit := func(body string, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/jobs", strings.NewReader(body))
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		a.create(w, r)
		return w
	}
	if w := submit(`{"description":"Go PostgreSQL"}`, "same"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if w := submit(`{"description":"Go PostgreSQL"}`, "same"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if w := submit(`{"description":"Python"}`, "same"); w.Code != 409 {
		t.Fatal("key conflict not rejected")
	}
	var n int
	db.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&n)
	if n != 1 {
		t.Fatal("duplicate inserted")
	}
	var wg sync.WaitGroup
	claimed := make(chan Job, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, e := a.claim(ctx)
			if e == nil {
				claimed <- j
			}
		}()
	}
	wg.Wait()
	close(claimed)
	var first Job
	for j := range claimed {
		n--
		first = j
	}
	if n != 0 || first.Attempts != 1 {
		t.Fatal("claim not exclusive")
	}
	db.Exec(ctx, "UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=$1", first.ID)
	second, err := a.claim(ctx)
	if err != nil || second.Attempts != 2 {
		t.Fatal("expired job not recovered", err)
	}
	if a.finish(ctx, first, json.RawMessage(`{}`), "") == nil {
		t.Fatal("stale completion accepted")
	}
	if err = a.finish(ctx, second, nil, "test failure"); err != nil {
		t.Fatal(err)
	}
	var state string
	db.QueryRow(ctx, "SELECT state FROM jobs WHERE id=$1", first.ID).Scan(&state)
	if state != "queued" {
		t.Fatal(state)
	}
	db.Exec(ctx, "UPDATE jobs SET available_at=now() WHERE id=$1", first.ID)
	third, err := a.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.finish(ctx, third, nil, "test failure"); err != nil {
		t.Fatal(err)
	}
	db.QueryRow(ctx, "SELECT state FROM jobs WHERE id=$1", first.ID).Scan(&state)
	if state != "failed" {
		t.Fatal("retry limit not enforced")
	}
	w := submit(`{"description":"Python"}`, "success")
	if w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	j, err := a.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.finish(ctx, j, []byte(`{"ok":true}`), ""); err != nil {
		t.Fatal(err)
	}
	db.QueryRow(ctx, "SELECT state FROM jobs WHERE id=$1", j.ID).Scan(&state)
	if state != "succeeded" {
		t.Fatal(state)
	}
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
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
	reg := newRegistry()
	s := server{App: a, reg: reg}
	t.Setenv("DEMO_MODE", "1")
	submit := func(body string, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/jobs", strings.NewReader(body))
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		s.create(w, r)
		return w
	}
	if w := submit(`{"payload":{"description":"Go PostgreSQL"}}`, "same"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	// Same input, different key order: must be treated as the same submission.
	if w := submit(`{"type":"analyze","payload":{"description":"Go PostgreSQL"}}`, "same"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if w := submit(`{"payload":{"description":"Python"}}`, "same"); w.Code != 409 {
		t.Fatal("key conflict not rejected")
	}
	if w := submit(`{"type":"nope","payload":{}}`, "unknown"); w.Code != 400 || !strings.Contains(w.Body.String(), "known_types") {
		t.Fatal("unknown type not rejected:", w.Body.String())
	}
	if w := submit(`{"payload":{"description":"x","demo_delay_seconds":99}}`, "bad"); w.Code != 400 {
		t.Fatal("handler validation not applied:", w.Body.String())
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
	if a.finish(ctx, first, json.RawMessage(`{}`), nil) == nil {
		t.Fatal("stale completion accepted")
	}
	if err = a.finish(ctx, second, nil, errors.New("test failure")); err != nil {
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
	if err = a.finish(ctx, third, nil, errors.New("test failure")); err != nil {
		t.Fatal(err)
	}
	db.QueryRow(ctx, "SELECT state FROM jobs WHERE id=$1", first.ID).Scan(&state)
	if state != "failed" {
		t.Fatal("retry limit not enforced")
	}
	w := submit(`{"payload":{"description":"Python"}}`, "success")
	if w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	j, err := a.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.finish(ctx, j, []byte(`{"ok":true}`), nil); err != nil {
		t.Fatal(err)
	}
	db.QueryRow(ctx, "SELECT state FROM jobs WHERE id=$1", j.ID).Scan(&state)
	if state != "succeeded" {
		t.Fatal(state)
	}

	// A terminal error fails on the first attempt with no retry scheduled.
	if w := submit(`{"payload":{"description":"Go"}}`, "terminal"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	j, err = a.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.finish(ctx, j, nil, Terminal(errors.New("404 gone"))); err != nil {
		t.Fatal(err)
	}
	var attempts int
	var event string
	db.QueryRow(ctx, "SELECT state,attempts FROM jobs WHERE id=$1", j.ID).Scan(&state, &attempts)
	db.QueryRow(ctx, "SELECT event FROM events WHERE job_id=$1 ORDER BY id DESC LIMIT 1", j.ID).Scan(&event)
	if state != "failed" || attempts != 1 || event != "failed_terminal" {
		t.Fatalf("terminal error retried: state=%s attempts=%d event=%s", state, attempts, event)
	}
	if _, err = a.claim(ctx); err == nil {
		t.Fatal("terminally failed job was claimable")
	}

	// A job whose type has no handler in this binary is terminal too.
	db.QueryRow(ctx, `INSERT INTO jobs(idempotency_key,type,payload) VALUES('orphan','vanished','{}') RETURNING id`).Scan(&j.ID)
	j, err = a.claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a.runOne(ctx, reg, j)
	db.QueryRow(ctx, "SELECT state,attempts,error FROM jobs WHERE id=$1", j.ID).Scan(&state, &attempts, &event)
	if state != "failed" || attempts != 1 || !strings.Contains(event, "no handler") {
		t.Fatalf("missing handler not terminal: %s %d %q", state, attempts, event)
	}

	// The real handler path end to end: claim, run, succeed.
	if w := submit(`{"payload":{"description":"Go and Docker"}}`, "e2e"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	j, _ = a.claim(ctx)
	a.runOne(ctx, reg, j)
	var result []byte
	db.QueryRow(ctx, "SELECT state,result FROM jobs WHERE id=$1", j.ID).Scan(&state, &result)
	if state != "succeeded" || !strings.Contains(string(result), `"Docker"`) {
		t.Fatalf("handler result not stored: %s %s", state, result)
	}
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strconv"
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
	if _, err = db.Exec(ctx, "TRUNCATE events,job_dependencies,jobs RESTART IDENTITY"); err != nil {
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

func TestDependencies(t *testing.T) {
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
	if _, err = db.Exec(ctx, "TRUNCATE events,job_dependencies,jobs RESTART IDENTITY"); err != nil {
		t.Fatal(err)
	}
	a := App{db}
	s := server{App: a, reg: newRegistry()}
	t.Setenv("DEMO_MODE", "1")
	submit := func(body, key string) (int64, int) {
		r := httptest.NewRequest("POST", "/jobs", strings.NewReader(body))
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		s.create(w, r)
		var resp struct{ ID int64 }
		json.Unmarshal(w.Body.Bytes(), &resp)
		return resp.ID, w.Code
	}
	state := func(id int64) (st string, pending int) {
		db.QueryRow(ctx, "SELECT state,pending_deps FROM jobs WHERE id=$1", id).Scan(&st, &pending)
		return
	}
	claimIDs := func() map[int64]Job {
		got := map[int64]Job{}
		for {
			j, err := a.claim(ctx)
			if err != nil {
				return got
			}
			got[j.ID] = j
		}
	}

	// Fan-out: three independent children, one fan-in that waits for all.
	ida, _ := submit(`{"payload":{"description":"a"}}`, "a")
	idb, _ := submit(`{"payload":{"description":"b"}}`, "b")
	idc, _ := submit(`{"payload":{"description":"c"}}`, "c")
	idd, code := submit(`{"payload":{"description":"d"},"depends_on":[`+itoa(ida)+`,`+itoa(idb)+`,`+itoa(idc)+`]}`, "d")
	if code != 202 {
		t.Fatal("fan-in rejected", code)
	}
	if _, p := state(idd); p != 3 {
		t.Fatalf("pending_deps=%d want 3", p)
	}
	if _, code := submit(`{"payload":{"description":"x"},"depends_on":[999999]}`, "bad"); code != 400 {
		t.Fatal("unknown dependency accepted", code)
	}

	claimed := claimIDs()
	if _, ok := claimed[idd]; ok {
		t.Fatal("fan-in claimed while dependencies pending")
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d want 3", len(claimed))
	}

	// One success, one retryable failure (must NOT release), then terminal.
	if err := a.finish(ctx, claimed[ida], []byte(`{"ok":1}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := a.finish(ctx, claimed[idb], nil, errors.New("flaky")); err != nil {
		t.Fatal(err)
	}
	if _, p := state(idd); p != 2 {
		t.Fatalf("retry released a dependent: pending=%d want 2", p)
	}
	if err := a.finish(ctx, claimed[idc], nil, Terminal(errors.New("gone"))); err != nil {
		t.Fatal(err)
	}
	if _, p := state(idd); p != 1 {
		t.Fatalf("pending=%d want 1", p)
	}
	if _, err := a.claim(ctx); err == nil {
		t.Fatal("claimed something while b is backing off and d is pending")
	}
	// b comes back, exhausts its attempts through the lease-expiry sweep path.
	db.Exec(ctx, "UPDATE jobs SET state='running',attempts=max_attempts,lease_until=now()-interval '1 second' WHERE id=$1", idb)
	j, err := a.claim(ctx)
	if err != nil {
		t.Fatal("fan-in not released after sweep:", err)
	}
	if j.ID != idd {
		t.Fatalf("claimed %d want fan-in %d", j.ID, idd)
	}
	if st, _ := state(idb); st != "failed" {
		t.Fatal("sweep did not fail b:", st)
	}
	deps, err := a.dependencies(ctx, idd)
	if err != nil || len(deps) != 3 {
		t.Fatal("dependencies()", err, len(deps))
	}
	outcomes := map[int64]string{}
	for _, d := range deps {
		outcomes[d.ID] = d.State
	}
	if outcomes[ida] != "succeeded" || outcomes[idb] != "failed" || outcomes[idc] != "failed" {
		t.Fatalf("fan-in sees wrong outcomes: %v", outcomes)
	}

	// Depending on an already-finished job is not pending at all.
	ide, _ := submit(`{"payload":{"description":"e"},"depends_on":[`+itoa(ida)+`]}`, "e")
	if _, p := state(ide); p != 0 {
		t.Fatalf("already-terminal dependency counted: pending=%d", p)
	}

	// Idempotent resubmission of a job with dependencies is a no-op.
	if id2, code := submit(`{"payload":{"description":"d"},"depends_on":[`+itoa(ida)+`,`+itoa(idb)+`,`+itoa(idc)+`]}`, "d"); code != 202 || id2 != idd {
		t.Fatal("resubmit changed identity", code, id2)
	}
	var edges int
	db.QueryRow(ctx, "SELECT count(*) FROM job_dependencies WHERE job_id=$1", idd).Scan(&edges)
	if edges != 3 {
		t.Fatal("resubmit duplicated edges:", edges)
	}
	if _, p := state(idd); p != 0 {
		t.Fatalf("resubmit reset pending_deps to %d", p)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

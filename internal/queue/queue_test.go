package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dispatch/internal/testdb"
)

// testDB connects to TEST_DATABASE_URL and resets the tables. Point it only
// at a disposable database.
func testDB(t *testing.T) (*Queue, context.Context) {
	t.Helper()
	ctx := context.Background()
	db := testdb.Open(t, "queue")
	q := New(db)
	if err := q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "TRUNCATE events,job_dependencies,jobs RESTART IDENTITY"); err != nil {
		t.Fatal(err)
	}
	return q, ctx
}

// stub is a handler whose behaviour is chosen per test.
type stub struct {
	run func(ctx context.Context, j Job) ([]byte, error)
}

func (stub) Validate(json.RawMessage) error                   { return nil }
func (s stub) Run(ctx context.Context, j Job) ([]byte, error) { return s.run(ctx, j) }

func enqueue(t *testing.T, q *Queue, ctx context.Context, key string, deps ...int64) int64 {
	t.Helper()
	id, _, err := q.Enqueue(ctx, EnqueueRequest{Type: "stub", Payload: []byte(`{"k":"` + key + `"}`), Key: key, DependsOn: deps})
	if err != nil {
		t.Fatal(key, err)
	}
	return id
}

func state(t *testing.T, q *Queue, ctx context.Context, id int64) (st string, attempts, pending int) {
	t.Helper()
	if err := q.db.QueryRow(ctx, "SELECT state,attempts,pending_deps FROM jobs WHERE id=$1", id).Scan(&st, &attempts, &pending); err != nil {
		t.Fatal(err)
	}
	return
}

func TestClaimFenceRetry(t *testing.T) {
	q, ctx := testDB(t)

	// Idempotency: same key + same payload is one job; different payload is a conflict.
	id1, created, err := q.Enqueue(ctx, EnqueueRequest{Type: "stub", Payload: []byte(`{"a":1}`), Key: "same"})
	if err != nil || !created {
		t.Fatal(err, created)
	}
	id2, created, err := q.Enqueue(ctx, EnqueueRequest{Type: "stub", Payload: []byte(`{"a":1}`), Key: "same"})
	if err != nil || created || id1 != id2 {
		t.Fatal("duplicate not collapsed", err, created, id1, id2)
	}
	if _, _, err = q.Enqueue(ctx, EnqueueRequest{Type: "stub", Payload: []byte(`{"a":2}`), Key: "same"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatal("payload conflict not rejected:", err)
	}
	if _, _, err = q.Enqueue(ctx, EnqueueRequest{Type: "other", Payload: []byte(`{"a":1}`), Key: "same"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatal("type conflict not rejected:", err)
	}

	// Exactly one of many concurrent claimers wins.
	var wg sync.WaitGroup
	claimed := make(chan Job, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if j, e := q.Claim(ctx); e == nil {
				claimed <- j
			}
		}()
	}
	wg.Wait()
	close(claimed)
	var first Job
	n := 0
	for j := range claimed {
		n++
		first = j
	}
	if n != 1 || first.Attempts != 1 {
		t.Fatalf("claim not exclusive: %d winners", n)
	}

	// Expired lease is reclaimable; the stale holder's completion is fenced out.
	q.db.Exec(ctx, "UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=$1", first.ID)
	second, err := q.Claim(ctx)
	if err != nil || second.Attempts != 2 {
		t.Fatal("expired job not recovered", err)
	}
	if q.Finish(ctx, first, json.RawMessage(`{}`), nil) == nil {
		t.Fatal("stale completion accepted")
	}

	// Retryable failure goes back to queued with backoff, then fails at the limit.
	if err = q.Finish(ctx, second, nil, errors.New("flaky")); err != nil {
		t.Fatal(err)
	}
	if st, _, _ := state(t, q, ctx, first.ID); st != "queued" {
		t.Fatal(st)
	}
	q.db.Exec(ctx, "UPDATE jobs SET available_at=now() WHERE id=$1", first.ID)
	third, err := q.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Finish(ctx, third, nil, errors.New("flaky again")); err != nil {
		t.Fatal(err)
	}
	if st, _, _ := state(t, q, ctx, first.ID); st != "failed" {
		t.Fatal("retry limit not enforced:", st)
	}

	// Success path through RunOne with a registered handler.
	reg := NewRegistry()
	reg.Register("stub", stub{run: func(context.Context, Job) ([]byte, error) { return []byte(`{"ok":true}`), nil }})
	ok := enqueue(t, q, ctx, "ok")
	j, err := q.Claim(ctx)
	if err != nil || j.ID != ok {
		t.Fatal(err)
	}
	q.RunOne(ctx, reg, j)
	var result []byte
	var st string
	q.db.QueryRow(ctx, "SELECT state,result FROM jobs WHERE id=$1", ok).Scan(&st, &result)
	if st != "succeeded" || string(result) != `{"ok": true}` {
		t.Fatalf("result not stored: %s %s", st, result)
	}

	// Terminal error fails on the first attempt and is never reclaimable.
	reg2 := NewRegistry()
	reg2.Register("stub", stub{run: func(context.Context, Job) ([]byte, error) { return nil, Terminal(errors.New("404 gone")) }})
	term := enqueue(t, q, ctx, "term")
	j, _ = q.Claim(ctx)
	q.RunOne(ctx, reg2, j)
	var event string
	q.db.QueryRow(ctx, "SELECT event FROM events WHERE job_id=$1 ORDER BY id DESC LIMIT 1", term).Scan(&event)
	if st, attempts, _ := state(t, q, ctx, term); st != "failed" || attempts != 1 || event != "failed_terminal" {
		t.Fatalf("terminal error retried: %s %d %s", st, attempts, event)
	}
	if _, err = q.Claim(ctx); err == nil {
		t.Fatal("terminally failed job was claimable")
	}

	// A type with no handler in this binary is terminal too.
	orphan := enqueue(t, q, ctx, "orphan")
	q.db.Exec(ctx, "UPDATE jobs SET type='vanished' WHERE id=$1", orphan)
	j, _ = q.Claim(ctx)
	q.RunOne(ctx, NewRegistry(), j)
	var msg string
	q.db.QueryRow(ctx, "SELECT error FROM jobs WHERE id=$1", orphan).Scan(&msg)
	if st, attempts, _ := state(t, q, ctx, orphan); st != "failed" || attempts != 1 || !strings.Contains(msg, "no handler") {
		t.Fatalf("missing handler not terminal: %s %d %q", st, attempts, msg)
	}
}

func TestDependencies(t *testing.T) {
	q, ctx := testDB(t)
	claimAll := func() map[int64]Job {
		got := map[int64]Job{}
		for {
			j, err := q.Claim(ctx)
			if err != nil {
				return got
			}
			got[j.ID] = j
		}
	}

	// Fan-out: three independent children, one fan-in that waits for all.
	a, b, c := enqueue(t, q, ctx, "a"), enqueue(t, q, ctx, "b"), enqueue(t, q, ctx, "c")
	d := enqueue(t, q, ctx, "d", a, b, c)
	if _, _, p := state(t, q, ctx, d); p != 3 {
		t.Fatalf("pending_deps=%d want 3", p)
	}
	if _, _, err := q.Enqueue(ctx, EnqueueRequest{Type: "stub", Payload: []byte(`{}`), Key: "bad", DependsOn: []int64{999999}}); !errors.Is(err, ErrUnknownDependency) {
		t.Fatal("unknown dependency accepted:", err)
	}

	claimed := claimAll()
	if _, ok := claimed[d]; ok || len(claimed) != 3 {
		t.Fatalf("fan-in claimed while pending, or wrong count %d", len(claimed))
	}

	// Success releases; a retryable failure must NOT; a terminal failure does.
	if err := q.Finish(ctx, claimed[a], []byte(`{"ok":1}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := q.Finish(ctx, claimed[b], nil, errors.New("flaky")); err != nil {
		t.Fatal(err)
	}
	if _, _, p := state(t, q, ctx, d); p != 2 {
		t.Fatalf("retry released a dependent: pending=%d want 2", p)
	}
	if err := q.Finish(ctx, claimed[c], nil, Terminal(errors.New("gone"))); err != nil {
		t.Fatal(err)
	}
	if _, _, p := state(t, q, ctx, d); p != 1 {
		t.Fatalf("pending=%d want 1", p)
	}
	if _, err := q.Claim(ctx); err == nil {
		t.Fatal("claimed something while b is backing off and d is pending")
	}

	// b exhausts its attempts through the expired-lease sweep, which must also release.
	q.db.Exec(ctx, "UPDATE jobs SET state='running',attempts=max_attempts,lease_until=now()-interval '1 second' WHERE id=$1", b)
	j, err := q.Claim(ctx)
	if err != nil || j.ID != d {
		t.Fatal("fan-in not released after sweep:", err, j.ID)
	}
	if st, _, _ := state(t, q, ctx, b); st != "failed" {
		t.Fatal("sweep did not fail b:", st)
	}
	deps, err := q.Dependencies(ctx, d)
	if err != nil || len(deps) != 3 {
		t.Fatal("Dependencies()", err, len(deps))
	}
	outcomes := map[int64]string{}
	for _, dep := range deps {
		outcomes[dep.ID] = dep.State
	}
	if outcomes[a] != "succeeded" || outcomes[b] != "failed" || outcomes[c] != "failed" {
		t.Fatalf("fan-in sees wrong outcomes: %v", outcomes)
	}

	// Depending on an already-finished job is satisfied immediately.
	e := enqueue(t, q, ctx, "e", a)
	if _, _, p := state(t, q, ctx, e); p != 0 {
		t.Fatalf("already-terminal dependency counted: pending=%d", p)
	}

	// Idempotent resubmission with dependencies neither duplicates edges nor resets the count.
	if d2 := enqueue(t, q, ctx, "d", a, b, c); d2 != d {
		t.Fatal("resubmit changed identity", d2)
	}
	var edges int
	q.db.QueryRow(ctx, "SELECT count(*) FROM job_dependencies WHERE job_id=$1", d).Scan(&edges)
	if _, _, p := state(t, q, ctx, d); edges != 3 || p != 0 {
		t.Fatalf("resubmit corrupted deps: edges=%d pending=%d", edges, p)
	}
}

func TestSchedules(t *testing.T) {
	q, ctx := testDB(t)
	q.db.Exec(ctx, "DELETE FROM schedules")
	if err := q.EnsureSchedule(ctx, Schedule{Name: "tick", Type: "stub", Interval: time.Minute}); err != nil {
		t.Fatal(err)
	}
	// Due immediately on creation.
	n, err := q.RunDue(ctx)
	if err != nil || n != 1 {
		t.Fatal("first run", n, err)
	}
	// Not due again yet, and idempotent across repeated ticks.
	if n, _ := q.RunDue(ctx); n != 0 {
		t.Fatal("fired twice in one interval")
	}
	ss, _ := q.Schedules(ctx)
	if len(ss) != 1 || ss[0].LastJobID == nil || !ss[0].NextAt.After(time.Now()) {
		t.Fatalf("schedule not advanced: %+v", ss)
	}
	// After a long outage, only one job is created (no catch-up storm) and next_at lands in the future.
	q.db.Exec(ctx, "UPDATE schedules SET next_at=now()-interval '10 minutes'")
	if n, _ := q.RunDue(ctx); n != 1 {
		t.Fatal("missed slots should fire once, got", n)
	}
	var jobs int
	q.db.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE idempotency_key LIKE 'schedule:tick:%'").Scan(&jobs)
	if jobs != 2 {
		t.Fatal("jobs:", jobs)
	}
	ss, _ = q.Schedules(ctx)
	if !ss[0].NextAt.After(time.Now()) {
		t.Fatal("next_at still in the past after catch-up")
	}
	// EnsureSchedule on restart keeps next_at (no reset), and a shorter interval pulls it in.
	before := ss[0].NextAt
	q.EnsureSchedule(ctx, Schedule{Name: "tick", Type: "stub", Interval: time.Minute})
	ss, _ = q.Schedules(ctx)
	if !ss[0].NextAt.Equal(before) {
		t.Fatal("restart reset next_at")
	}
	q.db.Exec(ctx, "UPDATE schedules SET next_at=now()+interval '1 hour'")
	q.EnsureSchedule(ctx, Schedule{Name: "tick", Type: "stub", Interval: 2 * time.Minute})
	ss, _ = q.Schedules(ctx)
	if ss[0].NextAt.After(time.Now().Add(3 * time.Minute)) {
		t.Fatal("shorter interval did not pull next_at in")
	}
	// Two concurrent tickers: exactly one enqueues per due slot.
	q.db.Exec(ctx, "UPDATE schedules SET next_at=now()")
	var wg sync.WaitGroup
	total := make(chan int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, _ := q.RunDue(ctx)
			total <- n
		}()
	}
	wg.Wait()
	close(total)
	sum := 0
	for n := range total {
		sum += n
	}
	if sum != 1 {
		t.Fatal("concurrent tickers enqueued", sum)
	}
}

// runWorker starts a worker in the background and returns a stop function.
func runWorker(t *testing.T, q *Queue, reg *Registry, opts WorkerOptions) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); q.Worker(ctx, reg, opts) }()
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		<-done
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < d {
		if cond() {
			return time.Since(start)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", d, what)
	return 0
}

func TestWorkerIdleBackoff(t *testing.T) {
	q, ctx := testDB(t)
	q.db.Exec(ctx, "DELETE FROM schedules")

	var runs atomic.Int64
	reg := NewRegistry()
	reg.Register("stub", stub{run: func(context.Context, Job) ([]byte, error) {
		runs.Add(1)
		return []byte(`{}`), nil
	}})
	opts := WorkerOptions{IdleMin: 10 * time.Millisecond, IdleMax: 200 * time.Millisecond}

	// An idle worker must not spin. With a 200ms ceiling, one second of doing
	// nothing is on the order of ten wake-ups, not thousands.
	stop := runWorker(t, q, reg, opts)
	time.Sleep(time.Second)
	stop()
	if n := claimAttempts(t, q, ctx); n > 40 {
		t.Errorf("idle worker claimed %d times in 1s; backoff is not working", n)
	}

	// A job enqueued while the worker sits at full backoff is still picked up.
	stop = runWorker(t, q, reg, opts)
	defer stop()
	time.Sleep(600 * time.Millisecond) // let it reach the ceiling
	enqueue(t, q, ctx, "late")
	waitFor(t, 3*time.Second, "a job enqueued during backoff to run", func() bool { return runs.Load() == 1 })

	// Backoff resets after work, so a burst drains at full speed instead of
	// each job waiting out a doubled pause.
	for i := 0; i < 10; i++ {
		enqueue(t, q, ctx, fmt.Sprintf("burst-%d", i))
	}
	el := waitFor(t, 5*time.Second, "a 10-job burst to drain", func() bool { return runs.Load() == 11 })
	// Ten jobs at the 200ms ceiling would take ~2s; resetting makes it near-instant.
	if el > time.Second {
		t.Errorf("draining 10 ready jobs took %v; backoff is not resetting after work", el)
	}
}

// claimAttempts counts how many times any job has been claimed, a proxy for
// how often the worker has gone to the database looking for work.
func claimAttempts(t *testing.T, q *Queue, ctx context.Context) int64 {
	t.Helper()
	var n int64
	q.db.QueryRow(ctx, `SELECT COALESCE(sum(attempts),0) FROM jobs`).Scan(&n)
	return n
}

func TestWorkerWakesForSchedule(t *testing.T) {
	q, ctx := testDB(t)
	q.db.Exec(ctx, "DELETE FROM schedules")
	var runs atomic.Int64
	reg := NewRegistry()
	reg.Register("stub", stub{run: func(context.Context, Job) ([]byte, error) {
		runs.Add(1)
		return []byte(`{}`), nil
	}})

	if err := q.EnsureSchedule(ctx, Schedule{Name: "soon", Type: "stub", Interval: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(ctx, `UPDATE schedules SET next_at = now() + interval '400 milliseconds'`); err != nil {
		t.Fatal(err)
	}

	// Both pauses are far longer than the 400ms until the schedule is due, so
	// this only passes if the worker shortens its sleep to meet it.
	stop := runWorker(t, q, reg, WorkerOptions{IdleMin: 5 * time.Second, IdleMax: time.Hour, Schedules: true})
	defer stop()
	waitFor(t, 4*time.Second, "the schedule to fire and run", func() bool { return runs.Load() == 1 })

	var fired int
	q.db.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key LIKE 'schedule:soon:%'`).Scan(&fired)
	if fired != 1 {
		t.Fatalf("schedule fired %d times, want 1", fired)
	}
}

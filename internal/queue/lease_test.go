package queue

import (
	"context"
	"testing"
	"time"
)

// TestLeaseRenewedWhileHandlerRuns covers the case that broke in production:
// a handler that legitimately takes longer than one lease. The feed poll
// downloads megabytes and reconciles thousands of rows, and ran past the
// 45-second lease on the very first deploy; the job was reclaimed underneath
// it and its completion was correctly, uselessly, fenced out. Renewal is what
// makes a long handler a supported case rather than an accident.
func TestLeaseRenewedWhileHandlerRuns(t *testing.T) {
	q, ctx := testDB(t)
	prev := leaseDuration
	leaseDuration = 150 * time.Millisecond
	defer func() { leaseDuration = prev }()

	id := enqueue(t, q, ctx, "slow")
	reg := NewRegistry()
	reg.Register("stub", stub{run: func(ctx context.Context, j Job) ([]byte, error) {
		// Four leases' worth of work. Without renewal the lease lapses after
		// the first and the completion is rejected.
		select {
		case <-time.After(4 * leaseDuration):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []byte(`{"ok":true}`), nil
	}})

	j, err := q.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	q.RunOne(ctx, reg, j)

	got, err := q.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "succeeded" {
		t.Fatalf("state=%q error=%q; want succeeded", got.State, got.Error)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts=%d; want 1, the job should never have been reclaimed", got.Attempts)
	}
}

// TestLeaseLostCancelsHandler is the other half: renewal must not paper over a
// lease that is genuinely gone. Once another worker holds the job, the first
// worker's remaining work is void, so it is cancelled rather than left running
// to produce a result nobody will accept.
func TestLeaseLostCancelsHandler(t *testing.T) {
	q, ctx := testDB(t)
	prev := leaseDuration
	leaseDuration = 150 * time.Millisecond
	defer func() { leaseDuration = prev }()

	id := enqueue(t, q, ctx, "stolen")
	started := make(chan struct{})
	var handlerErr error
	reg := NewRegistry()
	reg.Register("stub", stub{run: func(ctx context.Context, j Job) ([]byte, error) {
		close(started)
		select {
		case <-time.After(10 * time.Second):
			return []byte(`{"ok":true}`), nil
		case <-ctx.Done():
			handlerErr = ctx.Err()
			return nil, ctx.Err()
		}
	}})

	j, err := q.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); q.RunOne(ctx, reg, j) }()
	<-started

	// Another worker takes the job: attempts moves past this worker's fencing
	// token, so its next renewal cannot match the row.
	if _, err := q.db.Exec(ctx, `UPDATE jobs SET attempts=attempts+1,lease_until=now()+interval '1 hour' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not cancelled after the lease was lost")
	}
	if handlerErr == nil {
		t.Error("handler ran to completion; its context should have been cancelled")
	}

	// And it recorded nothing: the job still belongs to the other worker.
	var events int
	q.db.QueryRow(ctx, `SELECT count(*) FROM events WHERE job_id=$1`, id).Scan(&events)
	if events != 0 {
		t.Errorf("%d events recorded by a worker that had lost the lease", events)
	}
	got, err := q.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "running" {
		t.Errorf("state=%q; want running, the stale worker must not have touched it", got.State)
	}
}

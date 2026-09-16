package queue

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestChaosNoLostOrDuplicatedWork exercises the reliability claim rather than
// asserting it. Many workers race for a pool of jobs while a third of them
// are killed mid-job: a killed worker stops without reporting, so its lease
// lapses and someone else takes the job. The invariants are that every job
// ends succeeded exactly once, that no job records two completions, and that
// the stale-completion fence actually fired at least once, since a run where
// the race never happened would prove nothing.
//
// A worker is "killed" by abandoning its in-flight job without calling
// Finish, which is what SIGKILL, an OOM or a severed connection look like to
// the database. The lease is shortened so reclaims happen in milliseconds.
func TestChaosNoLostOrDuplicatedWork(t *testing.T) {
	q, ctx := testDB(t)
	prev := leaseDuration
	leaseDuration = 50 * time.Millisecond
	defer func() { leaseDuration = prev }()

	const jobs = 120
	const workers = 8
	for i := 0; i < jobs; i++ {
		if _, _, err := q.Enqueue(ctx, EnqueueRequest{Type: "chaos", Payload: []byte(`{}`), Key: fmt.Sprintf("chaos-%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Enough attempts that a job killed several times can still finish.
	if _, err := q.db.Exec(ctx, `UPDATE jobs SET max_attempts=40`); err != nil {
		t.Fatal(err)
	}

	var completions, kills, fenced int64
	reg := NewRegistry()
	// One job in six runs longer than the lease. Those are the interesting
	// ones: the lease lapses while the worker is still working, another worker
	// takes the job, and the first one's completion must be rejected. Without
	// a handler slower than the lease the fence is never exercised at all.
	reg.Register("chaos", stub{run: func(ctx context.Context, j Job) ([]byte, error) {
		d := time.Duration(rand.Intn(8)) * time.Millisecond
		if rand.Intn(6) == 0 {
			d = leaseDuration + time.Duration(rand.Intn(40))*time.Millisecond
		}
		time.Sleep(d)
		return []byte(`{"ok":true}`), nil
	}})

	deadline := time.Now().Add(20 * time.Second)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(seed)))
			for time.Now().Before(deadline) {
				j, err := q.Claim(ctx)
				if err != nil {
					// Nothing claimable: either everything is terminal, or the
					// outstanding leases have yet to lapse.
					var pending int
					q.db.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state NOT IN ('succeeded','failed')`).Scan(&pending)
					if pending == 0 {
						return
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}
				// Die mid-job about a third of the time: never report anything.
				if r.Intn(100) < 35 {
					atomic.AddInt64(&kills, 1)
					time.Sleep(time.Duration(r.Intn(4)) * time.Millisecond)
					continue
				}
				h, _ := reg.Get(j.Type)
				result, runErr := h.Run(ctx, j)
				if err := q.Finish(ctx, j, result, runErr); err != nil {
					// This worker's lease lapsed while it worked and the job was
					// reassigned: the fence rejected its stale completion.
					atomic.AddInt64(&fenced, 1)
					continue
				}
				atomic.AddInt64(&completions, 1)
			}
		}(w)
	}
	wg.Wait()

	var pending, succeeded, failed int
	q.db.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE state IN ('queued','running')),
		count(*) FILTER (WHERE state='succeeded'),
		count(*) FILTER (WHERE state='failed') FROM jobs`).Scan(&pending, &succeeded, &failed)

	// 1. Nothing was lost.
	if succeeded != jobs {
		t.Errorf("succeeded=%d want %d (pending=%d failed=%d)", succeeded, jobs, pending, failed)
	}
	// 2. Nothing completed twice.
	var dupes int
	q.db.QueryRow(ctx, `SELECT count(*) FROM (SELECT job_id FROM events WHERE event='succeeded' GROUP BY job_id HAVING count(*)>1) d`).Scan(&dupes)
	if dupes != 0 {
		t.Errorf("%d jobs recorded more than one completion", dupes)
	}
	// 3. Every job holds exactly one result.
	var missing int
	q.db.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE result IS NULL`).Scan(&missing)
	if missing != 0 {
		t.Errorf("%d jobs succeeded with no stored result", missing)
	}
	// 4. The scenario actually exercised the paths it claims to.
	if kills < 20 {
		t.Errorf("only %d kills; the test did not stress reclaim", kills)
	}
	if fenced == 0 {
		t.Error("no stale completion was fenced; the race never happened, so this run proves nothing")
	}
	t.Logf("jobs=%d workers=%d kills=%d fenced_stale_completions=%d completions=%d", jobs, workers, kills, fenced, completions)
}

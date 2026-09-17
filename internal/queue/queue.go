package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `CREATE TABLE IF NOT EXISTS jobs (
 id BIGSERIAL PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL, payload JSONB NOT NULL,
 state TEXT NOT NULL DEFAULT 'queued' CHECK(state IN ('queued','running','succeeded','failed')),
 attempts INT NOT NULL DEFAULT 0, max_attempts INT NOT NULL DEFAULT 3,
 available_at TIMESTAMPTZ NOT NULL DEFAULT now(), lease_until TIMESTAMPTZ,
 result JSONB, error TEXT NOT NULL DEFAULT '', created_at TIMESTAMPTZ NOT NULL DEFAULT now());
 ALTER TABLE jobs ADD COLUMN IF NOT EXISTS type TEXT NOT NULL DEFAULT 'analyze';
 ALTER TABLE jobs ADD COLUMN IF NOT EXISTS pending_deps INT NOT NULL DEFAULT 0;
 CREATE INDEX IF NOT EXISTS jobs_claim ON jobs(state,available_at);
 CREATE TABLE IF NOT EXISTS events (id BIGSERIAL PRIMARY KEY,job_id BIGINT REFERENCES jobs(id),attempt INT,event TEXT,at TIMESTAMPTZ DEFAULT now());
 CREATE TABLE IF NOT EXISTS job_dependencies (job_id BIGINT NOT NULL REFERENCES jobs(id), depends_on BIGINT NOT NULL REFERENCES jobs(id), PRIMARY KEY(job_id,depends_on));
 CREATE INDEX IF NOT EXISTS job_dependencies_depends_on ON job_dependencies(depends_on);` + scheduleSchema

// releaseDependents decrements pending_deps on every job that was waiting on
// one of the given jobs. It is called whenever a job reaches a terminal state
// by any path. Dependents are released on failure as well as success: a
// fan-in that waits forever because one child failed is worse than one that
// runs and reports the failure. Handlers that need all-success semantics can
// inspect their dependencies with Queue.Dependencies.
const releaseDependents = `UPDATE jobs SET pending_deps=pending_deps-1 WHERE id IN (SELECT job_id FROM job_dependencies WHERE depends_on = ANY($1::bigint[]))`

// leaseDuration is how long a claim is held before another worker may take
// the job. It is a var rather than a const only so the chaos test can shorten
// it; nothing changes it at runtime.
var leaseDuration = 45 * time.Second

type Job struct {
	ID          int64           `json:"id"`
	Type        string          `json:"type"`
	State       string          `json:"state"`
	Attempts    int             `json:"attempts"`
	PendingDeps int             `json:"pending_deps"`
	Payload     json.RawMessage `json:"payload"`
	Result      json.RawMessage `json:"result"`
	Error       string          `json:"error"`
}

// Queue is the job engine over one Postgres pool.
type Queue struct{ db *pgxpool.Pool }

func New(db *pgxpool.Pool) *Queue { return &Queue{db: db} }

// Migrate applies the schema. An advisory lock serialises it across API and
// worker processes starting at the same time.
func (q *Queue) Migrate(ctx context.Context) error {
	tx, err := q.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(314159)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Exec runs one statement outside the engine, for other packages that own
// tables in the same database (their migrations run under the same lock).
func (q *Queue) MigrateExtra(ctx context.Context, sql string) error {
	tx, err := q.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(314159)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, sql); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DB exposes the pool for packages that store their own tables alongside jobs.
func (q *Queue) DB() *pgxpool.Pool { return q.db }

// Ping reports database reachability for health checks.
func (q *Queue) Ping(ctx context.Context) error { return q.db.Ping(ctx) }

// TerminalError wraps a failure that retrying cannot fix. finish() moves the
// job straight to failed instead of scheduling another attempt.
type TerminalError struct{ Err error }

func (e *TerminalError) Error() string { return e.Err.Error() }
func (e *TerminalError) Unwrap() error { return e.Err }

// Terminal marks err as not retryable.
func Terminal(err error) error { return &TerminalError{Err: err} }

func IsTerminal(err error) bool {
	var t *TerminalError
	return errors.As(err, &t)
}

const JobColumns = `jobs.id,jobs.type,jobs.state,jobs.attempts,jobs.pending_deps,jobs.payload,jobs.result,jobs.error`

func ScanJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Type, &j.State, &j.Attempts, &j.PendingDeps, &j.Payload, &j.Result, &j.Error)
	return j, err
}

// enqueueRequest is one new job. DependsOn lists ids of existing jobs that
// must reach a terminal state before this one becomes claimable. Because a
// job can only depend on jobs that already exist, the dependency graph is
// acyclic by construction.
type EnqueueRequest struct {
	Type      string
	Payload   []byte // canonical JSON
	Key       string // idempotency key; derived from type+payload if empty
	DependsOn []int64
}

var ErrUnknownDependency = errors.New("depends_on references a job that does not exist")

// enqueue inserts a job and its dependency edges in one transaction. The
// dependency rows are locked FOR SHARE while pending_deps is computed, so a
// dependency cannot finish between the count and the insert: finish()'s
// UPDATE on that row waits for this transaction to commit, and its
// releaseDependents then sees the new edge.
func (q *Queue) Enqueue(ctx context.Context, req EnqueueRequest) (id int64, created bool, err error) {
	tx, err := q.db.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback(ctx)
	pending := 0
	if len(req.DependsOn) > 0 {
		rows, err := tx.Query(ctx, `SELECT id,state FROM jobs WHERE id = ANY($1::bigint[]) FOR SHARE`, req.DependsOn)
		if err != nil {
			return 0, false, err
		}
		seen := map[int64]bool{}
		for rows.Next() {
			var depID int64
			var state string
			if err := rows.Scan(&depID, &state); err != nil {
				rows.Close()
				return 0, false, err
			}
			seen[depID] = true
			if state != "succeeded" && state != "failed" {
				pending++
			}
		}
		rows.Close()
		for _, d := range req.DependsOn {
			if !seen[d] {
				return 0, false, ErrUnknownDependency
			}
		}
	}
	var storedType string
	var stored []byte
	var xmaxZero bool
	// xmax = 0 distinguishes a fresh insert from the ON CONFLICT no-op update.
	err = tx.QueryRow(ctx, `INSERT INTO jobs(idempotency_key,type,payload,pending_deps) VALUES($1,$2,$3,$4) ON CONFLICT(idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key RETURNING id,type,payload,(xmax=0)`, req.Key, req.Type, req.Payload, pending).Scan(&id, &storedType, &stored, &xmaxZero)
	if err != nil {
		return 0, false, err
	}
	var prev any
	json.Unmarshal(stored, &prev)
	prevBytes, _ := json.Marshal(prev)
	if storedType != req.Type || string(prevBytes) != string(req.Payload) {
		return id, false, ErrIdempotencyConflict
	}
	if xmaxZero && len(req.DependsOn) > 0 {
		for _, d := range req.DependsOn {
			if _, err := tx.Exec(ctx, `INSERT INTO job_dependencies(job_id,depends_on) VALUES($1,$2) ON CONFLICT DO NOTHING`, id, d); err != nil {
				return 0, false, err
			}
		}
	}
	return id, xmaxZero, tx.Commit(ctx)
}

var ErrIdempotencyConflict = errors.New("idempotency key belongs to different input")

// dependencies returns the jobs the given job waited on, so a fan-in handler
// can see which of its inputs succeeded.
func (q *Queue) Dependencies(ctx context.Context, jobID int64) ([]Job, error) {
	rows, err := q.db.Query(ctx, `SELECT `+JobColumns+` FROM jobs JOIN job_dependencies d ON d.depends_on=jobs.id WHERE d.job_id=$1 ORDER BY jobs.id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deps []Job
	for rows.Next() {
		j, err := ScanJob(rows)
		if err != nil {
			return nil, err
		}
		deps = append(deps, j)
	}
	return deps, rows.Err()
}

// claim atomically takes one runnable job. SKIP LOCKED lets independent
// workers contend without blocking each other; attempts is the fencing token
// that prevents a worker whose lease expired from completing a job that has
// since been reclaimed by someone else.
func (q *Queue) Claim(ctx context.Context) (Job, error) {
	_, err := q.db.Exec(ctx, `WITH expired AS (UPDATE jobs SET state='failed',error='lease expired; retry limit reached' WHERE state='running' AND lease_until<now() AND attempts>=max_attempts RETURNING id) `+
		`UPDATE jobs SET pending_deps=pending_deps-1 WHERE id IN (SELECT job_id FROM job_dependencies WHERE depends_on IN (SELECT id FROM expired))`)
	if err != nil {
		return Job{}, err
	}
	return ScanJob(q.db.QueryRow(ctx, `WITH candidate AS (SELECT id FROM jobs WHERE pending_deps=0 AND attempts<max_attempts AND ((state='queued' AND available_at<=now()) OR (state='running' AND lease_until<now())) ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE jobs SET state='running',attempts=attempts+1,lease_until=now()+$1::interval FROM candidate WHERE jobs.id=candidate.id RETURNING `+JobColumns, leaseDuration))
}

// finish records the outcome of an attempt. A nil jobErr succeeds; a
// TerminalError fails immediately; anything else retries with exponential
// backoff until max_attempts. The UPDATE is fenced on attempts and a live
// lease, so a stale worker's completion is rejected rather than applied.
func (q *Queue) Finish(ctx context.Context, j Job, result []byte, jobErr error) error {
	state, event, failure := "succeeded", "succeeded", ""
	delay := 1 << min(j.Attempts, 6)
	if jobErr != nil {
		failure = jobErr.Error()
		switch {
		case IsTerminal(jobErr):
			state, event = "failed", "failed_terminal"
		case j.Attempts >= 3:
			state, event = "failed", "failed"
		default:
			state, event = "queued", "retry"
		}
	}
	tx, err := q.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE jobs SET state=$1,result=$2,error=$3,lease_until=NULL,available_at=now()+$4*interval '1 second' WHERE id=$5 AND attempts=$6 AND state='running' AND lease_until>now()`, state, result, failure, delay, j.ID, j.Attempts)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("lease lost; completion rejected")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO events(job_id,attempt,event) VALUES($1,$2,$3)`, j.ID, j.Attempts, event); err != nil {
		return err
	}
	if state == "succeeded" || state == "failed" {
		if _, err = tx.Exec(ctx, releaseDependents, []int64{j.ID}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// runOne executes a claimed job with its registered handler and records the
// outcome. A job whose type has no handler in this binary is a terminal
// failure: retrying would only reach the same binary.
func (q *Queue) RunOne(ctx context.Context, reg *Registry, j Job) {
	slog.Info("job started", "job_id", j.ID, "type", j.Type, "attempt", j.Attempts)
	var result []byte
	var err error
	if h, ok := reg.Get(j.Type); ok {
		result, err = h.Run(ctx, j)
	} else {
		err = Terminal(fmt.Errorf("no handler registered for type %q", j.Type))
	}
	if ctx.Err() != nil {
		// Shutting down mid-job: leave the lease to expire so another worker
		// picks it up, rather than recording a spurious failure.
		return
	}
	if ferr := q.Finish(ctx, j, result, err); ferr != nil {
		slog.Error("completion", "job_id", j.ID, "error", ferr)
		return
	}
	if err != nil {
		slog.Warn("job failed", "job_id", j.ID, "attempt", j.Attempts, "terminal", IsTerminal(err), "error", err)
	} else {
		slog.Info("job completed", "job_id", j.ID, "attempt", j.Attempts)
	}
}

// WorkerOptions tunes the idle behaviour of a worker loop.
//
// The defaults are deliberately not "poll as fast as possible". A worker that
// queries every 250ms around the clock keeps a serverless Postgres awake
// permanently, which on a metered plan costs far more than the work is worth.
// Instead the loop polls tightly while there is work, then backs off
// exponentially to IdleMax, and never sleeps past the next scheduled job.
// Between sweeps it issues no queries at all, so pooled connections age out
// and the database is free to suspend.
//
// The cost is latency on ad-hoc work: a job enqueued while the worker is at
// full backoff waits up to IdleMax to start. That is the trade, and it is why
// IdleMax is configurable rather than baked in.
type WorkerOptions struct {
	IdleMin time.Duration // first pause after finding nothing; default 250ms
	IdleMax time.Duration // longest pause; default 30s
	// Schedules, when true, also runs due schedules on each pass. Every worker
	// may set this: RunDue takes an advisory lock, so only one fires each tick.
	Schedules bool
}

func (o WorkerOptions) withDefaults() WorkerOptions {
	if o.IdleMin <= 0 {
		o.IdleMin = 250 * time.Millisecond
	}
	if o.IdleMax < o.IdleMin {
		o.IdleMax = max(30*time.Second, o.IdleMin)
	}
	return o
}

// Worker claims and runs jobs until ctx ends, backing off when idle.
func (q *Queue) Worker(ctx context.Context, reg *Registry, opts WorkerOptions) {
	opts = opts.withDefaults()
	backoff := opts.IdleMin
	slog.Info("worker loop", "idle_min", opts.IdleMin, "idle_max", opts.IdleMax, "schedules", opts.Schedules)
	for ctx.Err() == nil {
		if opts.Schedules {
			if n, err := q.RunDue(ctx); err != nil {
				if ctx.Err() == nil {
					slog.Error("scheduler", "error", err)
				}
			} else if n > 0 {
				backoff = opts.IdleMin // something was just enqueued; go look for it
			}
		}
		j, err := q.Claim(ctx)
		if err == nil {
			q.RunOne(ctx, reg, j)
			backoff = opts.IdleMin
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) && ctx.Err() == nil {
			slog.Error("claim", "error", err)
		}
		q.idle(ctx, opts, &backoff)
	}
}

// idle sleeps for the current backoff, shortened so the loop wakes in time for
// the next scheduled job, then doubles the backoff up to IdleMax.
func (q *Queue) idle(ctx context.Context, opts WorkerOptions, backoff *time.Duration) {
	wait := *backoff
	if opts.Schedules {
		// Shorten the sleep to meet the next schedule. The floor is a small
		// constant, not IdleMin: clamping back up to IdleMin would let a
		// worker with a long IdleMin sleep straight through a schedule, which
		// is the bug this line exists to prevent. The floor only stops a hot
		// loop when a schedule is overdue but another worker keeps winning
		// the tick.
		if until, ok := q.untilNextSchedule(ctx); ok && until < wait {
			wait = max(until, 100*time.Millisecond)
		}
	}
	select {
	case <-ctx.Done():
	case <-time.After(wait):
	}
	if *backoff < opts.IdleMax {
		*backoff = min(*backoff*2, opts.IdleMax)
	}
}

// untilNextSchedule reports how long until the soonest enabled schedule is due.
// A schedule already overdue returns 0. ok is false when there are none, or the
// query failed, in which case the caller just uses its backoff.
func (q *Queue) untilNextSchedule(ctx context.Context) (time.Duration, bool) {
	var secs *float64
	err := q.db.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM (min(next_at) - now())) FROM schedules WHERE enabled`).Scan(&secs)
	if err != nil || secs == nil {
		return 0, false
	}
	if *secs <= 0 {
		return 0, true
	}
	return time.Duration(*secs * float64(time.Second)), true
}

// List returns the most recent jobs, newest first.
func (q *Queue) List(ctx context.Context, limit int) ([]Job, error) {
	rows, err := q.db.Query(ctx, `SELECT `+JobColumns+` FROM jobs ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []Job{}
	for rows.Next() {
		j, err := ScanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Get returns one job or pgx.ErrNoRows.
func (q *Queue) Get(ctx context.Context, id int64) (Job, error) {
	return ScanJob(q.db.QueryRow(ctx, `SELECT `+JobColumns+` FROM jobs WHERE id=$1`, id))
}

// Count is one row of Stats.
type Count struct {
	Type, State string
	N           int
}

// Stats returns job counts by type and state plus the total retry count.
// It is derived from durable state so it survives restarts.
func (q *Queue) Stats(ctx context.Context) (counts []Count, retries int, err error) {
	rows, err := q.db.Query(ctx, `SELECT type,state,count(*) FROM jobs GROUP BY type,state ORDER BY type,state`)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var c Count
		if err := rows.Scan(&c.Type, &c.State, &c.N); err != nil {
			return nil, 0, err
		}
		counts = append(counts, c)
	}
	err = q.db.QueryRow(ctx, `SELECT count(*) FROM events WHERE event='retry'`).Scan(&retries)
	return counts, retries, err
}

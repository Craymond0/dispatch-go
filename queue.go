package main

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
 CREATE INDEX IF NOT EXISTS jobs_claim ON jobs(state,available_at);
 CREATE TABLE IF NOT EXISTS events (id BIGSERIAL PRIMARY KEY,job_id BIGINT REFERENCES jobs(id),attempt INT,event TEXT,at TIMESTAMPTZ DEFAULT now());`

const leaseDuration = 45 * time.Second

type Job struct {
	ID       int64           `json:"id"`
	Type     string          `json:"type"`
	State    string          `json:"state"`
	Attempts int             `json:"attempts"`
	Payload  json.RawMessage `json:"payload"`
	Result   json.RawMessage `json:"result"`
	Error    string          `json:"error"`
}

type App struct{ db *pgxpool.Pool }

// TerminalError wraps a failure that retrying cannot fix. finish() moves the
// job straight to failed instead of scheduling another attempt.
type TerminalError struct{ Err error }

func (e *TerminalError) Error() string { return e.Err.Error() }
func (e *TerminalError) Unwrap() error { return e.Err }

// Terminal marks err as not retryable.
func Terminal(err error) error { return &TerminalError{Err: err} }

func isTerminal(err error) bool {
	var t *TerminalError
	return errors.As(err, &t)
}

const jobColumns = `jobs.id,jobs.type,jobs.state,jobs.attempts,jobs.payload,jobs.result,jobs.error`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Type, &j.State, &j.Attempts, &j.Payload, &j.Result, &j.Error)
	return j, err
}

// claim atomically takes one runnable job. SKIP LOCKED lets independent
// workers contend without blocking each other; attempts is the fencing token
// that prevents a worker whose lease expired from completing a job that has
// since been reclaimed by someone else.
func (a App) claim(ctx context.Context) (Job, error) {
	_, err := a.db.Exec(ctx, `UPDATE jobs SET state='failed',error='lease expired; retry limit reached' WHERE state='running' AND lease_until<now() AND attempts>=max_attempts`)
	if err != nil {
		return Job{}, err
	}
	return scanJob(a.db.QueryRow(ctx, `WITH candidate AS (SELECT id FROM jobs WHERE attempts<max_attempts AND ((state='queued' AND available_at<=now()) OR (state='running' AND lease_until<now())) ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE jobs SET state='running',attempts=attempts+1,lease_until=now()+$1::interval FROM candidate WHERE jobs.id=candidate.id RETURNING `+jobColumns, leaseDuration))
}

// finish records the outcome of an attempt. A nil jobErr succeeds; a
// TerminalError fails immediately; anything else retries with exponential
// backoff until max_attempts. The UPDATE is fenced on attempts and a live
// lease, so a stale worker's completion is rejected rather than applied.
func (a App) finish(ctx context.Context, j Job, result []byte, jobErr error) error {
	state, event, failure := "succeeded", "succeeded", ""
	delay := 1 << min(j.Attempts, 6)
	if jobErr != nil {
		failure = jobErr.Error()
		switch {
		case isTerminal(jobErr):
			state, event = "failed", "failed_terminal"
		case j.Attempts >= 3:
			state, event = "failed", "failed"
		default:
			state, event = "queued", "retry"
		}
	}
	tx, err := a.db.Begin(ctx)
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
	return tx.Commit(ctx)
}

// runOne executes a claimed job with its registered handler and records the
// outcome. A job whose type has no handler in this binary is a terminal
// failure: retrying would only reach the same binary.
func (a App) runOne(ctx context.Context, reg *Registry, j Job) {
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
	if ferr := a.finish(ctx, j, result, err); ferr != nil {
		slog.Error("completion", "job_id", j.ID, "error", ferr)
		return
	}
	if err != nil {
		slog.Warn("job failed", "job_id", j.ID, "attempt", j.Attempts, "terminal", isTerminal(err), "error", err)
	} else {
		slog.Info("job completed", "job_id", j.ID, "attempt", j.Attempts)
	}
}

func (a App) worker(ctx context.Context, reg *Registry) {
	for ctx.Err() == nil {
		j, err := a.claim(ctx)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) && ctx.Err() == nil {
				slog.Error("claim", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		a.runOne(ctx, reg, j)
	}
}

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Schedules are recurring jobs. Every worker runs the scheduler loop, but on
// each tick exactly one of them wins a transaction-scoped advisory lock and
// enqueues whatever is due. There is no long-lived leader to fail over: if
// the winner dies mid-tick its transaction rolls back and the next tick has
// a new winner. Enqueued jobs use the key schedule:<name>:<due time>, so
// even a lock bug could not produce duplicates.
const scheduleSchema = `
CREATE TABLE IF NOT EXISTS schedules (
  name TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}',
  interval_seconds INT NOT NULL CHECK (interval_seconds >= 60),
  next_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_job_id BIGINT,
  enabled BOOLEAN NOT NULL DEFAULT true);`

const schedulerLock = 271828

// Schedule describes one recurring job.
type Schedule struct {
	Name      string        `json:"name"`
	Type      string        `json:"type"`
	Payload   []byte        `json:"payload"`
	Interval  time.Duration `json:"interval"`
	NextAt    time.Time     `json:"next_at"`
	LastJobID *int64        `json:"last_job_id"`
	Enabled   bool          `json:"enabled"`
}

// EnsureSchedule creates a schedule if it does not exist and updates its
// type, payload and interval if it does. next_at is preserved so a restart
// does not reset the cadence; a shortened interval that makes the existing
// next_at too far out is pulled in.
func (q *Queue) EnsureSchedule(ctx context.Context, s Schedule) error {
	if len(s.Payload) == 0 {
		s.Payload = []byte(`{}`)
	}
	_, err := q.db.Exec(ctx, `INSERT INTO schedules(name,type,payload,interval_seconds,next_at) VALUES($1,$2,$3,$4,now())
		ON CONFLICT(name) DO UPDATE SET type=EXCLUDED.type,payload=EXCLUDED.payload,interval_seconds=EXCLUDED.interval_seconds,
		next_at=LEAST(schedules.next_at, now()+EXCLUDED.interval_seconds*interval '1 second')`,
		s.Name, s.Type, s.Payload, int(s.Interval.Seconds()))
	return err
}

// Schedules lists every schedule.
func (q *Queue) Schedules(ctx context.Context) ([]Schedule, error) {
	rows, err := q.db.Query(ctx, `SELECT name,type,payload,interval_seconds,next_at,last_job_id,enabled FROM schedules ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Schedule
	for rows.Next() {
		var s Schedule
		var secs int
		if err := rows.Scan(&s.Name, &s.Type, &s.Payload, &secs, &s.NextAt, &s.LastJobID, &s.Enabled); err != nil {
			return nil, err
		}
		s.Interval = time.Duration(secs) * time.Second
		out = append(out, s)
	}
	return out, rows.Err()
}

// RunDue enqueues every enabled schedule whose next_at has passed and
// advances it. It returns the number enqueued, or 0 with no error when
// another worker holds the tick lock. Safe to call from many workers.
func (q *Queue) RunDue(ctx context.Context) (int, error) {
	tx, err := q.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var won bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, schedulerLock).Scan(&won); err != nil || !won {
		return 0, err
	}
	rows, err := tx.Query(ctx, `SELECT name,type,payload,interval_seconds,next_at FROM schedules WHERE enabled AND next_at<=now() FOR UPDATE`)
	if err != nil {
		return 0, err
	}
	type due struct {
		s    Schedule
		secs int
	}
	var dues []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.s.Name, &d.s.Type, &d.s.Payload, &d.secs, &d.s.NextAt); err != nil {
			rows.Close()
			return 0, err
		}
		dues = append(dues, d)
	}
	rows.Close()
	n := 0
	for _, d := range dues {
		key := fmt.Sprintf("schedule:%s:%d", d.s.Name, d.s.NextAt.Unix())
		var id int64
		// Insert directly in this transaction so the schedule advance and the job are atomic.
		if err := tx.QueryRow(ctx, `INSERT INTO jobs(idempotency_key,type,payload) VALUES($1,$2,$3) ON CONFLICT(idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key RETURNING id`, key, d.s.Type, d.s.Payload).Scan(&id); err != nil {
			return 0, err
		}
		// Advance to the next slot strictly in the future so a long outage does not
		// replay every missed interval.
		if _, err := tx.Exec(ctx, `UPDATE schedules SET last_job_id=$2, next_at = next_at + ((floor(extract(epoch from (now()-next_at))/interval_seconds)::int + 1) * interval_seconds) * interval '1 second' WHERE name=$1`, d.s.Name, id); err != nil {
			return 0, err
		}
		slog.Info("schedule fired", "schedule", d.s.Name, "job_id", id, "due", d.s.NextAt)
		n++
	}
	return n, tx.Commit(ctx)
}

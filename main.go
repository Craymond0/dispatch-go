package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
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
 CREATE INDEX IF NOT EXISTS jobs_claim ON jobs(state,available_at);
 CREATE TABLE IF NOT EXISTS events (id BIGSERIAL PRIMARY KEY,job_id BIGINT REFERENCES jobs(id),attempt INT,event TEXT,at TIMESTAMPTZ DEFAULT now());`

type Payload struct {
	Company     string `json:"company"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Delay       int    `json:"demo_delay_seconds,omitempty"`
	FailUntil   int    `json:"demo_fail_attempts,omitempty"`
}
type Job struct {
	ID       int64           `json:"id"`
	State    string          `json:"state"`
	Attempts int             `json:"attempts"`
	Payload  json.RawMessage `json:"payload"`
	Result   json.RawMessage `json:"result"`
	Error    string          `json:"error"`
}
type App struct{ db *pgxpool.Pool }

func env(k, v string) string {
	if x := os.Getenv(k); x != "" {
		return x
	}
	return v
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

var skills = []string{"Go", "Python", "C++", "C", "Java", "JavaScript", "TypeScript", "SQL", "PostgreSQL", "Docker", "Kubernetes", "React", "Linux", "AWS", "PX4", "ArduPilot", "MATLAB", "UART", "CAN", "I2C", "SPI"}

// skillIndex maps a lower-cased term to its position in skills so output
// order is stable regardless of where the term appears in the text.
var skillIndex = func() map[string]int {
	m := make(map[string]int, len(skills))
	for i, s := range skills {
		m[strings.ToLower(s)] = i
	}
	return m
}()

// isTermByte reports whether b can be part of a term. This is the complement
// of the boundary class [^a-z0-9_+] used by the original regex, evaluated
// case-insensitively, so non-ASCII bytes are boundaries as before.
func isTermByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '+'
}

// matchTerms scans the text once and returns the skills that appear as
// whole tokens, where a token is a maximal run of term bytes. Every term in
// skills is made only of term bytes, so this is equivalent to matching
// (?i)(^|[^a-z0-9_+])TERM($|[^a-z0-9_+]) for each term, but in one pass.
func matchTerms(text string) []string {
	seen := make([]bool, len(skills))
	n := 0
	for i := 0; i < len(text); {
		if !isTermByte(text[i]) {
			i++
			continue
		}
		j := i
		for j < len(text) && isTermByte(text[j]) {
			j++
		}
		if idx, ok := skillIndex[strings.ToLower(text[i:j])]; ok && !seen[idx] {
			seen[idx] = true
			n++
		}
		i = j
	}
	found := make([]string, 0, n)
	for i, s := range skills {
		if seen[i] {
			found = append(found, s)
		}
	}
	return found
}

func analyze(p Payload) map[string]any {
	found := matchTerms(p.Description)
	return map[string]any{"company": p.Company, "title": p.Title, "technical_terms": found, "word_count": len(strings.Fields(p.Description)), "method": "literal dictionary matching; not a qualification or fit score"}
}
func (a App) create(w http.ResponseWriter, r *http.Request) {
	var p Payload
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 100000))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil || strings.TrimSpace(p.Description) == "" || p.Delay < 0 || p.Delay > 30 || p.FailUntil < 0 || p.FailUntil > 3 {
		write(w, 400, map[string]string{"error": "provide description; demo delay 0–30, fail attempts 0–3"})
		return
	}
	if (p.Delay > 0 || p.FailUntil > 0) && os.Getenv("DEMO_MODE") != "1" {
		write(w, 400, map[string]string{"error": "demo controls disabled"})
		return
	}
	b, _ := json.Marshal(p)
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		h := sha256.Sum256(b)
		key = hex.EncodeToString(h[:])
	}
	if len(key) > 200 {
		http.Error(w, "key too long", 400)
		return
	}
	var id int64
	var stored []byte
	err := a.db.QueryRow(r.Context(), `INSERT INTO jobs(idempotency_key,payload) VALUES($1,$2) ON CONFLICT(idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key RETURNING id,payload`, key, b).Scan(&id, &stored)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	var previous Payload
	json.Unmarshal(stored, &previous)
	if previous != p {
		write(w, 409, map[string]string{"error": "idempotency key belongs to different input"})
		return
	}
	write(w, 202, map[string]any{"id": id, "url": fmt.Sprintf("/jobs/%d", id)})
}
func (a App) list(w http.ResponseWriter, r *http.Request) {
	rows, err := a.db.Query(r.Context(), `SELECT id,state,attempts,payload,result,error FROM jobs ORDER BY id DESC LIMIT 100`)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	defer rows.Close()
	jobs := []Job{}
	for rows.Next() {
		var j Job
		if rows.Scan(&j.ID, &j.State, &j.Attempts, &j.Payload, &j.Result, &j.Error) != nil {
			http.Error(w, "read failed", 500)
			return
		}
		jobs = append(jobs, j)
	}
	write(w, 200, jobs)
}
func (a App) get(w http.ResponseWriter, r *http.Request) {
	var j Job
	err := a.db.QueryRow(r.Context(), `SELECT id,state,attempts,payload,result,error FROM jobs WHERE id=$1`, r.PathValue("id")).Scan(&j.ID, &j.State, &j.Attempts, &j.Payload, &j.Result, &j.Error)
	if err != nil {
		http.Error(w, "job not found", 404)
		return
	}
	write(w, 200, j)
}
func (a App) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	rows, err := a.db.Query(r.Context(), `SELECT state,count(*) FROM jobs GROUP BY state`)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	defer rows.Close()
	fmt.Fprintln(w, "# HELP dispatch_jobs Current jobs by state.\n# TYPE dispatch_jobs gauge")
	for rows.Next() {
		var state string
		var n int
		rows.Scan(&state, &n)
		fmt.Fprintf(w, "dispatch_jobs{state=%q} %d\n", state, n)
	}
	var retries int
	a.db.QueryRow(r.Context(), `SELECT count(*) FROM events WHERE event='retry'`).Scan(&retries)
	fmt.Fprintf(w, "# TYPE dispatch_retries_total counter\ndispatch_retries_total %d\n", retries)
}

// Claim is atomic across processes. SKIP LOCKED permits independent workers;
// attempts is the fencing token that prevents an expired worker from completing a reclaimed job.
func (a App) claim(ctx context.Context) (Job, error) {
	_, err := a.db.Exec(ctx, `UPDATE jobs SET state='failed',error='lease expired; retry limit reached' WHERE state='running' AND lease_until<now() AND attempts>=max_attempts`)
	if err != nil {
		return Job{}, err
	}
	var j Job
	err = a.db.QueryRow(ctx, `WITH candidate AS (SELECT id FROM jobs WHERE attempts<max_attempts AND ((state='queued' AND available_at<=now()) OR (state='running' AND lease_until<now())) ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE jobs SET state='running',attempts=attempts+1,lease_until=now()+interval '45 seconds' FROM candidate WHERE jobs.id=candidate.id RETURNING jobs.id,jobs.state,jobs.attempts,jobs.payload,jobs.result,jobs.error`).Scan(&j.ID, &j.State, &j.Attempts, &j.Payload, &j.Result, &j.Error)
	return j, err
}
func (a App) finish(ctx context.Context, j Job, result []byte, failure string) error {
	state := "succeeded"
	event := "succeeded"
	delay := 1 << min(j.Attempts, 6)
	if failure != "" {
		state = "queued"
		event = "retry"
		if j.Attempts >= 3 {
			state = "failed"
			event = "failed"
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
func (a App) worker(ctx context.Context) {
	for ctx.Err() == nil {
		j, err := a.claim(ctx)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				slog.Error("claim", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}
		var p Payload
		json.Unmarshal(j.Payload, &p)
		slog.Info("job started", "job_id", j.ID, "attempt", j.Attempts)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(p.Delay) * time.Second):
		}
		var result []byte
		failure := ""
		if j.Attempts <= p.FailUntil {
			failure = "injected demo failure"
		} else {
			result, _ = json.Marshal(analyze(p))
		}
		if err := a.finish(ctx, j, result, failure); err != nil {
			slog.Error("completion", "job_id", j.ID, "error", err)
		} else {
			slog.Info("job completed", "job_id", j.ID, "attempt", j.Attempts, "error", failure)
		}
	}
}
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://dispatch:dispatch@localhost:5432/dispatch?sslmode=disable"))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// Serialize startup migrations across API and worker processes.
	tx, err := db.Begin(ctx)
	if err != nil {
		panic(err)
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(314159)`); err != nil {
		panic(err)
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		panic(err)
	}
	if err = tx.Commit(ctx); err != nil {
		panic(err)
	}
	a := App{db}
	if env("ROLE", "api") == "worker" {
		a.worker(ctx)
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", a.create)
	mux.HandleFunc("GET /jobs", a.list)
	mux.HandleFunc("GET /jobs/{id}", a.get)
	mux.HandleFunc("GET /metrics", a.metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if db.Ping(r.Context()) != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		write(w, 200, map[string]string{"status": "ok"})
	})
	token := os.Getenv("API_TOKEN")
	if token == "" && os.Getenv("DEMO_MODE") != "1" {
		panic("API_TOKEN required outside local demo")
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && token != "" && r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", 401)
			return
		}
		mux.ServeHTTP(w, r)
	})
	srv := &http.Server{Addr: ":" + env("PORT", "8080"), Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(c)
	}()
	slog.Info("api listening", "address", srv.Addr)
	if err = srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}

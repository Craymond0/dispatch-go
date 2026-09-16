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
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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

// submitRequest is the body of POST /jobs. type defaults to "analyze".
type submitRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type server struct {
	App
	reg *Registry
}

func (s server) create(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 100000))
	d.DisallowUnknownFields()
	if err := d.Decode(&req); err != nil {
		write(w, 400, map[string]string{"error": "body must be {\"type\": string, \"payload\": object}"})
		return
	}
	if req.Type == "" {
		req.Type = "analyze"
	}
	h, ok := s.reg.Get(req.Type)
	if !ok {
		write(w, 400, map[string]any{"error": "unknown job type", "known_types": s.reg.Names()})
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}
	if err := h.Validate(req.Payload); err != nil {
		write(w, 400, map[string]string{"error": err.Error()})
		return
	}
	// Canonicalise the payload so the same logical input always hashes the same
	// and the stored form is byte-comparable on idempotency conflicts.
	var canon any
	if err := json.Unmarshal(req.Payload, &canon); err != nil {
		write(w, 400, map[string]string{"error": "payload must be valid JSON"})
		return
	}
	b, _ := json.Marshal(canon)
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		h := sha256.Sum256(append([]byte(req.Type+"\x00"), b...))
		key = hex.EncodeToString(h[:])
	}
	if len(key) > 200 {
		http.Error(w, "key too long", 400)
		return
	}
	var id int64
	var storedType string
	var stored []byte
	err := s.db.QueryRow(r.Context(), `INSERT INTO jobs(idempotency_key,type,payload) VALUES($1,$2,$3) ON CONFLICT(idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key RETURNING id,type,payload`, key, req.Type, b).Scan(&id, &storedType, &stored)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	var prev any
	json.Unmarshal(stored, &prev)
	prevBytes, _ := json.Marshal(prev)
	if storedType != req.Type || string(prevBytes) != string(b) {
		write(w, 409, map[string]string{"error": "idempotency key belongs to different input"})
		return
	}
	write(w, 202, map[string]any{"id": id, "url": fmt.Sprintf("/jobs/%d", id)})
}

func (s server) list(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `SELECT `+jobColumns+` FROM jobs ORDER BY id DESC LIMIT 100`)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	defer rows.Close()
	jobs := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			http.Error(w, "read failed", 500)
			return
		}
		jobs = append(jobs, j)
	}
	write(w, 200, jobs)
}

func (s server) get(w http.ResponseWriter, r *http.Request) {
	j, err := scanJob(s.db.QueryRow(r.Context(), `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, r.PathValue("id")))
	if err != nil {
		http.Error(w, "job not found", 404)
		return
	}
	write(w, 200, j)
}

func (s server) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	rows, err := s.db.Query(r.Context(), `SELECT type,state,count(*) FROM jobs GROUP BY type,state ORDER BY type,state`)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	defer rows.Close()
	fmt.Fprintln(w, "# HELP dispatch_jobs Current jobs by type and state.\n# TYPE dispatch_jobs gauge")
	for rows.Next() {
		var typ, state string
		var n int
		rows.Scan(&typ, &state, &n)
		fmt.Fprintf(w, "dispatch_jobs{type=%q,state=%q} %d\n", typ, state, n)
	}
	var retries int
	s.db.QueryRow(r.Context(), `SELECT count(*) FROM events WHERE event='retry'`).Scan(&retries)
	fmt.Fprintf(w, "# TYPE dispatch_retries_total counter\ndispatch_retries_total %d\n", retries)
}

func newRegistry() *Registry {
	reg := NewRegistry()
	reg.Register("analyze", analyzeHandler{})
	return reg
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
	reg := newRegistry()
	a := App{db}
	if env("ROLE", "api") == "worker" {
		slog.Info("worker started", "handlers", reg.Names())
		a.worker(ctx, reg)
		return
	}
	s := server{App: a, reg: reg}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.create)
	mux.HandleFunc("GET /jobs", s.list)
	mux.HandleFunc("GET /jobs/{id}", s.get)
	mux.HandleFunc("GET /metrics", s.metrics)
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
	slog.Info("api listening", "address", srv.Addr, "handlers", reg.Names())
	if err = srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}

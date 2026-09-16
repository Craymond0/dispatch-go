// Command dispatch runs either the HTTP API (default) or a worker
// (ROLE=worker) against the same Postgres database.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"dispatch/internal/analyze"
	"dispatch/internal/api"
	"dispatch/internal/llm"
	"dispatch/internal/queue"
	"dispatch/internal/tracker"
	"dispatch/internal/web"
)

func env(k, v string) string {
	if x := os.Getenv(k); x != "" {
		return x
	}
	return v
}

// registry lists every job type this binary can run. Both roles build the
// same registry so the API can validate what the worker will execute.
func registry(tr *tracker.Tracker) *queue.Registry {
	reg := queue.NewRegistry()
	reg.Register("analyze", analyze.Handler{})
	tr.Register(reg)
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
	q := queue.New(db)
	if err := q.Migrate(ctx); err != nil {
		panic(err)
	}
	if err := q.MigrateExtra(ctx, tracker.Schema); err != nil {
		panic(err)
	}
	tr := tracker.Defaults(q)
	tr.FeedURL = env("FEED_URL", tr.FeedURL)
	tr.LLM = &llm.Client{APIKey: os.Getenv("ANTHROPIC_API_KEY"), Model: os.Getenv("ANTHROPIC_MODEL")}
	tr.Email = tracker.Email{APIKey: os.Getenv("RESEND_API_KEY"), From: os.Getenv("DIGEST_FROM"), To: os.Getenv("DIGEST_TO")}
	if d, err := time.ParseDuration(env("SWEEP_INTERVAL", "6h")); err == nil {
		tr.SweepInterval = d
	}
	if err := tr.Start(ctx); err != nil {
		panic(err)
	}
	reg := registry(tr)
	role := env("ROLE", "api")
	if len(os.Args) > 1 && os.Args[1] != "" {
		role = os.Args[1] // fly.io process groups set the command, not the env
	}
	if role == "worker" {
		slog.Info("worker started", "handlers", reg.Names())
		go q.Scheduler(ctx, 30*time.Second)
		q.Worker(ctx, reg)
		return
	}
	token := os.Getenv("API_TOKEN")
	if token == "" && os.Getenv("DEMO_MODE") != "1" {
		panic("API_TOKEN required outside local demo")
	}
	s := &api.Server{Q: q, Reg: reg, Token: token, Password: os.Getenv("DASHBOARD_PASSWORD"), SessionSecret: []byte(os.Getenv("SESSION_SECRET")), Mount: tr.Routes, Static: web.Handler()}
	srv := &http.Server{Addr: ":" + env("PORT", "8080"), Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
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

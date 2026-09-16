// Package api is the HTTP surface over the queue.
package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"dispatch/internal/queue"
)

// Server serves the job API. Token, when non-empty, is required as a bearer
// token on every route except /healthz.
type Server struct {
	Q     *queue.Queue
	Reg   *queue.Registry
	Token string
}

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// submitRequest is the body of POST /jobs. type defaults to "analyze".
// depends_on lists existing job ids this job must wait for.
type submitRequest struct {
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	DependsOn []int64         `json:"depends_on"`
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.create)
	mux.HandleFunc("GET /jobs", s.list)
	mux.HandleFunc("GET /jobs/{id}", s.get)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /healthz", s.healthz)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && s.Token != "" && r.Header.Get("Authorization") != "Bearer "+s.Token {
			http.Error(w, "unauthorized", 401)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 100000))
	d.DisallowUnknownFields()
	if err := d.Decode(&req); err != nil {
		write(w, 400, map[string]string{"error": "body must be {\"type\": string, \"payload\": object, \"depends_on\": [ids]}"})
		return
	}
	if req.Type == "" {
		req.Type = "analyze"
	}
	h, ok := s.Reg.Get(req.Type)
	if !ok {
		write(w, 400, map[string]any{"error": "unknown job type", "known_types": s.Reg.Names()})
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
		sum := sha256.Sum256(append([]byte(req.Type+"\x00"), b...))
		key = hex.EncodeToString(sum[:])
	}
	if len(key) > 200 {
		http.Error(w, "key too long", 400)
		return
	}
	id, _, err := s.Q.Enqueue(r.Context(), queue.EnqueueRequest{Type: req.Type, Payload: b, Key: key, DependsOn: req.DependsOn})
	switch {
	case errors.Is(err, queue.ErrIdempotencyConflict):
		write(w, 409, map[string]string{"error": err.Error()})
	case errors.Is(err, queue.ErrUnknownDependency):
		write(w, 400, map[string]string{"error": err.Error()})
	case err != nil:
		http.Error(w, "database unavailable", 503)
	default:
		write(w, 202, map[string]any{"id": id, "url": fmt.Sprintf("/jobs/%d", id)})
	}
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.Q.List(r.Context(), 100)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	write(w, 200, jobs)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	var id int64
	if _, err := fmt.Sscan(r.PathValue("id"), &id); err != nil {
		http.Error(w, "job not found", 404)
		return
	}
	j, err := s.Q.Get(r.Context(), id)
	if err != nil {
		http.Error(w, "job not found", 404)
		return
	}
	deps, err := s.Q.Dependencies(r.Context(), j.ID)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	dependsOn := make([]map[string]any, 0, len(deps))
	for _, d := range deps {
		dependsOn = append(dependsOn, map[string]any{"id": d.ID, "type": d.Type, "state": d.State})
	}
	write(w, 200, map[string]any{"id": j.ID, "type": j.Type, "state": j.State, "attempts": j.Attempts, "pending_deps": j.PendingDeps, "payload": j.Payload, "result": j.Result, "error": j.Error, "depends_on": dependsOn})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	counts, retries, err := s.Q.Stats(r.Context())
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# HELP dispatch_jobs Current jobs by type and state.\n# TYPE dispatch_jobs gauge")
	for _, c := range counts {
		fmt.Fprintf(w, "dispatch_jobs{type=%q,state=%q} %d\n", c.Type, c.State, c.N)
	}
	fmt.Fprintf(w, "# TYPE dispatch_retries_total counter\ndispatch_retries_total %d\n", retries)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	if s.Q.Ping(r.Context()) != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	write(w, 200, map[string]string{"status": "ok"})
}

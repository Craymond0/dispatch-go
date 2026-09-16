package queue

import (
	"context"
	"encoding/json"
	"sort"
)

// Handler does the work for one job type. Validate runs at submission so a
// bad payload is rejected with a 400 instead of failing later on a worker.
// Run executes the job; return Terminal(err) for failures that must not be
// retried (a malformed input, a 404) and any other error to retry with
// backoff.
type Handler interface {
	Validate(payload json.RawMessage) error
	Run(ctx context.Context, j Job) ([]byte, error)
}

// Registry maps job types to handlers. It is populated once at startup and
// read concurrently by workers, so it has no locking.
type Registry struct{ m map[string]Handler }

func NewRegistry() *Registry { return &Registry{m: map[string]Handler{}} }

func (r *Registry) Register(name string, h Handler) {
	if _, dup := r.m[name]; dup {
		panic("handler registered twice: " + name)
	}
	r.m[name] = h
}

func (r *Registry) Get(name string) (Handler, bool) { h, ok := r.m[name]; return h, ok }

func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.m))
	for n := range r.m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

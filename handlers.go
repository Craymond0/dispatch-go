package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// Handler does the work for one job type. Validate runs at submission so a
// bad payload is rejected with a 400 instead of failing later on a worker.
// Run executes the job; return a TerminalError for failures that must not be
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

// --- analyze: the original demo workload ------------------------------------

// Payload is the analyze job's input. The demo_* fields inject delay and
// failure so the retry and lease paths can be exercised by hand; they are
// refused unless DEMO_MODE=1.
type Payload struct {
	Company     string `json:"company"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Delay       int    `json:"demo_delay_seconds,omitempty"`
	FailUntil   int    `json:"demo_fail_attempts,omitempty"`
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

type analyzeHandler struct{}

func decodePayload(raw json.RawMessage) (Payload, error) {
	var p Payload
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, fmt.Errorf("invalid payload: %w", err)
	}
	return p, nil
}

func (analyzeHandler) Validate(raw json.RawMessage) error {
	p, err := decodePayload(raw)
	if err != nil {
		return err
	}
	if strings.TrimSpace(p.Description) == "" || p.Delay < 0 || p.Delay > 30 || p.FailUntil < 0 || p.FailUntil > 3 {
		return errors.New("provide description; demo delay 0–30, fail attempts 0–3")
	}
	if (p.Delay > 0 || p.FailUntil > 0) && os.Getenv("DEMO_MODE") != "1" {
		return errors.New("demo controls disabled")
	}
	return nil
}

func (analyzeHandler) Run(ctx context.Context, j Job) ([]byte, error) {
	p, err := decodePayload(j.Payload)
	if err != nil {
		return nil, Terminal(err) // validated at submit; a decode failure here means corrupt storage, not a transient fault
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Duration(p.Delay) * time.Second):
	}
	if j.Attempts <= p.FailUntil {
		return nil, errors.New("injected demo failure")
	}
	return json.Marshal(analyze(p))
}

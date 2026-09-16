// Package analyze is the original demo workload: find a fixed list of
// technology terms in a text. It exists to give the queue something to run
// and to exercise the retry and lease paths via demo controls.
package analyze

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"dispatch/internal/queue"
)

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

// Skills is the fixed term dictionary; exported so tests can reference it.
var Skills = []string{"Go", "Python", "C++", "C", "Java", "JavaScript", "TypeScript", "SQL", "PostgreSQL", "Docker", "Kubernetes", "React", "Linux", "AWS", "PX4", "ArduPilot", "MATLAB", "UART", "CAN", "I2C", "SPI"}

// skillIndex maps a lower-cased term to its position in Skills so output
// order is stable regardless of where the term appears in the text.
var skillIndex = func() map[string]int {
	m := make(map[string]int, len(Skills))
	for i, s := range Skills {
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

// matchTerms scans the text once and returns the Skills that appear as
// whole tokens, where a token is a maximal run of term bytes. Every term in
// Skills is made only of term bytes, so this is equivalent to matching
// (?i)(^|[^a-z0-9_+])TERM($|[^a-z0-9_+]) for each term, but in one pass.
func MatchTerms(text string) []string {
	seen := make([]bool, len(Skills))
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
	for i, s := range Skills {
		if seen[i] {
			found = append(found, s)
		}
	}
	return found
}

func Analyze(p Payload) map[string]any {
	found := MatchTerms(p.Description)
	return map[string]any{"company": p.Company, "title": p.Title, "technical_terms": found, "word_count": len(strings.Fields(p.Description)), "method": "literal dictionary matching; not a qualification or fit score"}
}

// Handler is the queue handler for the "analyze" job type.
type Handler struct{}

func decodePayload(raw json.RawMessage) (Payload, error) {
	var p Payload
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, fmt.Errorf("invalid payload: %w", err)
	}
	return p, nil
}

func (Handler) Validate(raw json.RawMessage) error {
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

func (Handler) Run(ctx context.Context, j queue.Job) ([]byte, error) {
	p, err := decodePayload(j.Payload)
	if err != nil {
		return nil, queue.Terminal(err) // validated at submit; a decode failure here means corrupt storage, not a transient fault
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Duration(p.Delay) * time.Second):
	}
	if j.Attempts <= p.FailUntil {
		return nil, errors.New("injected demo failure")
	}
	return json.Marshal(Analyze(p))
}

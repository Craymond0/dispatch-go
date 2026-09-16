package main

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"
)

func TestAnalyzeBoundaries(t *testing.T) {
	r := analyze(Payload{Description: "Django is useful. Go, PostgreSQL and C++ required; CAN bus."})
	skills := r["technical_terms"].([]string)
	found := map[string]bool{}
	for _, s := range skills {
		found[s] = true
	}
	if !found["Go"] || !found["C++"] || !found["PostgreSQL"] || found["C"] {
		t.Fatalf("wrong boundaries: %v", skills)
	}
	r = analyze(Payload{Description: "Django programmer"})
	if len(r["technical_terms"].([]string)) != 0 {
		t.Fatal("substring false match")
	}
}

func BenchmarkAnalyze(b *testing.B) {
	p := Payload{Description: strings.Repeat("We need Go, Python, PostgreSQL, Docker, Kubernetes and Linux experience; C++ and Java are a plus. ", 20)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		analyze(p)
	}
}

// referenceMatch is the original per-term regex, kept as the specification
// that matchTerms must agree with.
var referencePatterns = func() []*regexp.Regexp {
	ps := make([]*regexp.Regexp, len(skills))
	for i, s := range skills {
		ps[i] = regexp.MustCompile(`(?i)(^|[^a-z0-9_+])` + regexp.QuoteMeta(s) + `($|[^a-z0-9_+])`)
	}
	return ps
}()

func referenceMatch(text string) []string {
	found := []string{}
	for i, re := range referencePatterns {
		if re.MatchString(text) {
			found = append(found, skills[i])
		}
	}
	return found
}

func TestMatchTermsAgreesWithRegex(t *testing.T) {
	fixed := []string{
		"", "Go", "go", "GO!", "Django", "C", "C++", "C+++", "C++ and C", "PostgreSQL SQL", "MySQL",
		"React+Redux", "I2C/SPI/UART", "AWS_ARM", "Kubernetes.", "Linux\nDocker", "Java JavaScript",
		"café Go", "CAN bus", "PX4;ArduPilot", "_Python_", "Go,Go,Go", "TypeScript\tC", "x+Go",
	}
	for _, s := range fixed {
		if got, want := matchTerms(s), referenceMatch(s); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%q: got %v want %v", s, got, want)
		}
	}
	alphabet := []byte("goGOcC+psqlSQL _-.,;/\n\t0123é")
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		b := make([]byte, r.Intn(24))
		for k := range b {
			b[k] = alphabet[r.Intn(len(alphabet))]
		}
		s := string(b)
		if got, want := matchTerms(s), referenceMatch(s); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%q: got %v want %v", s, got, want)
		}
	}
}

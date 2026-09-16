package main

import (
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

package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// FakeAnthropic serves a canned streaming response and records the request.
func FakeAnthropic(t *testing.T, text string) (*httptest.Server, *map[string]any) {
	t.Helper()
	var last map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") == "" || r.Header.Get("anthropic-version") == "" {
			w.WriteHeader(401)
			return
		}
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &last)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: message_start\ndata: {\"message\":{\"usage\":{\"input_tokens\":123}}}\n\n")
		for _, chunk := range strings.SplitAfter(text, " ") {
			d, _ := json.Marshal(chunk)
			fmt.Fprintf(w, "event: content_block_delta\ndata: {\"delta\":{\"type\":\"text_delta\",\"text\":%s}}\n\n", d)
		}
		fmt.Fprint(w, "event: message_delta\ndata: {\"usage\":{\"output_tokens\":45}}\n\nevent: message_stop\ndata: {}\n\n")
	}))
	t.Cleanup(srv.Close)
	return srv, &last
}

func TestStream(t *testing.T) {
	srv, last := FakeAnthropic(t, "Strong match on Go. Gap: Kafka.")
	c := &Client{APIKey: "k", Endpoint: srv.URL}
	var got strings.Builder
	u, err := c.Stream(context.Background(), "SYS", "USER", 100, func(s string) error { got.WriteString(s); return nil })
	if err != nil || got.String() != "Strong match on Go. Gap: Kafka." || u.InputTokens != 123 || u.OutputTokens != 45 {
		t.Fatal(err, got.String(), u)
	}
	if (*last)["system"] != "SYS" || (*last)["stream"] != true || (*last)["model"] != "claude-sonnet-5" {
		t.Fatalf("request: %v", *last)
	}
	if _, err := (&Client{}).Stream(context.Background(), "", "", 1, nil); err != ErrNotConfigured {
		t.Fatal("unconfigured should error cleanly")
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(529); w.Write([]byte("overloaded")) }))
	defer bad.Close()
	if _, err := (&Client{APIKey: "k", Endpoint: bad.URL}).Stream(context.Background(), "", "", 1, nil); err == nil || !strings.Contains(err.Error(), "529") {
		t.Fatal("status not surfaced:", err)
	}
}

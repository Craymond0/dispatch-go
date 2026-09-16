// Package llm is a minimal streaming client for the Anthropic Messages API.
// It has no SDK dependency; the streaming protocol is a few SSE event types.
package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client calls the Messages API. Endpoint is overridable for tests.
type Client struct {
	APIKey   string
	Model    string
	Endpoint string
	HTTP     *http.Client
}

// Configured reports whether an API key is present.
func (c *Client) Configured() bool { return c != nil && c.APIKey != "" }

// Usage is the token accounting the API reports for one stream.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

var ErrNotConfigured = errors.New("ANTHROPIC_API_KEY not set")

// Stream sends one user message under a system prompt and calls onText with
// each text delta as it arrives. It returns the usage once the stream ends.
func (c *Client) Stream(ctx context.Context, system, user string, maxTokens int, onText func(string) error) (Usage, error) {
	if !c.Configured() {
		return Usage{}, ErrNotConfigured
	}
	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = "https://api.anthropic.com/v1/messages"
	}
	model := c.Model
	if model == "" {
		model = "claude-sonnet-5"
	}
	body, _ := json.Marshal(map[string]any{
		"model":      model,
		"max_tokens": maxTokens,
		"stream":     true,
		"system":     system,
		"messages":   []map[string]string{{"role": "user", "content": user}},
	})
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return Usage{}, err
	}
	req.Header.Set("x-api-key", c.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("accept", "text/event-stream")
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Minute}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return Usage{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return Usage{}, fmt.Errorf("anthropic: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var usage Usage
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var event string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			switch event {
			case "content_block_delta":
				var d struct {
					Delta struct {
						Type string `json:"type"`
						Text string `json:"text"`
					} `json:"delta"`
				}
				if json.Unmarshal([]byte(data), &d) == nil && d.Delta.Type == "text_delta" && d.Delta.Text != "" {
					if err := onText(d.Delta.Text); err != nil {
						return usage, err
					}
				}
			case "message_start":
				var m struct {
					Message struct {
						Usage Usage `json:"usage"`
					} `json:"message"`
				}
				if json.Unmarshal([]byte(data), &m) == nil {
					usage.InputTokens = m.Message.Usage.InputTokens
				}
			case "message_delta":
				var m struct {
					Usage Usage `json:"usage"`
				}
				if json.Unmarshal([]byte(data), &m) == nil {
					usage.OutputTokens = m.Usage.OutputTokens
				}
			case "error":
				var e struct {
					Error struct {
						Message string `json:"message"`
					} `json:"error"`
				}
				json.Unmarshal([]byte(data), &e)
				return usage, fmt.Errorf("anthropic: %s", e.Error.Message)
			}
		}
	}
	return usage, sc.Err()
}

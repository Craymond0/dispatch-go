package tracker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"strings"
	"time"

	"dispatch/internal/queue"
)

// Email configures digest delivery through Resend. Empty means email is off;
// the notify job then succeeds with a "skipped" result rather than failing,
// so a missing API key never poisons the sweep.
type Email struct {
	APIKey string
	From   string
	To     string
	// Endpoint is overridable for tests.
	Endpoint string
}

func (e Email) configured() bool { return e.APIKey != "" && e.From != "" && e.To != "" }

// notifyEmail sends one digest. Its idempotency key is derived from the
// digest event id (set by the digest job), and the same key is passed to
// Resend as Idempotency-Key, so a retry after a network failure cannot
// deliver the same digest twice.
type notifyEmail struct{ t *Tracker }

func (notifyEmail) Validate(raw json.RawMessage) error {
	_, err := idPayload(raw, "digest_event_id")
	return err
}

func (h notifyEmail) Run(ctx context.Context, j queue.Job) ([]byte, error) {
	t := h.t
	eventID, err := idPayload(j.Payload, "digest_event_id")
	if err != nil {
		return nil, queue.Terminal(err)
	}
	if !t.Email.configured() {
		return json.Marshal(map[string]any{"skipped": "email not configured"})
	}
	var raw []byte
	if err := t.db().QueryRow(ctx, `SELECT detail FROM tracker_events WHERE id=$1 AND kind='digest'`, eventID).Scan(&raw); err != nil {
		return nil, queue.Terminal(fmt.Errorf("digest event %d: %w", eventID, err))
	}
	var d Digest
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, queue.Terminal(err)
	}
	subject, text, htmlBody := renderDigest(d)
	body, _ := json.Marshal(map[string]any{"from": t.Email.From, "to": []string{t.Email.To}, "subject": subject, "text": text, "html": htmlBody})
	endpoint := t.Email.Endpoint
	if endpoint == "" {
		endpoint = "https://api.resend.com/emails"
	}
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, queue.Terminal(err)
	}
	req.Header.Set("Authorization", "Bearer "+t.Email.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", fmt.Sprintf("dispatch-digest-%d", eventID))
	resp, err := t.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		ID string `json:"id"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	switch {
	case resp.StatusCode == 429 || resp.StatusCode >= 500:
		return nil, fmt.Errorf("resend: HTTP %d", resp.StatusCode)
	case resp.StatusCode >= 400:
		return nil, queue.Terminal(fmt.Errorf("resend: HTTP %d", resp.StatusCode))
	}
	return json.Marshal(map[string]any{"sent": true, "message_id": out.ID, "to": t.Email.To})
}

// renderDigest produces the subject, plain text and HTML for one digest.
func renderDigest(d Digest) (subject, text, htmlBody string) {
	var parts []string
	if n := len(d.New); n > 0 {
		parts = append(parts, fmt.Sprintf("%d new", n))
	}
	if n := len(d.Closed); n > 0 {
		parts = append(parts, fmt.Sprintf("%d closed", n))
	}
	if n := len(d.FollowUps); n > 0 {
		parts = append(parts, fmt.Sprintf("%d to follow up", n))
	}
	if n := len(d.FailedSources); n > 0 {
		parts = append(parts, fmt.Sprintf("%d source errors", n))
	}
	if len(parts) == 0 {
		parts = []string{"no changes"}
	}
	subject = "Dispatch digest: " + strings.Join(parts, ", ")

	var tb, hb strings.Builder
	hb.WriteString("<div style=\"font-family:-apple-system,Segoe UI,sans-serif;font-size:14px;line-height:1.5\">")
	section := func(title string, ps []Posting) {
		if len(ps) == 0 {
			return
		}
		fmt.Fprintf(&tb, "%s (%d)\n", title, len(ps))
		fmt.Fprintf(&hb, "<h3 style=\"margin:16px 0 4px\">%s (%d)</h3><ul style=\"margin:0;padding-left:18px\">", html.EscapeString(title), len(ps))
		for _, p := range ps {
			loc := p.Location
			if loc != "" {
				loc = " — " + loc
			}
			fmt.Fprintf(&tb, "  %s: %s%s\n    %s\n", p.Company, p.Title, loc, p.URL)
			fmt.Fprintf(&hb, "<li><b>%s</b>: <a href=\"%s\">%s</a>%s</li>", html.EscapeString(p.Company), html.EscapeString(p.URL), html.EscapeString(p.Title), html.EscapeString(loc))
		}
		tb.WriteString("\n")
		hb.WriteString("</ul>")
	}
	section("New postings", d.New)
	section("Closed", d.Closed)
	section("Changed", d.Changed)
	if len(d.FollowUps) > 0 {
		fmt.Fprintf(&tb, "Follow up (%d)\n", len(d.FollowUps))
		fmt.Fprintf(&hb, "<h3 style=\"margin:16px 0 4px\">Follow up (%d)</h3><ul style=\"margin:0;padding-left:18px\">", len(d.FollowUps))
		for _, f := range d.FollowUps {
			fmt.Fprintf(&tb, "  %s: %s — %s, quiet %d days\n", f.Company, f.Title, f.Status, f.DaysQuiet)
			fmt.Fprintf(&hb, "<li><b>%s</b>: %s — %s, quiet %d days</li>", html.EscapeString(f.Company), html.EscapeString(f.Title), html.EscapeString(f.Status), f.DaysQuiet)
		}
		tb.WriteString("\n")
		hb.WriteString("</ul>")
	}
	if len(d.FailedSources) > 0 {
		tb.WriteString("Sources that failed this sweep\n")
		hb.WriteString("<h3 style=\"margin:16px 0 4px\">Sources that failed this sweep</h3><ul style=\"margin:0;padding-left:18px\">")
		for _, s := range d.FailedSources {
			fmt.Fprintf(&tb, "  %s\n", s)
			fmt.Fprintf(&hb, "<li><code>%s</code></li>", html.EscapeString(s))
		}
		hb.WriteString("</ul>")
	}
	if tb.Len() == 0 {
		tb.WriteString("Nothing changed since the last digest.\n")
		hb.WriteString("<p>Nothing changed since the last digest.</p>")
	}
	fmt.Fprintf(&tb, "\nSweep #%d at %s", d.SweepID, d.At.Format(time.RFC1123))
	fmt.Fprintf(&hb, "<p style=\"color:#888;margin-top:16px\">Sweep #%d at %s</p></div>", d.SweepID, d.At.Format(time.RFC1123))
	return subject, tb.String(), hb.String()
}

var errNoDigest = errors.New("no digest")

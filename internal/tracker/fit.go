package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"dispatch/internal/llm"
)

const fitSchema = `
ALTER TABLE postings ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS fit_reports (
  posting_id BIGINT PRIMARY KEY REFERENCES postings(id),
  report TEXT NOT NULL,
  model TEXT NOT NULL,
  input_tokens INT NOT NULL DEFAULT 0,
  output_tokens INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now());`

// fitSystem is the grounding contract. The model only gets the resume and
// the posting; the instructions forbid claims not supported by the resume.
const fitSystem = `You evaluate how well a candidate's resume matches one job posting.

Rules:
- The RESUME is the only source of truth about the candidate. Never attribute a skill, tool, project or experience to the candidate unless it appears in the resume text. If something is not in the resume, say it is not shown.
- For every claimed match, quote the supporting resume fragment in parentheses.
- Be direct and specific. No filler, no encouragement, no restating the posting.
- Use exactly these Markdown sections, in this order:
## Verdict
One sentence: strong / plausible / weak match, and why.
## Strong matches
Bullets. Each: the posting requirement, then the resume evidence in parentheses.
## Gaps
Bullets. What the posting asks for that the resume does not show. If the resume shows something adjacent, name it as adjacent, not as a match.
## Lead with
Two or three resume items to emphasise for this posting, and one sentence each on why.
## Likely interview probes
Three questions this posting's team would ask given the gaps.`

var (
	reScript = regexp.MustCompile(`(?is)<(script|style|noscript|svg|nav|footer|header)\b.*?</\s*(script|style|noscript|svg|nav|footer|header)\s*>`)
	reTag    = regexp.MustCompile(`(?s)<[^>]+>`)
	reSpace  = regexp.MustCompile(`[ \t\r\f\v]+`)
	reLines  = regexp.MustCompile(`\n{3,}`)
)

// stripHTML turns a job page into plain text good enough for a model to
// read. It is deliberately crude; the user can paste a description instead.
func stripHTML(s string) string {
	s = reScript.ReplaceAllString(s, " ")
	s = regexp.MustCompile(`(?i)<br\s*/?>|</p>|</div>|</li>|</h[1-6]>|</tr>`).ReplaceAllString(s, "\n")
	s = reTag.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	s = reSpace.ReplaceAllString(s, " ")
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimSpace(l)
	}
	s = strings.Join(lines, "\n")
	s = reLines.ReplaceAllString(s, "\n\n")
	return strings.TrimSpace(s)
}

const maxDescription = 16000

// description returns the posting's stored text, fetching and storing it
// from the URL on first use.
func (t *Tracker) description(ctx context.Context, pid int64) (string, error) {
	var desc, u string
	if err := t.db().QueryRow(ctx, `SELECT description,url FROM postings WHERE id=$1`, pid).Scan(&desc, &u); err != nil {
		return "", err
	}
	if desc != "" {
		return desc, nil
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; dispatch-tracker/1)")
	req.Header.Set("Accept", "text/html,*/*")
	resp, err := t.do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("posting page returned HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", err
	}
	desc = stripHTML(string(b))
	if len(desc) > maxDescription {
		desc = desc[:maxDescription]
	}
	if len(desc) < 200 {
		return "", fmt.Errorf("could not extract a description from %s (got %d characters; the page is probably rendered by JavaScript). Paste the description instead", u, len(desc))
	}
	_, err = t.db().Exec(ctx, `UPDATE postings SET description=$2 WHERE id=$1`, pid, desc)
	return desc, err
}

// FitReport is a stored analysis.
type FitReport struct {
	PostingID    int64     `json:"posting_id"`
	Report       string    `json:"report"`
	Model        string    `json:"model"`
	InputTokens  int       `json:"input_tokens"`
	OutputTokens int       `json:"output_tokens"`
	CreatedAt    time.Time `json:"created_at"`
}

func (t *Tracker) getFit(ctx context.Context, pid int64) (FitReport, bool) {
	var r FitReport
	err := t.db().QueryRow(ctx, `SELECT posting_id,report,model,input_tokens,output_tokens,created_at FROM fit_reports WHERE posting_id=$1`, pid).Scan(&r.PostingID, &r.Report, &r.Model, &r.InputTokens, &r.OutputTokens, &r.CreatedAt)
	return r, err == nil
}

// --- HTTP ---------------------------------------------------------------------

func (t *Tracker) fitRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /tracker/resume", t.getResume)
	mux.HandleFunc("PUT /tracker/resume", t.putResume)
	mux.HandleFunc("GET /tracker/postings/{id}/fit", t.fitGet)
	mux.HandleFunc("POST /tracker/postings/{id}/fit", t.fitStream)
	mux.HandleFunc("PUT /tracker/postings/{id}/description", t.putDescription)
}

func (t *Tracker) getResume(w http.ResponseWriter, r *http.Request) {
	text := t.getState(r.Context(), "resume.master")
	write(w, 200, map[string]any{"text": text, "chars": len(text), "updated_at": t.getState(r.Context(), "resume.updated_at")})
}

func (t *Tracker) putResume(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Text string `json:"text"`
	}
	if !decode(w, r, &req) {
		return
	}
	req.Text = strings.TrimSpace(req.Text)
	if len(req.Text) < 200 || len(req.Text) > 60000 {
		bad(w, "resume text must be between 200 and 60,000 characters")
		return
	}
	if err := t.setState(r.Context(), "resume.master", req.Text); err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	t.setState(r.Context(), "resume.updated_at", time.Now().UTC().Format(time.RFC3339))
	// Reports were grounded in the old resume.
	t.db().Exec(r.Context(), `DELETE FROM fit_reports`)
	write(w, 200, map[string]any{"chars": len(req.Text)})
}

func (t *Tracker) putDescription(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		http.Error(w, "not found", 404)
		return
	}
	var req struct {
		Text string `json:"text"`
	}
	if !decode(w, r, &req) {
		return
	}
	req.Text = strings.TrimSpace(req.Text)
	if len(req.Text) > maxDescription {
		req.Text = req.Text[:maxDescription]
	}
	tag, err := t.db().Exec(r.Context(), `UPDATE postings SET description=$2 WHERE id=$1`, id, req.Text)
	if err != nil || tag.RowsAffected() == 0 {
		http.Error(w, "not found", 404)
		return
	}
	t.db().Exec(r.Context(), `DELETE FROM fit_reports WHERE posting_id=$1`, id)
	write(w, 200, map[string]any{"chars": len(req.Text)})
}

func (t *Tracker) fitGet(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		http.Error(w, "not found", 404)
		return
	}
	if rep, ok := t.getFit(r.Context(), id); ok {
		write(w, 200, map[string]any{"report": rep})
		return
	}
	// 200 with a null report, not 404: "no report yet" is the normal state for
	// most postings and should not look like an error in the browser console.
	write(w, 200, map[string]any{"report": nil})
}

// fitStream streams a fit analysis as server-sent events:
//
//	data: {"text":"..."}      repeated, one per model delta
//	event: done  data: {...}  the stored report with usage
//	event: error data: {...}  once, then the stream ends
//
// A cached report is replayed as a single delta unless ?force=1.
func (t *Tracker) fitStream(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		http.Error(w, "not found", 404)
		return
	}
	ctx := r.Context()
	flusher, canFlush := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	send := func(event string, v any) {
		b, _ := json.Marshal(v)
		if event != "" {
			fmt.Fprintf(w, "event: %s\n", event)
		}
		fmt.Fprintf(w, "data: %s\n\n", b)
		if canFlush {
			flusher.Flush()
		}
	}
	fail := func(status int, msg string) {
		// Headers may already be sent; the status is advisory for clients that read it early.
		if status > 0 {
			w.WriteHeader(status)
		}
		send("error", map[string]any{"error": msg})
	}
	if r.URL.Query().Get("force") != "1" {
		if rep, ok := t.getFit(ctx, id); ok {
			w.WriteHeader(200)
			send("", map[string]any{"text": rep.Report})
			send("done", map[string]any{"report": rep, "cached": true})
			return
		}
	}
	if !t.LLM.Configured() {
		fail(503, "ANTHROPIC_API_KEY is not set on the server")
		return
	}
	resume := t.getState(ctx, "resume.master")
	if resume == "" {
		fail(400, "no resume stored; paste your master resume under Settings first")
		return
	}
	var p Posting
	var err error
	if p, err = scanPosting(t.db().QueryRow(ctx, `SELECT `+postingColumns+` FROM postings p JOIN companies c ON c.id=p.company_id WHERE p.id=$1`, id)); err != nil {
		fail(404, "posting not found")
		return
	}
	desc, err := t.description(ctx, id)
	if err != nil {
		fail(422, err.Error())
		return
	}
	w.WriteHeader(200)
	user := fmt.Sprintf("RESUME:\n<<<\n%s\n>>>\n\nPOSTING: %s at %s (%s)\nURL: %s\n<<<\n%s\n>>>", resume, p.Title, p.Company, p.Location, p.URL, desc)
	var full strings.Builder
	usage, err := t.LLM.Stream(ctx, fitSystem, user, 1500, func(s string) error {
		full.WriteString(s)
		send("", map[string]any{"text": s})
		return nil
	})
	if err != nil {
		send("error", map[string]any{"error": err.Error(), "partial": full.String()})
		return
	}
	model := t.LLM.Model
	if model == "" {
		model = "claude-sonnet-5"
	}
	rep := FitReport{PostingID: id, Report: full.String(), Model: model, InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, CreatedAt: time.Now().UTC()}
	if _, err := t.db().Exec(ctx, `INSERT INTO fit_reports(posting_id,report,model,input_tokens,output_tokens,created_at) VALUES($1,$2,$3,$4,$5,$6)
		ON CONFLICT(posting_id) DO UPDATE SET report=EXCLUDED.report,model=EXCLUDED.model,input_tokens=EXCLUDED.input_tokens,output_tokens=EXCLUDED.output_tokens,created_at=EXCLUDED.created_at`,
		rep.PostingID, rep.Report, rep.Model, rep.InputTokens, rep.OutputTokens, rep.CreatedAt); err != nil {
		send("error", map[string]any{"error": "report generated but could not be saved: " + err.Error(), "partial": full.String()})
		return
	}
	send("done", map[string]any{"report": rep, "cached": false})
}

var _ = llm.ErrNotConfigured

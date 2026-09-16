package tracker

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dispatch/internal/llm"
)

func TestStripHTML(t *testing.T) {
	in := `<html><head><style>.x{}</style><script>var a=1;</script></head><body><nav>Home</nav><h1>Backend  Engineer</h1><p>We need <b>Go</b> &amp; Postgres.</p><ul><li>Kafka</li><li>gRPC</li></ul><footer>© Co</footer></body></html>`
	got := stripHTML(in)
	for _, want := range []string{"Backend Engineer", "We need Go & Postgres.", "Kafka\ngRPC"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
	for _, bad := range []string{"var a=1", ".x{}", "Home", "©"} {
		if strings.Contains(got, bad) {
			t.Errorf("should have stripped %q: %q", bad, got)
		}
	}
}

// sse parses a text/event-stream body into (event, data) pairs.
func sse(body string) (events []string, datas []string) {
	ev := ""
	for _, line := range strings.Split(body, "\n") {
		switch {
		case strings.HasPrefix(line, "event: "):
			ev = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			events = append(events, ev)
			datas = append(datas, strings.TrimPrefix(line, "data: "))
			ev = ""
		}
	}
	return
}

func TestFitStream(t *testing.T) {
	tr, _, ctx := setup(t)
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "<html><body><h1>Backend Engineer</h1><p>"+strings.Repeat("We want Go, Postgres, Kafka and on-call experience. ", 10)+"</p></body></html>")
	}))
	defer page.Close()
	var calls int
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		b, _ := io.ReadAll(r.Body)
		var req map[string]any
		json.Unmarshal(b, &req)
		msgs := req["messages"].([]any)
		user := msgs[0].(map[string]any)["content"].(string)
		if !strings.Contains(user, "RESUME:") || !strings.Contains(user, "Kafka") || !strings.Contains(user, "MY RESUME LINE") {
			t.Errorf("prompt missing resume or description: %s", user[:200])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: message_start\ndata: {\"message\":{\"usage\":{\"input_tokens\":900}}}\n\n")
		for _, chunk := range []string{"## Verdict\n", "Plausible ", "match."} {
			d, _ := json.Marshal(chunk)
			fmt.Fprintf(w, "event: content_block_delta\ndata: {\"delta\":{\"type\":\"text_delta\",\"text\":%s}}\n\n", d)
		}
		fmt.Fprint(w, "event: message_delta\ndata: {\"usage\":{\"output_tokens\":12}}\n\n")
	}))
	defer fake.Close()

	var cid, pid int64
	tr.db().QueryRow(ctx, `INSERT INTO companies(name,slug,board_type) VALUES('Co','co','manual') RETURNING id`).Scan(&cid)
	tr.db().QueryRow(ctx, `INSERT INTO postings(company_id,source,external_id,url,title,location) VALUES($1,'manual','x',$2,'Backend Engineer','Remote') RETURNING id`, cid, page.URL+"/job").Scan(&pid)
	mux := http.NewServeMux()
	tr.Routes(mux)
	do := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	fit := fmt.Sprintf("/tracker/postings/%d/fit", pid)

	// Unconfigured LLM is a clear 503, not a crash.
	if w := do("POST", fit, ""); w.Code != 503 || !strings.Contains(w.Body.String(), "ANTHROPIC_API_KEY") {
		t.Fatal(w.Code, w.Body.String())
	}
	tr.LLM = &llm.Client{APIKey: "k", Endpoint: fake.URL}
	// No resume stored yet.
	if w := do("POST", fit, ""); w.Code != 400 || !strings.Contains(w.Body.String(), "resume") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := do("PUT", "/tracker/resume", `{"text":"too short"}`); w.Code != 400 {
		t.Fatal("short resume accepted")
	}
	resume := strings.Repeat("MY RESUME LINE: built Go services on Postgres. ", 8)
	if w := do("PUT", "/tracker/resume", `{"text":`+jsonStr(resume)+`}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	// First call streams from the model, fetches the description, stores both.
	w := do("POST", fit, "")
	if w.Code != 200 || w.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatal(w.Code, w.Header())
	}
	events, datas := sse(w.Body.String())
	if len(datas) != 4 || events[3] != "done" {
		t.Fatalf("stream shape: %v %v", events, datas)
	}
	var done struct {
		Cached bool `json:"cached"`
		Report FitReport
	}
	json.Unmarshal([]byte(datas[3]), &done)
	if done.Cached || done.Report.Report != "## Verdict\nPlausible match." || done.Report.InputTokens != 900 || done.Report.OutputTokens != 12 {
		t.Fatalf("done: %+v", done)
	}
	var desc string
	tr.db().QueryRow(ctx, `SELECT description FROM postings WHERE id=$1`, pid).Scan(&desc)
	if !strings.Contains(desc, "Kafka") {
		t.Fatal("description not stored")
	}
	if w := do("GET", fit, ""); w.Code != 200 || !strings.Contains(w.Body.String(), "Plausible") {
		t.Fatal("GET report", w.Code)
	}

	// Second call is served from cache: one delta, no model call.
	w = do("POST", fit, "")
	events, datas = sse(w.Body.String())
	if calls != 1 || len(datas) != 2 || !strings.Contains(datas[1], `"cached":true`) {
		t.Fatalf("cache: calls=%d %v", calls, datas)
	}
	// force=1 regenerates; a new resume invalidates all reports; a new description invalidates one.
	do("POST", fit+"?force=1", "")
	if calls != 2 {
		t.Fatal("force did not regenerate")
	}
	do("PUT", "/tracker/resume", `{"text":`+jsonStr(resume+" updated")+`}`)
	if w := do("GET", fit, ""); w.Code != 404 {
		t.Fatal("resume change should invalidate reports")
	}
	do("POST", fit, "")
	do("PUT", fmt.Sprintf("/tracker/postings/%d/description", pid), `{"text":"`+strings.Repeat("Pasted JD text with Kafka. ", 20)+`"}`)
	if w := do("GET", fit, ""); w.Code != 404 {
		t.Fatal("description change should invalidate the report")
	}

	// A page too thin to extract from is a 422 with a useful message, not a model call.
	thin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "<html><body><div id=app></div></body></html>")
	}))
	defer thin.Close()
	var pid2 int64
	tr.db().QueryRow(ctx, `INSERT INTO postings(company_id,source,external_id,url,title) VALUES($1,'manual','y',$2,'SPA role') RETURNING id`, cid, thin.URL).Scan(&pid2)
	before := calls
	if w := do("POST", fmt.Sprintf("/tracker/postings/%d/fit", pid2), ""); w.Code != 422 || !strings.Contains(w.Body.String(), "Paste the description") || calls != before {
		t.Fatal(w.Code, w.Body.String())
	}
}

func jsonStr(s string) string { b, _ := json.Marshal(s); return string(b) }

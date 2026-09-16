package tracker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dispatch/internal/queue"
)

func TestDigestEmail(t *testing.T) {
	tr, reg, ctx := setup(t)
	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(feedJSON(entry("f1", "Acme", "SWE New Grad", "https://acme.example/jobs/1", true)))
	}))
	defer feed.Close()
	tr.FeedURL = feed.URL

	var sent int32
	var lastKey, lastBody atomic.Value
	var mode atomic.Value
	mode.Store("ok")
	resend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer re_test" {
			w.WriteHeader(401)
			return
		}
		b, _ := io.ReadAll(r.Body)
		lastBody.Store(string(b))
		lastKey.Store(r.Header.Get("Idempotency-Key"))
		if mode.Load() == "500" {
			w.WriteHeader(500)
			return
		}
		atomic.AddInt32(&sent, 1)
		w.Write([]byte(`{"id":"msg_1"}`))
	}))
	defer resend.Close()

	// Email off: digest still runs, notify job is skipped (not failed).
	tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: "s1"})
	drain(t, tr, reg, ctx)
	var st string
	var res []byte
	tr.db().QueryRow(ctx, `SELECT state,result FROM jobs WHERE type='notify.email' ORDER BY id DESC LIMIT 1`).Scan(&st, &res)
	if st != "succeeded" || !strings.Contains(string(res), "skipped") {
		t.Fatalf("unconfigured email should skip: %s %s", st, res)
	}

	// Email on: the digest with changes sends once, with a stable idempotency key.
	tr.Email = Email{APIKey: "re_test", From: "d@x.test", To: "ray@x.test", Endpoint: resend.URL}
	feedBody := feedJSON(entry("f1", "Acme", "SWE New Grad", "https://acme.example/jobs/1", true), entry("f2", "Acme", "Infra", "https://acme.example/jobs/2", true))
	feed.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(feedBody) })
	mode.Store("500")
	tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: "s2"})
	// First pass: the resend 500 makes notify retry.
	for i := 0; i < 20; i++ {
		tr.db().Exec(ctx, "UPDATE jobs SET available_at=now() WHERE state='queued' AND type<>'notify.email'")
		j, err := tr.Q.Claim(ctx)
		if err != nil {
			break
		}
		tr.Q.RunOne(ctx, reg, j)
	}
	var attempts int
	tr.db().QueryRow(ctx, `SELECT state,attempts FROM jobs WHERE type='notify.email' ORDER BY id DESC LIMIT 1`).Scan(&st, &attempts)
	if st != "queued" || attempts != 1 {
		t.Fatalf("500 from provider should retry: %s %d", st, attempts)
	}
	firstKey := lastKey.Load().(string)
	mode.Store("ok")
	drain(t, tr, reg, ctx)
	if atomic.LoadInt32(&sent) != 1 {
		t.Fatal("sent", sent)
	}
	if lastKey.Load().(string) != firstKey || !strings.HasPrefix(firstKey, "dispatch-digest-") {
		t.Fatalf("idempotency key changed across retry: %q vs %q", firstKey, lastKey.Load())
	}
	var mail map[string]any
	json.Unmarshal([]byte(lastBody.Load().(string)), &mail)
	if mail["to"].([]any)[0] != "ray@x.test" || !strings.Contains(mail["subject"].(string), "1 new") || !strings.Contains(mail["text"].(string), "Infra") {
		t.Fatalf("mail content: %v", mail)
	}

	// No changes: no email job at all.
	before := atomic.LoadInt32(&sent)
	var notifyJobs int
	tr.db().QueryRow(ctx, `SELECT count(*) FROM jobs WHERE type='notify.email'`).Scan(&notifyJobs)
	tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: "s3"})
	drain(t, tr, reg, ctx)
	var after int
	tr.db().QueryRow(ctx, `SELECT count(*) FROM jobs WHERE type='notify.email'`).Scan(&after)
	if after != notifyJobs || atomic.LoadInt32(&sent) != before {
		t.Fatal("empty digest should not email")
	}
}

func TestRenderDigest(t *testing.T) {
	subject, text, htmlBody := renderDigest(Digest{})
	if subject != "Dispatch digest: no changes" || !strings.Contains(text, "Nothing changed") || !strings.Contains(htmlBody, "Nothing changed") {
		t.Fatal(subject, text)
	}
	subject, text, htmlBody = renderDigest(Digest{
		New:           []Posting{{Company: "A<b>", Title: "T", URL: "https://x/1", Location: "NYC"}},
		FollowUps:     []FollowUp{{Company: "B", Title: "U", Status: "oa", DaysQuiet: 20}},
		FailedSources: []string{"board.poll#9: HTTP 500"},
	})
	if subject != "Dispatch digest: 1 new, 1 to follow up, 1 source errors" {
		t.Fatal(subject)
	}
	if !strings.Contains(htmlBody, "A&lt;b&gt;") || strings.Contains(htmlBody, "A<b>") {
		t.Fatal("html not escaped")
	}
	if !strings.Contains(text, "quiet 20 days") || !strings.Contains(text, "HTTP 500") {
		t.Fatal(text)
	}
}

func TestHostLimiter(t *testing.T) {
	l := newHostLimiter(5, 2) // 5/s, burst 2
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 2; i++ {
		if err := l.wait(ctx, "a"); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("burst should not wait")
	}
	// Third request on the same host must wait ~200ms; a different host does not.
	if err := l.wait(ctx, "b"); err != nil || time.Since(start) > 100*time.Millisecond {
		t.Fatal("other host throttled")
	}
	l.wait(ctx, "a")
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatal("not throttled:", el)
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	l.wait(ctx, "a")
	if err := l.wait(cctx, "a"); err == nil {
		t.Fatal("cancelled wait should error")
	}
	var nilLimiter *hostLimiter
	if err := nilLimiter.wait(ctx, "x"); err != nil {
		t.Fatal("nil limiter should be a no-op")
	}
}

package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"dispatch/internal/queue"
	"dispatch/internal/testdb"
)

func setup(t *testing.T) (*Tracker, *queue.Registry, context.Context) {
	t.Helper()
	ctx := context.Background()
	db := testdb.Open(t, "tracker")
	q := queue.New(db)
	if err := q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.MigrateExtra(ctx, Schema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "TRUNCATE events,job_dependencies,jobs,tracker_events,applications,postings,companies,tracker_state RESTART IDENTITY"); err != nil {
		t.Fatal(err)
	}
	tr := &Tracker{Q: q, HTTP: http.DefaultClient}
	reg := queue.NewRegistry()
	tr.Register(reg)
	return tr, reg, ctx
}

// drain runs claimable jobs until none are left. Jobs in retry backoff are
// pulled forward so the test does not have to sleep.
func drain(t *testing.T, tr *Tracker, reg *queue.Registry, ctx context.Context) int {
	t.Helper()
	n := 0
	for i := 0; i < 500; i++ {
		tr.db().Exec(ctx, "UPDATE jobs SET available_at=now() WHERE state='queued'")
		j, err := tr.Q.Claim(ctx)
		if err != nil {
			return n
		}
		tr.Q.RunOne(ctx, reg, j)
		n++
	}
	t.Fatal("drain did not converge")
	return n
}

func feedJSON(entries ...map[string]any) []byte {
	b, _ := json.Marshal(entries)
	return b
}

func entry(id, company, title, url string, active bool) map[string]any {
	return map[string]any{"id": id, "company_name": company, "company_url": "https://simplify.jobs/c/" + company, "title": title, "url": url, "locations": []string{"Remote"}, "category": "Software", "active": active, "is_visible": true, "date_posted": 1757900000}
}

func lastDigest(t *testing.T, tr *Tracker, ctx context.Context) Digest {
	t.Helper()
	var raw []byte
	if err := tr.db().QueryRow(ctx, `SELECT detail FROM tracker_events WHERE kind='digest' ORDER BY id DESC LIMIT 1`).Scan(&raw); err != nil {
		t.Fatal("no digest:", err)
	}
	var d Digest
	json.Unmarshal(raw, &d)
	return d
}

func TestSweepEndToEnd(t *testing.T) {
	tr, reg, ctx := setup(t)

	// A feed with two companies, served with an ETag so the 304 path can be exercised.
	var feedBody atomic.Value
	feedBody.Store(feedJSON(
		entry("f1", "Acme", "SWE New Grad", "https://acme.example/jobs/1", true),
		entry("f2", "Acme", "Backend Engineer", "https://acme.example/jobs/2", true),
		entry("f3", "Globex", "Platform Engineer", "https://globex.example/jobs/9", true),
		entry("f4", "Globex", "Old role", "https://globex.example/jobs/8", false),
	))
	var feedHits, feed304 int32
	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&feedHits, 1)
		body := feedBody.Load().([]byte)
		etag := fmt.Sprintf(`"%d"`, len(body))
		if r.Header.Get("If-None-Match") == etag {
			atomic.AddInt32(&feed304, 1)
			w.WriteHeader(304)
			return
		}
		w.Header().Set("ETag", etag)
		w.Write(body)
	}))
	defer feed.Close()
	tr.FeedURL = feed.URL

	// A Greenhouse board that will later start failing.
	var ghMode atomic.Value
	ghMode.Store("ok")
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/initech/jobs" {
			w.WriteHeader(404)
			return
		}
		switch ghMode.Load().(string) {
		case "500":
			w.WriteHeader(500)
		case "404":
			w.WriteHeader(404)
		default:
			fmt.Fprint(w, `{"jobs":[{"id":101,"title":"Software Engineer, New Grad","absolute_url":"https://boards.greenhouse.io/initech/jobs/101","location":{"name":"Austin, TX"},"first_published":"2026-09-10T00:00:00Z"},{"id":102,"title":"Infra Engineer","absolute_url":"https://boards.greenhouse.io/initech/jobs/102","location":{"name":"Remote"}}]}`)
		}
	}))
	defer gh.Close()
	tr.GreenhouseBase = gh.URL + "/"
	var initech int64
	if err := tr.db().QueryRow(ctx, `INSERT INTO companies(name,slug,board_type,board_id,followed) VALUES('Initech','initech','greenhouse','initech',true) RETURNING id`).Scan(&initech); err != nil {
		t.Fatal(err)
	}

	// Sweep 1: everything is new.
	sweepID, _, err := tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: "sweep-1"})
	if err != nil {
		t.Fatal(err)
	}
	ran := drain(t, tr, reg, ctx)
	// sweep + feed + 1 board + digest + notify (skipped: email off)
	if ran != 5 {
		t.Fatalf("ran %d jobs, want 5", ran)
	}
	var sweepResult struct {
		Children int   `json:"children"`
		DigestID int64 `json:"digest_id"`
	}
	sj, _ := tr.Q.Get(ctx, sweepID)
	json.Unmarshal(sj.Result, &sweepResult)
	if sj.State != "succeeded" || sweepResult.Children != 2 {
		t.Fatalf("sweep: %s %s", sj.State, sj.Result)
	}
	dj, _ := tr.Q.Get(ctx, sweepResult.DigestID)
	if dj.State != "succeeded" {
		t.Fatalf("digest did not run: %s %s", dj.State, dj.Error)
	}
	d := lastDigest(t, tr, ctx)
	if len(d.New) != 5 || len(d.Closed) != 0 || len(d.FailedSources) != 0 {
		t.Fatalf("digest 1: new=%d closed=%d failed=%v", len(d.New), len(d.Closed), d.FailedSources)
	}
	var open int
	tr.db().QueryRow(ctx, `SELECT count(*) FROM postings WHERE state='open'`).Scan(&open)
	if open != 5 {
		t.Fatal("open postings:", open)
	}
	var companies int
	tr.db().QueryRow(ctx, `SELECT count(*) FROM companies`).Scan(&companies)
	if companies != 3 {
		t.Fatal("companies:", companies)
	}

	// Sweep 2: nothing changed at the feed (304), board unchanged. Digest is empty.
	tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: "sweep-2"})
	drain(t, tr, reg, ctx)
	if atomic.LoadInt32(&feed304) != 1 {
		t.Fatal("feed did not use ETag / 304")
	}
	d = lastDigest(t, tr, ctx)
	if len(d.New)+len(d.Closed)+len(d.Changed) != 0 {
		t.Fatalf("digest 2 should be empty: %+v", d)
	}

	// Sweep 3: feed drops f2 and retitles f3; the board 500s once (retry) then 404s (terminal).
	feedBody.Store(feedJSON(
		entry("f1", "Acme", "SWE New Grad", "https://acme.example/jobs/1", true),
		entry("f3", "Globex", "Platform Engineer II", "https://globex.example/jobs/9", true),
	))
	ghMode.Store("500")
	tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: "sweep-3"})
	// Run until only the retrying board poll and the waiting digest remain.
	for i := 0; i < 10; i++ {
		j, err := tr.Q.Claim(ctx)
		if err != nil {
			break
		}
		tr.Q.RunOne(ctx, reg, j)
	}
	var boardState string
	var boardAttempts int
	tr.db().QueryRow(ctx, `SELECT state,attempts FROM jobs WHERE type='board.poll' ORDER BY id DESC LIMIT 1`).Scan(&boardState, &boardAttempts)
	if boardState != "queued" || boardAttempts != 1 {
		t.Fatalf("500 should retry: %s %d", boardState, boardAttempts)
	}
	ghMode.Store("404")
	drain(t, tr, reg, ctx)
	tr.db().QueryRow(ctx, `SELECT state,attempts FROM jobs WHERE type='board.poll' ORDER BY id DESC LIMIT 1`).Scan(&boardState, &boardAttempts)
	if boardState != "failed" || boardAttempts != 2 {
		t.Fatalf("404 should be terminal on attempt 2: %s %d", boardState, boardAttempts)
	}
	d = lastDigest(t, tr, ctx)
	if len(d.Closed) != 1 || d.Closed[0].ExternalID != "f2" {
		t.Fatalf("digest 3 closed: %+v", d.Closed)
	}
	if len(d.Changed) != 1 || d.Changed[0].Title != "Platform Engineer II" {
		t.Fatalf("digest 3 changed: %+v", d.Changed)
	}
	if len(d.FailedSources) != 1 {
		t.Fatalf("digest 3 should name the failed board: %v", d.FailedSources)
	}
	var boardErr string
	tr.db().QueryRow(ctx, `SELECT board_error FROM companies WHERE id=$1`, initech).Scan(&boardErr)
	if boardErr == "" {
		t.Fatal("board error not recorded on company")
	}
	// The board's postings were NOT closed by a failed poll: absence of evidence is not evidence of absence.
	tr.db().QueryRow(ctx, `SELECT count(*) FROM postings WHERE company_id=$1 AND state='open'`, initech).Scan(&open)
	if open != 2 {
		t.Fatal("failed poll closed postings:", open)
	}

	// Sweep 4: a posting comes back. It is reopened, not duplicated.
	feedBody.Store(feedJSON(
		entry("f1", "Acme", "SWE New Grad", "https://acme.example/jobs/1", true),
		entry("f2", "Acme", "Backend Engineer", "https://acme.example/jobs/2", true),
		entry("f3", "Globex", "Platform Engineer II", "https://globex.example/jobs/9", true),
	))
	ghMode.Store("ok")
	tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: "sweep-4"})
	drain(t, tr, reg, ctx)
	var f2state string
	var f2count int
	tr.db().QueryRow(ctx, `SELECT state FROM postings WHERE external_id='f2'`).Scan(&f2state)
	tr.db().QueryRow(ctx, `SELECT count(*) FROM postings WHERE external_id='f2'`).Scan(&f2count)
	if f2state != "open" || f2count != 1 {
		t.Fatalf("reopen: state=%s count=%d", f2state, f2count)
	}
	var reopened int
	tr.db().QueryRow(ctx, `SELECT count(*) FROM tracker_events WHERE kind='posting.reopened'`).Scan(&reopened)
	if reopened != 1 {
		t.Fatal("reopen event missing")
	}
}

func TestRecheck(t *testing.T) {
	tr, reg, ctx := setup(t)
	var status atomic.Int32
	status.Store(200)
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	defer site.Close()
	var cid, pid int64
	tr.db().QueryRow(ctx, `INSERT INTO companies(name,slug,board_type) VALUES('Manual Co','manual-co','manual') RETURNING id`).Scan(&cid)
	tr.db().QueryRow(ctx, `INSERT INTO postings(company_id,source,external_id,url,title,tracked) VALUES($1,'manual',$2,$2,'Pasted role',true) RETURNING id`, cid, site.URL+"/job/1").Scan(&pid)

	// Sweep includes the tracked posting; 200 keeps it open and bumps last_seen.
	tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: "s1"})
	drainWithFeed(t, tr, reg, ctx, site)
	var state string
	tr.db().QueryRow(ctx, `SELECT state FROM postings WHERE id=$1`, pid).Scan(&state)
	if state != "open" {
		t.Fatal("200 should keep open:", state)
	}

	// 404 closes it and records an event; the job itself succeeds.
	status.Store(404)
	tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: "s2"})
	drainWithFeed(t, tr, reg, ctx, site)
	tr.db().QueryRow(ctx, `SELECT state FROM postings WHERE id=$1`, pid).Scan(&state)
	var jobState string
	tr.db().QueryRow(ctx, `SELECT state FROM jobs WHERE type='posting.recheck' ORDER BY id DESC LIMIT 1`).Scan(&jobState)
	if state != "closed" || jobState != "succeeded" {
		t.Fatalf("404 should close (posting=%s) and succeed (job=%s)", state, jobState)
	}
	d := lastDigest(t, tr, ctx)
	if len(d.Closed) != 1 || d.Closed[0].ID != pid {
		t.Fatalf("digest should list the closed posting: %+v", d.Closed)
	}
	// A closed posting is no longer rechecked.
	tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: "s3"})
	drainWithFeed(t, tr, reg, ctx, site)
	var rechecks int
	tr.db().QueryRow(ctx, `SELECT count(*) FROM jobs WHERE type='posting.recheck'`).Scan(&rechecks)
	if rechecks != 2 {
		t.Fatal("closed posting still rechecked:", rechecks)
	}
}

// drainWithFeed points the feed at an empty listing (the posting site answers
// every path with its current status, so the feed needs its own server) and
// drains.
func drainWithFeed(t *testing.T, tr *Tracker, reg *queue.Registry, ctx context.Context, site *httptest.Server) {
	t.Helper()
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`[]`)) }))
	defer empty.Close()
	tr.FeedURL = empty.URL
	drain(t, tr, reg, ctx)
}

func TestDetectBoard(t *testing.T) {
	cases := map[string][2]string{
		"https://boards.greenhouse.io/stripe":           {"greenhouse", "stripe"},
		"https://boards.greenhouse.io/stripe/jobs/1234": {"greenhouse", "stripe"},
		"https://job-boards.greenhouse.io/figma":        {"greenhouse", "figma"},
		"https://jobs.lever.co/ramp/abc-def":            {"lever", "ramp"},
		"https://jobs.ashbyhq.com/vanta":                {"ashby", "vanta"},
		"https://careers.google.com/jobs/results/123":   {"", ""},
		"not a url": {"", ""},
	}
	for in, want := range cases {
		typ, id := DetectBoard(in)
		if typ != want[0] || id != want[1] {
			t.Errorf("%s: got %s/%s want %s/%s", in, typ, id, want[0], want[1])
		}
	}
	if slugify("  Sainsbury's  Ltd. ") != "sainsbury-s-ltd" {
		t.Error(slugify("  Sainsbury's  Ltd. "))
	}
}

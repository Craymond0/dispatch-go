package tracker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestHTTPFlow(t *testing.T) {
	tr, reg, ctx := setup(t)
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`[]`)) }))
	defer empty.Close()
	tr.FeedURL = empty.URL
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"jobs":[{"id":7,"title":"New Grad SWE","absolute_url":"https://boards.greenhouse.io/initech/jobs/7","location":{"name":"NYC"}}]}`))
	}))
	defer gh.Close()
	tr.GreenhouseBase = gh.URL + "/"

	mux := http.NewServeMux()
	tr.Routes(mux)
	do := func(method, path, body string) (int, map[string]any, []map[string]any) {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		var obj map[string]any
		var arr []map[string]any
		b := w.Body.Bytes()
		if len(b) > 0 && b[0] == '[' {
			json.Unmarshal(b, &arr)
		} else {
			json.Unmarshal(b, &obj)
		}
		return w.Code, obj, arr
	}

	// Follow a company by board URL; a poll is queued immediately.
	code, c, _ := do("POST", "/tracker/companies", `{"name":"Initech","board_url":"https://boards.greenhouse.io/initech/jobs/123"}`)
	if code != 201 || c["board_type"] != "greenhouse" || c["board_id"] != "initech" || c["followed"] != true {
		t.Fatalf("follow: %d %v", code, c)
	}
	if code, c, _ := do("POST", "/tracker/companies", `{"board_url":"https://careers.google.com/"}`); code != 400 {
		t.Fatal("non-ATS board accepted", code, c)
	}
	drain(t, tr, reg, ctx)
	code, _, posts := do("GET", "/tracker/postings?state=open", "")
	if code != 200 || len(posts) != 1 || posts[0]["title"] != "New Grad SWE" {
		t.Fatalf("board poll on follow: %d %v", code, posts)
	}

	// Paste a posting URL from a company not on any ATS.
	code, p, _ := do("POST", "/tracker/postings", `{"url":"https://careers.google.com/jobs/1","title":"SWE, Early Career","company":"Google"}`)
	if code != 201 || p["tracked"] != true || p["source"] != "manual" || p["company"] != "Google" {
		t.Fatalf("paste: %d %v", code, p)
	}
	pid := int64(p["id"].(float64))
	// Pasting the same URL again is the same posting.
	if code, p2, _ := do("POST", "/tracker/postings", `{"url":"https://careers.google.com/jobs/1","title":"SWE, Early Career","company":"Google"}`); code != 201 || int64(p2["id"].(float64)) != pid {
		t.Fatal("duplicate paste created a new posting")
	}
	if code, _, _ := do("POST", "/tracker/postings", `{"url":"ftp://x","company":"x"}`); code != 400 {
		t.Fatal("bad url accepted")
	}
	// Pasting a Greenhouse posting URL creates the company with its board.
	if code, p3, _ := do("POST", "/tracker/postings", `{"url":"https://boards.greenhouse.io/stripe/jobs/55","title":"Backend","company":"Stripe"}`); code != 201 || p3["company"] != "Stripe" {
		t.Fatal(code, p3)
	}
	_, _, companies := do("GET", "/tracker/companies", "")
	var stripe map[string]any
	for _, c := range companies {
		if c["slug"] == "greenhouse-stripe" {
			stripe = c
		}
	}
	if stripe == nil || stripe["followed"] != false {
		t.Fatalf("pasted ATS posting should create an unfollowed board company: %v", stripe)
	}

	// Record an application, then move it along.
	code, a, _ := do("POST", "/tracker/applications", `{"posting_id":`+itoa(pid)+`,"applied_on":"2026-09-01","resume_ref":"Tailored/Google - SWE"}`)
	if code != 201 || a["status"] != "applied" || a["applied_on"] != "2026-09-01" {
		t.Fatalf("apply: %d %v", code, a)
	}
	aid := itoa(int64(a["id"].(float64)))
	if code, _, _ := do("POST", "/tracker/applications", `{"posting_id":999999}`); code != 400 {
		t.Fatal("unknown posting accepted for application", code)
	}
	if code, a2, _ := do("PATCH", "/tracker/applications/"+aid, `{"status":"oa","last_contact_on":"2026-08-25"}`); code != 200 || a2["status"] != "oa" {
		t.Fatalf("patch: %d %v", code, a2)
	}
	if code, _, _ := do("PATCH", "/tracker/applications/"+aid, `{"status":"hired!"}`); code != 400 {
		t.Fatal("bad status accepted")
	}

	// Sweep via the API; the digest lists the follow-up because last contact was >14 days ago.
	if code, s, _ := do("POST", "/tracker/sweep", ""); code != 202 || s["job_id"] == nil {
		t.Fatal("sweep", code, s)
	}
	drain(t, tr, reg, ctx)
	code, d, _ := do("GET", "/tracker/digest", "")
	if code != 200 {
		t.Fatal(code)
	}
	fu, _ := d["follow_ups"].([]any)
	if len(fu) != 1 {
		t.Fatalf("follow-ups: %v", d["follow_ups"])
	}
	// Unfollow and untrack round-trip.
	if code, _, _ := do("PATCH", "/tracker/companies/"+itoa(int64(c["id"].(float64))), `{"followed":false}`); code != 200 {
		t.Fatal("unfollow", code)
	}
	if code, p4, _ := do("PATCH", "/tracker/postings/"+itoa(pid), `{"tracked":false}`); code != 200 || p4["tracked"] != false {
		t.Fatal("untrack", code, p4)
	}
	_, _, apps := do("GET", "/tracker/applications", "")
	if len(apps) != 1 || apps[0]["company"] != "Google" {
		t.Fatalf("applications list: %v", apps)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

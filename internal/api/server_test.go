package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dispatch/internal/analyze"
	"dispatch/internal/queue"
	"dispatch/internal/testdb"
)

func TestHTTP(t *testing.T) {
	ctx := context.Background()
	db := testdb.Open(t, "api")
	q := queue.New(db)
	if err := q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "TRUNCATE events,job_dependencies,jobs RESTART IDENTITY"); err != nil {
		t.Fatal(err)
	}
	reg := queue.NewRegistry()
	reg.Register("analyze", analyze.Handler{})
	t.Setenv("DEMO_MODE", "1")
	h := (&Server{Q: q, Reg: reg, Token: "secret"}).Handler()
	do := func(method, path, body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer secret")
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	if w := do("GET", "/jobs", "", ""); w.Code != 200 {
		t.Fatal("auth failed", w.Code)
	}
	r := httptest.NewRequest("GET", "/jobs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("missing token accepted")
	}
	r = httptest.NewRequest("GET", "/healthz", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal("healthz should not need a token")
	}

	if w := do("POST", "/jobs", `{"payload":{"description":"Go PostgreSQL"}}`, "same"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	// Same input with explicit type and different key order is the same job.
	if w := do("POST", "/jobs", `{"type":"analyze","payload":{"description":"Go PostgreSQL"}}`, "same"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if w := do("POST", "/jobs", `{"payload":{"description":"Python"}}`, "same"); w.Code != 409 {
		t.Fatal("key conflict not rejected")
	}
	if w := do("POST", "/jobs", `{"type":"nope","payload":{}}`, ""); w.Code != 400 || !strings.Contains(w.Body.String(), "known_types") {
		t.Fatal("unknown type:", w.Body.String())
	}
	if w := do("POST", "/jobs", `{"payload":{"description":"x","demo_delay_seconds":99}}`, ""); w.Code != 400 {
		t.Fatal("handler validation not applied:", w.Body.String())
	}
	if w := do("POST", "/jobs", `{"payload":{"description":"x"},"depends_on":[999]}`, ""); w.Code != 400 {
		t.Fatal("unknown dependency:", w.Body.String())
	}
	w = do("POST", "/jobs", `{"payload":{"description":"x"},"depends_on":[1]}`, "child")
	if w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	var created struct{ ID int64 }
	json.Unmarshal(w.Body.Bytes(), &created)
	w = do("GET", fmt.Sprintf("/jobs/%d", created.ID), "", "")
	var got struct {
		PendingDeps int `json:"pending_deps"`
		DependsOn   []struct {
			ID    int64  `json:"id"`
			State string `json:"state"`
		} `json:"depends_on"`
	}
	json.Unmarshal(w.Body.Bytes(), &got)
	if w.Code != 200 || got.PendingDeps != 1 || len(got.DependsOn) != 1 || got.DependsOn[0].ID != 1 || got.DependsOn[0].State != "queued" {
		t.Fatalf("GET /jobs/{id} deps wrong: %s", w.Body.String())
	}
	if w := do("GET", "/jobs/abc", "", ""); w.Code != 404 {
		t.Fatal("bad id")
	}
	if w := do("GET", "/metrics", "", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `dispatch_jobs{type="analyze",state="queued"} 2`) {
		t.Fatal("metrics:", w.Body.String())
	}
}

func TestCookieAuth(t *testing.T) {
	ctx := context.Background()
	db := testdb.Open(t, "api")
	q := queue.New(db)
	if err := q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	s := &Server{Q: q, Reg: queue.NewRegistry(), Token: "tok", Password: "pw", Static: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("SPA")) })}
	h := s.Handler()
	req := func(method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := req("GET", "/", "", nil); w.Code != 200 || w.Body.String() != "SPA" {
		t.Fatal("static should be open")
	}
	if w := req("GET", "/some/client/route", "", nil); w.Body.String() != "SPA" {
		t.Fatal("SPA fallback")
	}
	if w := req("GET", "/jobs", "", nil); w.Code != 401 {
		t.Fatal("API should need auth")
	}
	if w := req("GET", "/auth/me", "", nil); w.Code != 200 || !strings.Contains(w.Body.String(), `"authenticated":false`) {
		t.Fatal(w.Body.String())
	}
	if w := req("POST", "/auth/login", `{"password":"nope"}`, nil); w.Code != 401 {
		t.Fatal("wrong password accepted")
	}
	w := req("POST", "/auth/login", `{"password":"pw"}`, nil)
	if w.Code != 200 || len(w.Result().Cookies()) != 1 {
		t.Fatal(w.Code, w.Body.String())
	}
	c := w.Result().Cookies()[0]
	if !c.HttpOnly || c.Name != sessionCookie {
		t.Fatal("cookie flags", c)
	}
	if w := req("GET", "/jobs", "", c); w.Code != 200 {
		t.Fatal("cookie should authorise", w.Code)
	}
	if w := req("GET", "/auth/me", "", c); !strings.Contains(w.Body.String(), `"method":"cookie"`) {
		t.Fatal(w.Body.String())
	}
	forged := &http.Cookie{Name: sessionCookie, Value: strings.Replace(c.Value, "0", "1", 1)}
	if w := req("GET", "/jobs", "", forged); w.Code != 401 {
		t.Fatal("tampered cookie accepted")
	}
	w = req("POST", "/auth/logout", "", c)
	if cl := w.Result().Cookies(); len(cl) != 1 || cl[0].MaxAge != -1 {
		t.Fatal("logout should clear the cookie")
	}
	// Without DASHBOARD_PASSWORD the login route says so instead of accepting anything.
	noPw := (&Server{Q: q, Reg: queue.NewRegistry(), Token: "tok"}).Handler()
	rec := httptest.NewRecorder()
	noPw.ServeHTTP(rec, httptest.NewRequest("POST", "/auth/login", strings.NewReader(`{"password":"x"}`)))
	if rec.Code != 503 {
		t.Fatal("login without password configured should be 503, got", rec.Code)
	}
}

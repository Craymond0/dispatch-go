package tracker

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"dispatch/internal/queue"
)

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func bad(w http.ResponseWriter, msg string) { write(w, 400, map[string]string{"error": msg}) }

func pathID(r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	return id, err == nil && id > 0
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		bad(w, "invalid body: "+err.Error())
		return false
	}
	return true
}

// Routes mounts the tracker API on mux. Auth is applied by the caller.
func (t *Tracker) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /tracker/postings", t.listPostings)
	mux.HandleFunc("POST /tracker/postings", t.addPosting)
	mux.HandleFunc("PATCH /tracker/postings/{id}", t.patchPosting)
	mux.HandleFunc("GET /tracker/companies", t.listCompanies)
	mux.HandleFunc("POST /tracker/companies", t.addCompany)
	mux.HandleFunc("PATCH /tracker/companies/{id}", t.patchCompany)
	mux.HandleFunc("GET /tracker/applications", t.listApplications)
	mux.HandleFunc("POST /tracker/applications", t.addApplication)
	mux.HandleFunc("PATCH /tracker/applications/{id}", t.patchApplication)
	mux.HandleFunc("GET /tracker/digest", t.latestDigest)
	mux.HandleFunc("POST /tracker/sweep", t.startSweep)
	mux.HandleFunc("GET /tracker/status", t.status)
}

// status is the one call a dashboard makes on load.
func (t *Tracker) status(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	out := map[string]any{"email_configured": t.Email.configured()}
	var open, tracked, followed, apps int
	t.db().QueryRow(ctx, `SELECT count(*) FILTER (WHERE state='open'), count(*) FILTER (WHERE tracked AND state='open') FROM postings`).Scan(&open, &tracked)
	t.db().QueryRow(ctx, `SELECT count(*) FROM companies WHERE followed`).Scan(&followed)
	t.db().QueryRow(ctx, `SELECT count(*) FROM applications WHERE status IN ('applied','oa','phone','onsite')`).Scan(&apps)
	out["open_postings"], out["tracked_postings"], out["followed_companies"], out["active_applications"] = open, tracked, followed, apps
	if s := t.getState(ctx, "digest.last_at"); s != "" {
		out["last_digest_at"] = s
	}
	if ss, err := t.Q.Schedules(ctx); err == nil {
		for _, s := range ss {
			if s.Name == "sweep" {
				out["next_sweep_at"] = s.NextAt
				out["sweep_interval_seconds"] = int(s.Interval.Seconds())
			}
		}
	}
	var running, queued int
	t.db().QueryRow(ctx, `SELECT count(*) FILTER (WHERE state='running'), count(*) FILTER (WHERE state='queued') FROM jobs`).Scan(&running, &queued)
	out["jobs_running"], out["jobs_queued"] = running, queued
	write(w, 200, out)
}

// --- postings ----------------------------------------------------------------

func (t *Tracker) listPostings(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	where := []string{"1=1"}
	args := []any{}
	if s := q.Get("state"); s == "open" || s == "closed" {
		args = append(args, s)
		where = append(where, fmt.Sprintf("p.state=$%d", len(args)))
	}
	if q.Get("tracked") == "1" {
		where = append(where, "p.tracked")
	}
	if c := q.Get("category"); c != "" {
		args = append(args, c)
		where = append(where, fmt.Sprintf("p.category=$%d", len(args)))
	}
	if cid, err := strconv.ParseInt(q.Get("company_id"), 10, 64); err == nil {
		args = append(args, cid)
		where = append(where, fmt.Sprintf("p.company_id=$%d", len(args)))
	}
	if s := strings.TrimSpace(q.Get("q")); s != "" {
		args = append(args, "%"+s+"%")
		where = append(where, fmt.Sprintf("(p.title ILIKE $%d OR c.name ILIKE $%d)", len(args), len(args)))
	}
	limit := 100
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 && n <= 1000 {
		limit = n
	}
	args = append(args, limit)
	rows, err := t.db().Query(r.Context(), `SELECT `+postingColumns+` FROM postings p JOIN companies c ON c.id=p.company_id WHERE `+strings.Join(where, " AND ")+fmt.Sprintf(` ORDER BY p.first_seen_at DESC, p.id DESC LIMIT $%d`, len(args)), args...)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	defer rows.Close()
	out := []Posting{}
	for rows.Next() {
		p, err := scanPosting(rows)
		if err != nil {
			http.Error(w, "read failed", 500)
			return
		}
		out = append(out, p)
	}
	write(w, 200, out)
}

// addPosting records a URL the user pasted. It is tracked immediately so the
// next sweep rechecks it. If the URL is on a known ATS, the company is
// created with that board so it can be followed with one more click.
func (t *Tracker) addPosting(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL      string `json:"url"`
		Title    string `json:"title"`
		Company  string `json:"company"`
		Location string `json:"location"`
	}
	if !decode(w, r, &req) {
		return
	}
	req.URL = strings.TrimSpace(req.URL)
	if !strings.HasPrefix(req.URL, "http://") && !strings.HasPrefix(req.URL, "https://") {
		bad(w, "url must be http(s)")
		return
	}
	if strings.TrimSpace(req.Company) == "" {
		bad(w, "company is required")
		return
	}
	if strings.TrimSpace(req.Title) == "" {
		req.Title = "(untitled)"
	}
	boardType, boardID := DetectBoard(req.URL)
	slug := slugify(req.Company)
	if boardType == "" {
		boardType = "manual"
	} else {
		slug = boardType + "-" + boardID
	}
	tx, err := t.db().Begin(r.Context())
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	defer tx.Rollback(r.Context())
	cid, err := t.upsertCompany(r.Context(), tx, req.Company, slug, boardType, boardID)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	var pid int64
	err = tx.QueryRow(r.Context(), `INSERT INTO postings(company_id,source,external_id,url,title,location,tracked) VALUES($1,'manual',$2,$2,$3,$4,true)
		ON CONFLICT(source,external_id) DO UPDATE SET tracked=true RETURNING id`, cid, req.URL, req.Title, req.Location).Scan(&pid)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	tx.Exec(r.Context(), `INSERT INTO tracker_events(kind,posting_id,company_id) VALUES('posting.tracked',$1,$2)`, pid, cid)
	if err := tx.Commit(r.Context()); err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	p, _ := scanPosting(t.db().QueryRow(r.Context(), `SELECT `+postingColumns+` FROM postings p JOIN companies c ON c.id=p.company_id WHERE p.id=$1`, pid))
	write(w, 201, p)
}

func (t *Tracker) patchPosting(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		http.Error(w, "not found", 404)
		return
	}
	var req struct {
		Tracked *bool `json:"tracked"`
	}
	if !decode(w, r, &req) || req.Tracked == nil {
		if req.Tracked == nil {
			bad(w, "tracked is required")
		}
		return
	}
	tag, err := t.db().Exec(r.Context(), `UPDATE postings SET tracked=$2 WHERE id=$1`, id, *req.Tracked)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	if tag.RowsAffected() == 0 {
		http.Error(w, "not found", 404)
		return
	}
	p, _ := scanPosting(t.db().QueryRow(r.Context(), `SELECT `+postingColumns+` FROM postings p JOIN companies c ON c.id=p.company_id WHERE p.id=$1`, id))
	write(w, 200, p)
}

// --- companies ---------------------------------------------------------------

func (t *Tracker) listCompanies(w http.ResponseWriter, r *http.Request) {
	where := "1=1"
	if r.URL.Query().Get("followed") == "1" {
		where = "followed"
	}
	rows, err := t.db().Query(r.Context(), `SELECT id,name,slug,board_type,board_id,followed,board_error,added_at FROM companies WHERE `+where+` ORDER BY followed DESC, name`)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	defer rows.Close()
	out := []Company{}
	for rows.Next() {
		var c Company
		if err := rows.Scan(&c.ID, &c.Name, &c.Slug, &c.BoardType, &c.BoardID, &c.Followed, &c.BoardError, &c.AddedAt); err != nil {
			http.Error(w, "read failed", 500)
			return
		}
		out = append(out, c)
	}
	write(w, 200, out)
}

// addCompany follows a company by its board URL (Greenhouse, Lever, Ashby).
func (t *Tracker) addCompany(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		BoardURL string `json:"board_url"`
	}
	if !decode(w, r, &req) {
		return
	}
	boardType, boardID := DetectBoard(req.BoardURL)
	if boardType == "" {
		bad(w, "board_url must be a Greenhouse, Lever or Ashby board URL")
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		req.Name = boardID
	}
	var c Company
	err := t.db().QueryRow(r.Context(), `INSERT INTO companies(name,slug,board_type,board_id,followed) VALUES($1,$2,$3,$4,true)
		ON CONFLICT(slug) DO UPDATE SET followed=true,name=EXCLUDED.name RETURNING id,name,slug,board_type,board_id,followed,board_error,added_at`,
		req.Name, boardType+"-"+boardID, boardType, boardID).Scan(&c.ID, &c.Name, &c.Slug, &c.BoardType, &c.BoardID, &c.Followed, &c.BoardError, &c.AddedAt)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	// Poll it right away so the user sees postings without waiting for a sweep.
	t.Q.Enqueue(r.Context(), queue.EnqueueRequest{Type: "board.poll", Payload: []byte(fmt.Sprintf(`{"company_id":%d}`, c.ID)), Key: fmt.Sprintf("board.poll:follow:%d:%d", c.ID, time.Now().Unix())})
	write(w, 201, c)
}

func (t *Tracker) patchCompany(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		http.Error(w, "not found", 404)
		return
	}
	var req struct {
		Followed *bool `json:"followed"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Followed == nil {
		bad(w, "followed is required")
		return
	}
	tag, err := t.db().Exec(r.Context(), `UPDATE companies SET followed=$2 WHERE id=$1`, id, *req.Followed)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	if tag.RowsAffected() == 0 {
		http.Error(w, "not found", 404)
		return
	}
	write(w, 200, map[string]any{"id": id, "followed": *req.Followed})
}

// --- applications ------------------------------------------------------------

type Application struct {
	ID            int64     `json:"id"`
	PostingID     int64     `json:"posting_id"`
	Company       string    `json:"company"`
	Title         string    `json:"title"`
	URL           string    `json:"url"`
	PostingState  string    `json:"posting_state"`
	AppliedOn     string    `json:"applied_on"`
	ResumeRef     string    `json:"resume_ref"`
	Status        string    `json:"status"`
	LastContactOn *string   `json:"last_contact_on"`
	Notes         string    `json:"notes"`
	UpdatedAt     time.Time `json:"updated_at"`
}

const applicationColumns = `a.id,a.posting_id,c.name,p.title,p.url,p.state,to_char(a.applied_on,'YYYY-MM-DD'),a.resume_ref,a.status,to_char(a.last_contact_on,'YYYY-MM-DD'),a.notes,a.updated_at`

func scanApplication(row pgx.Row) (Application, error) {
	var a Application
	err := row.Scan(&a.ID, &a.PostingID, &a.Company, &a.Title, &a.URL, &a.PostingState, &a.AppliedOn, &a.ResumeRef, &a.Status, &a.LastContactOn, &a.Notes, &a.UpdatedAt)
	return a, err
}

func (t *Tracker) listApplications(w http.ResponseWriter, r *http.Request) {
	rows, err := t.db().Query(r.Context(), `SELECT `+applicationColumns+` FROM applications a JOIN postings p ON p.id=a.posting_id JOIN companies c ON c.id=p.company_id ORDER BY a.applied_on DESC, a.id DESC`)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	defer rows.Close()
	out := []Application{}
	for rows.Next() {
		a, err := scanApplication(rows)
		if err != nil {
			http.Error(w, "read failed", 500)
			return
		}
		out = append(out, a)
	}
	write(w, 200, out)
}

func parseDate(s string) (time.Time, error) { return time.Parse("2006-01-02", strings.TrimSpace(s)) }

func (t *Tracker) addApplication(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PostingID int64  `json:"posting_id"`
		AppliedOn string `json:"applied_on"`
		ResumeRef string `json:"resume_ref"`
		Notes     string `json:"notes"`
	}
	if !decode(w, r, &req) {
		return
	}
	applied := time.Now()
	if req.AppliedOn != "" {
		var err error
		if applied, err = parseDate(req.AppliedOn); err != nil {
			bad(w, "applied_on must be YYYY-MM-DD")
			return
		}
	}
	var id int64
	err := t.db().QueryRow(r.Context(), `INSERT INTO applications(posting_id,applied_on,resume_ref,notes) VALUES($1,$2,$3,$4)
		ON CONFLICT(posting_id) DO UPDATE SET applied_on=EXCLUDED.applied_on,resume_ref=EXCLUDED.resume_ref,notes=EXCLUDED.notes,updated_at=now() RETURNING id`, req.PostingID, applied, req.ResumeRef, req.Notes).Scan(&id)
	if err != nil {
		if strings.Contains(err.Error(), "foreign key") {
			bad(w, "posting_id does not exist")
			return
		}
		http.Error(w, "database unavailable", 503)
		return
	}
	t.db().Exec(r.Context(), `UPDATE postings SET tracked=true WHERE id=$1`, req.PostingID)
	t.db().Exec(r.Context(), `INSERT INTO tracker_events(kind,posting_id,detail) VALUES('application.recorded',$1,$2)`, req.PostingID, fmt.Sprintf(`{"application_id":%d}`, id))
	a, _ := scanApplication(t.db().QueryRow(r.Context(), `SELECT `+applicationColumns+` FROM applications a JOIN postings p ON p.id=a.posting_id JOIN companies c ON c.id=p.company_id WHERE a.id=$1`, id))
	write(w, 201, a)
}

var statuses = map[string]bool{"applied": true, "oa": true, "phone": true, "onsite": true, "offer": true, "rejected": true, "ghosted": true, "withdrawn": true}

func (t *Tracker) patchApplication(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		http.Error(w, "not found", 404)
		return
	}
	var req struct {
		Status        *string `json:"status"`
		LastContactOn *string `json:"last_contact_on"`
		Notes         *string `json:"notes"`
		ResumeRef     *string `json:"resume_ref"`
	}
	if !decode(w, r, &req) {
		return
	}
	sets := []string{"updated_at=now()"}
	args := []any{id}
	if req.Status != nil {
		if !statuses[*req.Status] {
			bad(w, "unknown status")
			return
		}
		args = append(args, *req.Status)
		sets = append(sets, fmt.Sprintf("status=$%d", len(args)))
	}
	if req.LastContactOn != nil {
		d, err := parseDate(*req.LastContactOn)
		if err != nil {
			bad(w, "last_contact_on must be YYYY-MM-DD")
			return
		}
		args = append(args, d)
		sets = append(sets, fmt.Sprintf("last_contact_on=$%d", len(args)))
	}
	if req.Notes != nil {
		args = append(args, *req.Notes)
		sets = append(sets, fmt.Sprintf("notes=$%d", len(args)))
	}
	if req.ResumeRef != nil {
		args = append(args, *req.ResumeRef)
		sets = append(sets, fmt.Sprintf("resume_ref=$%d", len(args)))
	}
	tag, err := t.db().Exec(r.Context(), `UPDATE applications SET `+strings.Join(sets, ",")+` WHERE id=$1`, args...)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	if tag.RowsAffected() == 0 {
		http.Error(w, "not found", 404)
		return
	}
	if req.Status != nil {
		t.db().Exec(r.Context(), `INSERT INTO tracker_events(kind,detail) VALUES('application.status',$1)`, fmt.Sprintf(`{"application_id":%d,"status":%q}`, id, *req.Status))
	}
	a, err := scanApplication(t.db().QueryRow(r.Context(), `SELECT `+applicationColumns+` FROM applications a JOIN postings p ON p.id=a.posting_id JOIN companies c ON c.id=p.company_id WHERE a.id=$1`, id))
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	write(w, 200, a)
}

// --- digest & sweep ----------------------------------------------------------

func (t *Tracker) latestDigest(w http.ResponseWriter, r *http.Request) {
	var raw []byte
	err := t.db().QueryRow(r.Context(), `SELECT detail FROM tracker_events WHERE kind='digest' ORDER BY id DESC LIMIT 1`).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		write(w, 200, map[string]any{"digest": nil})
		return
	}
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(raw)
}

func (t *Tracker) startSweep(w http.ResponseWriter, r *http.Request) {
	id, created, err := t.Q.Enqueue(r.Context(), queue.EnqueueRequest{Type: "sweep", Payload: []byte(`{}`), Key: fmt.Sprintf("sweep:manual:%d", time.Now().Unix()/60)})
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	write(w, 202, map[string]any{"job_id": id, "created": created})
}

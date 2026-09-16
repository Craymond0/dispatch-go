package tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"dispatch/internal/queue"
)

func noPayload(raw json.RawMessage) error {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return errors.New("payload must be an object")
	}
	return nil
}

func idPayload(raw json.RawMessage, field string) (int64, error) {
	var m map[string]int64
	if err := json.Unmarshal(raw, &m); err != nil || m[field] <= 0 {
		return 0, fmt.Errorf("payload needs a positive %q", field)
	}
	return m[field], nil
}

// --- feed.poll ---------------------------------------------------------------

// feedPoll pulls the whole SimplifyJobs listing and reconciles every company
// in it. One job, broad coverage.
type feedPoll struct{ t *Tracker }

func (feedPoll) Validate(raw json.RawMessage) error { return noPayload(raw) }

func (h feedPoll) Run(ctx context.Context, j queue.Job) ([]byte, error) {
	t := h.t
	byCompany, names, etag, notModified, err := t.fetchFeed(ctx, t.getState(ctx, "feed.etag"))
	if err != nil {
		if errors.Is(err, errNotFound) {
			return nil, queue.Terminal(fmt.Errorf("feed URL gone: %s", t.FeedURL))
		}
		return nil, err
	}
	if notModified {
		return json.Marshal(map[string]any{"not_modified": true})
	}
	tx, err := t.db().Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var total reconcileResult
	companies := 0
	for slug, entries := range byCompany {
		cid, err := t.upsertCompany(ctx, tx, names[slug], slug, "feed", "")
		if err != nil {
			return nil, err
		}
		items := make([]incoming, 0, len(entries))
		for _, e := range entries {
			items = append(items, e.incoming())
		}
		r, err := t.reconcile(ctx, tx, cid, "feed", items)
		if err != nil {
			return nil, err
		}
		total.New += r.New
		total.Updated += r.Updated
		total.Closed += r.Closed
		total.Seen += r.Seen
		companies++
	}
	// Companies that vanished from the feed entirely: close their feed postings.
	rows, err := tx.Query(ctx, `SELECT id FROM companies WHERE board_type='feed' AND NOT (slug = ANY($1::text[]))`, keys(byCompany))
	if err != nil {
		return nil, err
	}
	var gone []int64
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		gone = append(gone, id)
	}
	rows.Close()
	for _, cid := range gone {
		r, err := t.reconcile(ctx, tx, cid, "feed", nil)
		if err != nil {
			return nil, err
		}
		total.Closed += r.Closed
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	if etag != "" {
		t.setState(ctx, "feed.etag", etag)
	}
	return json.Marshal(map[string]any{"companies": companies, "seen": total.Seen, "new": total.New, "updated": total.Updated, "closed": total.Closed})
}

func keys(m map[string][]feedEntry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// --- board.poll --------------------------------------------------------------

// boardPoll fetches one followed company's ATS board directly. Fresher and
// more complete than the feed for that company.
type boardPoll struct{ t *Tracker }

func (boardPoll) Validate(raw json.RawMessage) error {
	_, err := idPayload(raw, "company_id")
	return err
}

func (h boardPoll) Run(ctx context.Context, j queue.Job) ([]byte, error) {
	t := h.t
	cid, err := idPayload(j.Payload, "company_id")
	if err != nil {
		return nil, queue.Terminal(err)
	}
	var boardType, boardID string
	if err := t.db().QueryRow(ctx, `SELECT board_type,board_id FROM companies WHERE id=$1`, cid).Scan(&boardType, &boardID); err != nil {
		return nil, queue.Terminal(fmt.Errorf("company %d: %w", cid, err))
	}
	items, err := t.fetchBoard(ctx, boardType, boardID)
	if err != nil {
		if errors.Is(err, errNotFound) {
			// The board moved or the token is wrong. Retrying will not help; record it on the company.
			t.db().Exec(ctx, `UPDATE companies SET board_error=$2 WHERE id=$1`, cid, "board not found: "+boardType+"/"+boardID)
			return nil, queue.Terminal(fmt.Errorf("board %s/%s not found", boardType, boardID))
		}
		return nil, err
	}
	tx, err := t.db().Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	r, err := t.reconcile(ctx, tx, cid, boardType, items)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE companies SET board_error='' WHERE id=$1`, cid); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}

// --- posting.recheck ---------------------------------------------------------

// postingRecheck asks the posting's own URL whether it still exists. A 404
// or 410 is a successful result meaning "closed", not a failure: the
// interesting outcome here is the one that looks like an error.
type postingRecheck struct{ t *Tracker }

func (postingRecheck) Validate(raw json.RawMessage) error {
	_, err := idPayload(raw, "posting_id")
	return err
}

func (h postingRecheck) Run(ctx context.Context, j queue.Job) ([]byte, error) {
	t := h.t
	pid, err := idPayload(j.Payload, "posting_id")
	if err != nil {
		return nil, queue.Terminal(err)
	}
	var u, state string
	if err := t.db().QueryRow(ctx, `SELECT url,state FROM postings WHERE id=$1`, pid).Scan(&u, &state); err != nil {
		return nil, queue.Terminal(fmt.Errorf("posting %d: %w", pid, err))
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, queue.Terminal(err)
	}
	req.Header.Set("User-Agent", "dispatch-tracker/1 (+https://github.com/Craymond0/dispatch-go)")
	resp, err := t.do(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode == 404 || resp.StatusCode == 410:
		if state == "open" {
			if _, err := t.db().Exec(ctx, `UPDATE postings SET state='closed',closed_at=now() WHERE id=$1`, pid); err != nil {
				return nil, err
			}
			t.db().Exec(ctx, `INSERT INTO tracker_events(kind,posting_id,detail) VALUES('posting.closed',$1,$2)`, pid, fmt.Sprintf(`{"http":%d}`, resp.StatusCode))
		}
		return json.Marshal(map[string]any{"state": "closed", "http": resp.StatusCode})
	case resp.StatusCode == 429 || resp.StatusCode >= 500:
		return nil, fmt.Errorf("%s: HTTP %d", u, resp.StatusCode)
	case resp.StatusCode >= 400:
		// Forbidden or similar: we cannot tell. Leave state alone but note it.
		return json.Marshal(map[string]any{"state": state, "http": resp.StatusCode, "note": "could not verify"})
	}
	if _, err := t.db().Exec(ctx, `UPDATE postings SET last_seen_at=now() WHERE id=$1`, pid); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"state": "open", "http": resp.StatusCode})
}

// --- sweep ------------------------------------------------------------------

// sweep is the fan-out parent. It enqueues one feed poll, one board poll per
// followed company with a real board, one recheck per tracked open posting,
// and a digest that depends on all of them. Child idempotency keys include
// the sweep's own job id, so if the sweep is retried after a partial run it
// re-finds its existing children instead of creating duplicates.
type sweep struct{ t *Tracker }

func (sweep) Validate(raw json.RawMessage) error { return noPayload(raw) }

func (h sweep) Run(ctx context.Context, j queue.Job) ([]byte, error) {
	t := h.t
	prefix := fmt.Sprintf("sweep:%d:", j.ID)
	var children []int64
	add := func(typ, key string, payload string) error {
		id, _, err := t.Q.Enqueue(ctx, queue.EnqueueRequest{Type: typ, Payload: []byte(payload), Key: prefix + key})
		if err != nil {
			return fmt.Errorf("enqueue %s: %w", key, err)
		}
		children = append(children, id)
		return nil
	}
	if err := add("feed.poll", "feed", `{}`); err != nil {
		return nil, err
	}
	rows, err := t.db().Query(ctx, `SELECT id FROM companies WHERE followed AND board_type IN ('greenhouse','lever','ashby') ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var boards []int64
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		boards = append(boards, id)
	}
	rows.Close()
	for _, cid := range boards {
		if err := add("board.poll", fmt.Sprintf("board:%d", cid), fmt.Sprintf(`{"company_id":%d}`, cid)); err != nil {
			return nil, err
		}
	}
	rows, err = t.db().Query(ctx, `SELECT id FROM postings WHERE tracked AND state='open' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var tracked []int64
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		tracked = append(tracked, id)
	}
	rows.Close()
	for _, pid := range tracked {
		if err := add("posting.recheck", fmt.Sprintf("recheck:%d", pid), fmt.Sprintf(`{"posting_id":%d}`, pid)); err != nil {
			return nil, err
		}
	}
	digestID, _, err := t.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "digest", Payload: []byte(fmt.Sprintf(`{"sweep_id":%d}`, j.ID)), Key: prefix + "digest", DependsOn: children})
	if err != nil {
		return nil, fmt.Errorf("enqueue digest: %w", err)
	}
	return json.Marshal(map[string]any{"children": len(children), "boards": len(boards), "rechecks": len(tracked), "digest_id": digestID})
}

// --- digest -----------------------------------------------------------------

// Digest is the fan-in summary of one sweep.
type Digest struct {
	SweepID       int64      `json:"sweep_id"`
	Since         time.Time  `json:"since"`
	At            time.Time  `json:"at"`
	New           []Posting  `json:"new"`
	Closed        []Posting  `json:"closed"`
	Changed       []Posting  `json:"changed"`
	FollowUps     []FollowUp `json:"follow_ups"`
	FailedSources []string   `json:"failed_sources"`
}

// FollowUp is an application that has gone quiet.
type FollowUp struct {
	ApplicationID int64  `json:"application_id"`
	PostingID     int64  `json:"posting_id"`
	Company       string `json:"company"`
	Title         string `json:"title"`
	Status        string `json:"status"`
	DaysQuiet     int    `json:"days_quiet"`
}

// digest runs after every child of a sweep has finished, whether or not
// they succeeded. It reports what changed since the previous digest and
// names any source that failed, rather than blocking on it.
type digest struct{ t *Tracker }

func (digest) Validate(raw json.RawMessage) error { _, err := idPayload(raw, "sweep_id"); return err }

func (h digest) Run(ctx context.Context, j queue.Job) ([]byte, error) {
	t := h.t
	sweepID, err := idPayload(j.Payload, "sweep_id")
	if err != nil {
		return nil, queue.Terminal(err)
	}
	deps, err := t.Q.Dependencies(ctx, j.ID)
	if err != nil {
		return nil, err
	}
	d := Digest{SweepID: sweepID, At: time.Now().UTC()}
	for _, dep := range deps {
		if dep.State == "failed" {
			d.FailedSources = append(d.FailedSources, fmt.Sprintf("%s#%d: %s", dep.Type, dep.ID, dep.Error))
		}
	}
	since := time.Time{}
	if s := t.getState(ctx, "digest.last_at"); s != "" {
		since, _ = time.Parse(time.RFC3339Nano, s)
	}
	d.Since = since
	for _, kind := range []string{"posting.new", "posting.closed", "posting.changed"} {
		rows, err := t.db().Query(ctx, `SELECT `+postingColumns+` FROM tracker_events e JOIN postings p ON p.id=e.posting_id JOIN companies c ON c.id=p.company_id WHERE e.kind=$1 AND e.at>$2 ORDER BY e.at DESC LIMIT 200`, kind, since)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			p, err := scanPosting(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			switch kind {
			case "posting.new":
				d.New = append(d.New, p)
			case "posting.closed":
				d.Closed = append(d.Closed, p)
			default:
				d.Changed = append(d.Changed, p)
			}
		}
		rows.Close()
	}
	rows, err := t.db().Query(ctx, `SELECT a.id,p.id,c.name,p.title,a.status,(current_date - COALESCE(a.last_contact_on,a.applied_on)) FROM applications a JOIN postings p ON p.id=a.posting_id JOIN companies c ON c.id=p.company_id WHERE a.status IN ('applied','oa','phone','onsite') AND COALESCE(a.last_contact_on,a.applied_on) <= current_date - 14 ORDER BY 6 DESC`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var f FollowUp
		if err := rows.Scan(&f.ApplicationID, &f.PostingID, &f.Company, &f.Title, &f.Status, &f.DaysQuiet); err != nil {
			rows.Close()
			return nil, err
		}
		d.FollowUps = append(d.FollowUps, f)
	}
	rows.Close()
	out, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	var eventID int64
	if err := t.db().QueryRow(ctx, `INSERT INTO tracker_events(kind,detail) VALUES('digest',$1) RETURNING id`, out).Scan(&eventID); err != nil {
		return nil, err
	}
	if err := t.setState(ctx, "digest.last_at", d.At.Format(time.RFC3339Nano)); err != nil {
		return nil, err
	}
	// Only email when there is something to say. The key ties the email to
	// this digest event so a retried digest job cannot queue a second copy.
	if len(d.New)+len(d.Closed)+len(d.Changed)+len(d.FollowUps)+len(d.FailedSources) > 0 {
		if _, _, err := t.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "notify.email", Payload: []byte(fmt.Sprintf(`{"digest_event_id":%d}`, eventID)), Key: fmt.Sprintf("notify.email:digest:%d", eventID)}); err != nil {
			return nil, err
		}
	}
	return out, nil
}

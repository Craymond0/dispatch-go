// Package tracker watches job boards and records applications. It owns its
// own tables in the same database as the queue and runs its work as queue
// jobs: a periodic sweep fans out into one poll per source and one recheck
// per tracked posting, and a digest fans back in once they are all done.
package tracker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"dispatch/internal/llm"
	"dispatch/internal/queue"
)

// Schema is applied by the binary at startup under the queue's migration
// lock. Everything uses IF NOT EXISTS so it is safe to re-run.
const Schema = `
CREATE TABLE IF NOT EXISTS companies (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  slug TEXT UNIQUE NOT NULL,
  board_type TEXT NOT NULL CHECK (board_type IN ('greenhouse','lever','ashby','feed','manual')),
  board_id TEXT NOT NULL DEFAULT '',
  followed BOOLEAN NOT NULL DEFAULT false,
  board_error TEXT NOT NULL DEFAULT '',
  added_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS postings (
  id BIGSERIAL PRIMARY KEY,
  company_id BIGINT NOT NULL REFERENCES companies(id),
  source TEXT NOT NULL,
  external_id TEXT NOT NULL,
  url TEXT NOT NULL,
  title TEXT NOT NULL,
  location TEXT NOT NULL DEFAULT '',
  category TEXT NOT NULL DEFAULT '',
  posted_at TIMESTAMPTZ,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  state TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open','closed')),
  closed_at TIMESTAMPTZ,
  content_hash TEXT NOT NULL DEFAULT '',
  tracked BOOLEAN NOT NULL DEFAULT false,
  raw JSONB,
  UNIQUE (source, external_id));
CREATE INDEX IF NOT EXISTS postings_open ON postings(state, first_seen_at DESC);
CREATE INDEX IF NOT EXISTS postings_company ON postings(company_id);
CREATE TABLE IF NOT EXISTS applications (
  id BIGSERIAL PRIMARY KEY,
  posting_id BIGINT UNIQUE NOT NULL REFERENCES postings(id),
  applied_on DATE NOT NULL,
  resume_ref TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'applied' CHECK (status IN ('applied','oa','phone','onsite','offer','rejected','ghosted','withdrawn')),
  last_contact_on DATE,
  notes TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS tracker_events (
  id BIGSERIAL PRIMARY KEY,
  kind TEXT NOT NULL,
  posting_id BIGINT REFERENCES postings(id),
  company_id BIGINT REFERENCES companies(id),
  detail JSONB,
  at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS tracker_events_at ON tracker_events(at DESC);
CREATE TABLE IF NOT EXISTS tracker_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
` + fitSchema

// Tracker holds the dependencies the job handlers need. Base URLs are fields
// so tests can point them at httptest servers.
type Tracker struct {
	Q    *queue.Queue
	HTTP *http.Client

	FeedURL        string
	GreenhouseBase string
	LeverBase      string
	AshbyBase      string

	// Email is digest delivery; zero value means off.
	Email Email
	// LLM backs the fit analysis; unconfigured means the endpoint returns 503.
	LLM *llm.Client
	// SweepInterval is how often the scheduled sweep runs.
	SweepInterval time.Duration

	limiter *hostLimiter
}

// Defaults returns a Tracker configured for the real services.
func Defaults(q *queue.Queue) *Tracker {
	return &Tracker{
		Q:              q,
		HTTP:           &http.Client{Timeout: 60 * time.Second},
		FeedURL:        "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/dev/.github/scripts/listings.json",
		GreenhouseBase: "https://boards-api.greenhouse.io/v1/boards/",
		LeverBase:      "https://api.lever.co/v0/postings/",
		AshbyBase:      "https://api.ashbyhq.com/posting-api/job-board/",
		SweepInterval:  6 * time.Hour,
		LLM:            &llm.Client{},
		limiter:        newHostLimiter(2, 4), // 2 req/s per host, burst 4
	}
}

// Start registers the recurring sweep. Call it once at startup from any
// role; it is idempotent and preserves the existing cadence.
func (t *Tracker) Start(ctx context.Context) error {
	if t.SweepInterval <= 0 {
		return nil
	}
	return t.Q.EnsureSchedule(ctx, queue.Schedule{Name: "sweep", Type: "sweep", Interval: t.SweepInterval})
}

func (t *Tracker) db() *pgxpool.Pool { return t.Q.DB() }

// Register adds every tracker job type to the registry.
func (t *Tracker) Register(reg *queue.Registry) {
	reg.Register("feed.poll", feedPoll{t})
	reg.Register("board.poll", boardPoll{t})
	reg.Register("posting.recheck", postingRecheck{t})
	reg.Register("sweep", sweep{t})
	reg.Register("digest", digest{t})
	reg.Register("notify.email", notifyEmail{t})
}

// --- domain types --------------------------------------------------------

type Company struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	Slug       string    `json:"slug"`
	BoardType  string    `json:"board_type"`
	BoardID    string    `json:"board_id"`
	Followed   bool      `json:"followed"`
	BoardError string    `json:"board_error,omitempty"`
	AddedAt    time.Time `json:"added_at"`
}

type Posting struct {
	ID          int64      `json:"id"`
	CompanyID   int64      `json:"company_id"`
	Company     string     `json:"company"`
	Source      string     `json:"source"`
	ExternalID  string     `json:"external_id"`
	URL         string     `json:"url"`
	Title       string     `json:"title"`
	Location    string     `json:"location"`
	Category    string     `json:"category"`
	PostedAt    *time.Time `json:"posted_at"`
	FirstSeenAt time.Time  `json:"first_seen_at"`
	LastSeenAt  time.Time  `json:"last_seen_at"`
	State       string     `json:"state"`
	ClosedAt    *time.Time `json:"closed_at"`
	Tracked     bool       `json:"tracked"`
}

const postingColumns = `p.id,p.company_id,c.name,p.source,p.external_id,p.url,p.title,p.location,p.category,p.posted_at,p.first_seen_at,p.last_seen_at,p.state,p.closed_at,p.tracked`

func scanPosting(row pgx.Row) (Posting, error) {
	var p Posting
	err := row.Scan(&p.ID, &p.CompanyID, &p.Company, &p.Source, &p.ExternalID, &p.URL, &p.Title, &p.Location, &p.Category, &p.PostedAt, &p.FirstSeenAt, &p.LastSeenAt, &p.State, &p.ClosedAt, &p.Tracked)
	return p, err
}

// incoming is one posting as a source reports it, before it is reconciled
// with what is stored.
type incoming struct {
	ExternalID string
	URL        string
	Title      string
	Location   string
	Category   string
	PostedAt   *time.Time
	Raw        any
}

func (in incoming) hash() string {
	h := sha256.Sum256([]byte(in.Title + "\x00" + in.Location + "\x00" + in.URL))
	return hex.EncodeToString(h[:])[:16]
}

// reconcileResult is what a poll changed.
type reconcileResult struct {
	New, Updated, Closed, Seen int
}

// reconcile upserts one source's current listings for one company and closes
// stored postings from that source that the listing no longer contains.
// A posting the source stops listing is treated as closed; that is the only
// signal the feed and board APIs give.
func (t *Tracker) reconcile(ctx context.Context, tx pgx.Tx, companyID int64, source string, items []incoming) (reconcileResult, error) {
	var res reconcileResult
	ids := make([]string, 0, len(items))
	for _, in := range items {
		ids = append(ids, in.ExternalID)
		raw, _ := json.Marshal(in.Raw)
		var id int64
		var prevHash, prevState string
		var inserted bool
		err := tx.QueryRow(ctx, `INSERT INTO postings(company_id,source,external_id,url,title,location,category,posted_at,content_hash,raw)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			ON CONFLICT(source,external_id) DO UPDATE SET last_seen_at=now()
			RETURNING id,(xmax=0),content_hash,state`, companyID, source, in.ExternalID, in.URL, in.Title, in.Location, in.Category, in.PostedAt, in.hash(), raw).Scan(&id, &inserted, &prevHash, &prevState)
		if err != nil {
			return res, err
		}
		res.Seen++
		switch {
		case inserted:
			res.New++
			if _, err := tx.Exec(ctx, `INSERT INTO tracker_events(kind,posting_id,company_id,detail) VALUES('posting.new',$1,$2,$3)`, id, companyID, raw); err != nil {
				return res, err
			}
		case prevState == "closed":
			// It came back. Reopen rather than pretend it never left.
			res.Updated++
			if _, err := tx.Exec(ctx, `UPDATE postings SET state='open',closed_at=NULL,url=$2,title=$3,location=$4,content_hash=$5,raw=$6 WHERE id=$1`, id, in.URL, in.Title, in.Location, in.hash(), raw); err != nil {
				return res, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO tracker_events(kind,posting_id,company_id) VALUES('posting.reopened',$1,$2)`, id, companyID); err != nil {
				return res, err
			}
		case prevHash != in.hash():
			res.Updated++
			if _, err := tx.Exec(ctx, `UPDATE postings SET url=$2,title=$3,location=$4,content_hash=$5,raw=$6 WHERE id=$1`, id, in.URL, in.Title, in.Location, in.hash(), raw); err != nil {
				return res, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO tracker_events(kind,posting_id,company_id,detail) VALUES('posting.changed',$1,$2,$3)`, id, companyID, raw); err != nil {
				return res, err
			}
		}
	}
	// Anything open from this source for this company that was not in the listing is closed.
	rows, err := tx.Query(ctx, `UPDATE postings SET state='closed',closed_at=now() WHERE company_id=$1 AND source=$2 AND state='open' AND NOT (external_id = ANY($3::text[])) RETURNING id`, companyID, source, ids)
	if err != nil {
		return res, err
	}
	var closed []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return res, err
		}
		closed = append(closed, id)
	}
	rows.Close()
	for _, id := range closed {
		res.Closed++
		if _, err := tx.Exec(ctx, `INSERT INTO tracker_events(kind,posting_id,company_id) VALUES('posting.closed',$1,$2)`, id, companyID); err != nil {
			return res, err
		}
	}
	return res, nil
}

// upsertCompany returns the id for a (slug, board) pair, creating it if new.
// An existing company keeps its followed flag and board settings.
func (t *Tracker) upsertCompany(ctx context.Context, tx pgx.Tx, name, slug, boardType, boardID string) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, `INSERT INTO companies(name,slug,board_type,board_id) VALUES($1,$2,$3,$4) ON CONFLICT(slug) DO UPDATE SET name=EXCLUDED.name RETURNING id`, name, slug, boardType, boardID).Scan(&id)
	return id, err
}

// slugify makes a stable key from a company name.
func slugify(s string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(s)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevDash = false
		default:
			if !prevDash && b.Len() > 0 {
				b.WriteByte('-')
				prevDash = true
			}
		}
	}
	return strings.TrimRight(b.String(), "-")
}

// --- HTTP helpers ----------------------------------------------------------

var errNotFound = errors.New("not found")

// getJSON fetches a URL and decodes it. 5xx and transport errors are
// returned as-is so the queue retries; a 404 is errNotFound so callers can
// decide whether that is terminal (a board) or a result (a posting).
func (t *Tracker) getJSON(ctx context.Context, url string, etag string, v any) (newETag string, notModified bool, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", false, queue.Terminal(err)
	}
	req.Header.Set("User-Agent", "dispatch-tracker/1 (+https://github.com/Craymond0/dispatch-go)")
	req.Header.Set("Accept", "application/json")
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := t.do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 304:
		return etag, true, nil
	case resp.StatusCode == 404 || resp.StatusCode == 410:
		return "", false, errNotFound
	case resp.StatusCode == 429 || resp.StatusCode >= 500:
		return "", false, fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	case resp.StatusCode >= 400:
		return "", false, queue.Terminal(fmt.Errorf("%s: HTTP %d", url, resp.StatusCode))
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return "", false, fmt.Errorf("%s: decode: %w", url, err)
	}
	return resp.Header.Get("ETag"), false, nil
}

func (t *Tracker) getState(ctx context.Context, key string) string {
	var v string
	t.db().QueryRow(ctx, `SELECT value FROM tracker_state WHERE key=$1`, key).Scan(&v)
	return v
}

func (t *Tracker) setState(ctx context.Context, key, value string) error {
	_, err := t.db().Exec(ctx, `INSERT INTO tracker_state(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=now()`, key, value)
	return err
}

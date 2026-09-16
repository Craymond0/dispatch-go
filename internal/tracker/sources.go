package tracker

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// --- SimplifyJobs feed --------------------------------------------------------

// feedEntry is one row of listings.json from SimplifyJobs/New-Grad-Positions.
type feedEntry struct {
	ID          string   `json:"id"`
	CompanyName string   `json:"company_name"`
	CompanyURL  string   `json:"company_url"`
	Title       string   `json:"title"`
	URL         string   `json:"url"`
	Locations   []string `json:"locations"`
	Category    string   `json:"category"`
	Active      bool     `json:"active"`
	Visible     bool     `json:"is_visible"`
	DatePosted  int64    `json:"date_posted"`
	Sponsorship string   `json:"sponsorship"`
	Degrees     []string `json:"degrees"`
}

// fetchFeed returns the active, visible entries grouped by company slug.
// The feed is ~13 MB, so it sends If-None-Match and reports notModified when
// nothing changed.
func (t *Tracker) fetchFeed(ctx context.Context, etag string) (byCompany map[string][]feedEntry, names map[string]string, newETag string, notModified bool, err error) {
	var entries []feedEntry
	newETag, notModified, err = t.getJSON(ctx, t.FeedURL, etag, &entries)
	if err != nil || notModified {
		return nil, nil, newETag, notModified, err
	}
	byCompany = map[string][]feedEntry{}
	names = map[string]string{}
	for _, e := range entries {
		if !e.Active || !e.Visible || e.URL == "" {
			continue
		}
		slug := feedSlug(e)
		byCompany[slug] = append(byCompany[slug], e)
		names[slug] = e.CompanyName
	}
	return byCompany, names, newETag, false, nil
}

// feedSlug prefers the stable simplify.jobs/c/<Slug> id over the display name.
func feedSlug(e feedEntry) string {
	if i := strings.LastIndex(e.CompanyURL, "/c/"); i >= 0 {
		if s := slugify(e.CompanyURL[i+3:]); s != "" {
			return "simplify-" + s
		}
	}
	return "simplify-" + slugify(e.CompanyName)
}

func (e feedEntry) incoming() incoming {
	var posted *time.Time
	if e.DatePosted > 0 {
		ts := time.Unix(e.DatePosted, 0).UTC()
		posted = &ts
	}
	return incoming{ExternalID: e.ID, URL: e.URL, Title: e.Title, Location: strings.Join(e.Locations, "; "), Category: e.Category, PostedAt: posted, Raw: e}
}

// --- Board APIs ---------------------------------------------------------------

// fetchBoard returns a company's current openings from its ATS public API.
func (t *Tracker) fetchBoard(ctx context.Context, boardType, boardID string) ([]incoming, error) {
	switch boardType {
	case "greenhouse":
		var resp struct {
			Jobs []struct {
				ID             int64  `json:"id"`
				Title          string `json:"title"`
				AbsoluteURL    string `json:"absolute_url"`
				FirstPublished string `json:"first_published"`
				Location       struct {
					Name string `json:"name"`
				} `json:"location"`
			} `json:"jobs"`
		}
		if _, _, err := t.getJSON(ctx, t.GreenhouseBase+url.PathEscape(boardID)+"/jobs", "", &resp); err != nil {
			return nil, err
		}
		out := make([]incoming, 0, len(resp.Jobs))
		for _, j := range resp.Jobs {
			out = append(out, incoming{ExternalID: strconv.FormatInt(j.ID, 10), URL: j.AbsoluteURL, Title: j.Title, Location: j.Location.Name, PostedAt: parseTime(j.FirstPublished), Raw: j})
		}
		return out, nil
	case "lever":
		var resp []struct {
			ID         string `json:"id"`
			Text       string `json:"text"`
			HostedURL  string `json:"hostedUrl"`
			CreatedAt  int64  `json:"createdAt"`
			Categories struct {
				Location string `json:"location"`
				Team     string `json:"team"`
			} `json:"categories"`
		}
		if _, _, err := t.getJSON(ctx, t.LeverBase+url.PathEscape(boardID)+"?mode=json", "", &resp); err != nil {
			return nil, err
		}
		out := make([]incoming, 0, len(resp))
		for _, j := range resp {
			var posted *time.Time
			if j.CreatedAt > 0 {
				ts := time.UnixMilli(j.CreatedAt).UTC()
				posted = &ts
			}
			out = append(out, incoming{ExternalID: j.ID, URL: j.HostedURL, Title: j.Text, Location: j.Categories.Location, Category: j.Categories.Team, PostedAt: posted, Raw: j})
		}
		return out, nil
	case "ashby":
		var resp struct {
			Jobs []struct {
				ID          string `json:"id"`
				Title       string `json:"title"`
				JobURL      string `json:"jobUrl"`
				Location    string `json:"location"`
				Department  string `json:"department"`
				PublishedAt string `json:"publishedAt"`
			} `json:"jobs"`
		}
		if _, _, err := t.getJSON(ctx, t.AshbyBase+url.PathEscape(boardID), "", &resp); err != nil {
			return nil, err
		}
		out := make([]incoming, 0, len(resp.Jobs))
		for _, j := range resp.Jobs {
			out = append(out, incoming{ExternalID: j.ID, URL: j.JobURL, Title: j.Title, Location: j.Location, Category: j.Department, PostedAt: parseTime(j.PublishedAt), Raw: j})
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported board type %q", boardType)
}

func parseTime(s string) *time.Time {
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05.000Z", "2006-01-02"} {
		if ts, err := time.Parse(layout, s); err == nil {
			ts = ts.UTC()
			return &ts
		}
	}
	return nil
}

// DetectBoard guesses a board from a careers URL the user pasted, e.g.
// https://boards.greenhouse.io/stripe or https://jobs.lever.co/figma or
// https://jobs.ashbyhq.com/ramp. Returns ("", "") if it cannot tell.
func DetectBoard(raw string) (boardType, boardID string) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", ""
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	host := strings.ToLower(u.Host)
	switch {
	case strings.HasSuffix(host, "greenhouse.io") && len(parts) >= 1:
		// boards.greenhouse.io/<token>[/jobs/<id>] or job-boards.greenhouse.io/<token>
		return "greenhouse", parts[0]
	case strings.HasSuffix(host, "lever.co") && len(parts) >= 1:
		return "lever", parts[0]
	case strings.HasSuffix(host, "ashbyhq.com") && len(parts) >= 1:
		return "ashby", parts[0]
	}
	return "", ""
}

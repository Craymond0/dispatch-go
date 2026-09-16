// Command dispatchctl is a small client for a running dispatch server.
//
//	export DISPATCH_URL=https://dispatch.example.com
//	export DISPATCH_TOKEN=...
//	dispatchctl status
//	dispatchctl jobs --state failed
//	dispatchctl watch
//	dispatchctl sweep
//	dispatchctl enqueue analyze '{"description":"Go and Postgres"}'
//	dispatchctl postings --q backend --limit 20
//	dispatchctl track https://boards.greenhouse.io/stripe/jobs/1 Stripe "New Grad SWE"
//	dispatchctl follow https://jobs.lever.co/ramp
//	dispatchctl fit 42
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

type client struct {
	base, token string
	http        *http.Client
}

func (c *client) do(method, path string, body any) ([]byte, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, strings.TrimRight(c.base, "/")+path, r)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 401 {
		return nil, errors.New("unauthorized: check DISPATCH_TOKEN")
	}
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(out, &e) == nil && e.Error != "" {
			return nil, fmt.Errorf("%s: %s", resp.Status, e.Error)
		}
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func (c *client) get(path string, v any) error {
	b, err := c.do("GET", path, nil)
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}
	return json.Unmarshal(b, v)
}

type job struct {
	ID          int64  `json:"id"`
	Type        string `json:"type"`
	State       string `json:"state"`
	Attempts    int    `json:"attempts"`
	PendingDeps int    `json:"pending_deps"`
	Error       string `json:"error"`
}

type posting struct {
	ID       int64  `json:"id"`
	Company  string `json:"company"`
	Title    string `json:"title"`
	Location string `json:"location"`
	State    string `json:"state"`
	Tracked  bool   `json:"tracked"`
	URL      string `json:"url"`
}

func tw() *tabwriter.Writer { return tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0) }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func usage() {
	fmt.Fprint(os.Stderr, `dispatchctl — client for a dispatch server

  status                       counts, next sweep, last digest
  jobs [--state S] [--type T]  recent jobs
  watch [--interval 2s]        poll the queue until it drains
  sweep                        run a sweep now
  enqueue TYPE [JSON]          enqueue one job
  postings [--q S] [--state S] [--tracked] [--limit N]
  track URL COMPANY [TITLE]    track a pasted posting
  follow BOARD_URL [NAME]      follow a Greenhouse/Lever/Ashby board
  applications                 recorded applications
  digest                       latest digest as JSON
  fit POSTING_ID               stream a fit analysis

Environment: DISPATCH_URL (default http://localhost:8088), DISPATCH_TOKEN
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	c := &client{
		base:  cmp(os.Getenv("DISPATCH_URL"), "http://localhost:8088"),
		token: os.Getenv("DISPATCH_TOKEN"),
		http:  &http.Client{Timeout: 10 * time.Minute},
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "status":
		err = cmdStatus(c)
	case "jobs":
		err = cmdJobs(c, args)
	case "watch":
		err = cmdWatch(c, args)
	case "sweep":
		err = cmdSweep(c)
	case "enqueue":
		err = cmdEnqueue(c, args)
	case "postings":
		err = cmdPostings(c, args)
	case "track":
		err = cmdTrack(c, args)
	case "follow":
		err = cmdFollow(c, args)
	case "applications":
		err = cmdApplications(c)
	case "digest":
		err = cmdDigest(c)
	case "fit":
		err = cmdFit(c, args)
	case "help", "-h", "--help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "dispatchctl:", err)
		os.Exit(1)
	}
}

func cmp(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}

func cmdStatus(c *client) error {
	var s map[string]any
	if err := c.get("/tracker/status", &s); err != nil {
		return err
	}
	w := tw()
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s\t%v\n", k, s[k])
	}
	return w.Flush()
}

func cmdJobs(c *client, args []string) error {
	fs := flag.NewFlagSet("jobs", flag.ExitOnError)
	state := fs.String("state", "", "filter by state")
	typ := fs.String("type", "", "filter by type")
	fs.Parse(args)
	var jobs []job
	if err := c.get("/jobs", &jobs); err != nil {
		return err
	}
	w := tw()
	fmt.Fprintln(w, "ID\tTYPE\tSTATE\tTRIES\tWAIT\tERROR")
	for _, j := range jobs {
		if (*state != "" && j.State != *state) || (*typ != "" && j.Type != *typ) {
			continue
		}
		wait := ""
		if j.PendingDeps > 0 {
			wait = fmt.Sprintf("%d deps", j.PendingDeps)
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%s\t%s\n", j.ID, j.Type, j.State, j.Attempts, wait, truncate(j.Error, 60))
	}
	return w.Flush()
}

// cmdWatch polls until the queue has nothing queued or running, which is what
// you want after kicking off a sweep.
func cmdWatch(c *client, args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	interval := fs.Duration("interval", 2*time.Second, "poll interval")
	fs.Parse(args)
	for {
		var s struct {
			Running int `json:"jobs_running"`
			Queued  int `json:"jobs_queued"`
			Open    int `json:"open_postings"`
		}
		if err := c.get("/tracker/status", &s); err != nil {
			return err
		}
		fmt.Printf("\r%s  running=%d queued=%d open_postings=%d   ", time.Now().Format("15:04:05"), s.Running, s.Queued, s.Open)
		if s.Running == 0 && s.Queued == 0 {
			fmt.Println("\nidle")
			return nil
		}
		time.Sleep(*interval)
	}
}

func cmdSweep(c *client) error {
	b, err := c.do("POST", "/tracker/sweep", nil)
	if err != nil {
		return err
	}
	fmt.Println(strings.TrimSpace(string(b)))
	return nil
}

func cmdEnqueue(c *client, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: enqueue TYPE [JSON]")
	}
	payload := json.RawMessage(`{}`)
	if len(args) > 1 {
		if !json.Valid([]byte(args[1])) {
			return errors.New("payload is not valid JSON")
		}
		payload = json.RawMessage(args[1])
	}
	b, err := c.do("POST", "/jobs", map[string]any{"type": args[0], "payload": payload})
	if err != nil {
		return err
	}
	fmt.Println(strings.TrimSpace(string(b)))
	return nil
}

func cmdPostings(c *client, args []string) error {
	fs := flag.NewFlagSet("postings", flag.ExitOnError)
	q := fs.String("q", "", "search title or company")
	state := fs.String("state", "open", "open|closed|all")
	tracked := fs.Bool("tracked", false, "tracked only")
	limit := fs.Int("limit", 40, "max rows")
	fs.Parse(args)
	path := fmt.Sprintf("/tracker/postings?limit=%d", *limit)
	if *state != "all" {
		path += "&state=" + *state
	}
	if *tracked {
		path += "&tracked=1"
	}
	if *q != "" {
		path += "&q=" + urlQuery(*q)
	}
	var ps []posting
	if err := c.get(path, &ps); err != nil {
		return err
	}
	w := tw()
	fmt.Fprintln(w, "ID\tCOMPANY\tTITLE\tLOCATION\tSTATE")
	for _, p := range ps {
		mark := ""
		if p.Tracked {
			mark = "*"
		}
		fmt.Fprintf(w, "%d%s\t%s\t%s\t%s\t%s\n", p.ID, mark, truncate(p.Company, 24), truncate(p.Title, 46), truncate(p.Location, 22), p.State)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	fmt.Printf("%d shown (* = tracked)\n", len(ps))
	return nil
}

func urlQuery(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == ' ' {
			b.WriteByte('+')
		} else if r < 128 && (r == '-' || r == '_' || r == '.' || r == '~' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			b.WriteRune(r)
		} else {
			b.WriteString(fmt.Sprintf("%%%02X", r))
		}
	}
	return b.String()
}

func cmdTrack(c *client, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: track URL COMPANY [TITLE]")
	}
	body := map[string]string{"url": args[0], "company": args[1]}
	if len(args) > 2 {
		body["title"] = args[2]
	}
	b, err := c.do("POST", "/tracker/postings", body)
	if err != nil {
		return err
	}
	var p posting
	json.Unmarshal(b, &p)
	fmt.Printf("tracking #%d %s — %s\n", p.ID, p.Company, p.Title)
	return nil
}

func cmdFollow(c *client, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: follow BOARD_URL [NAME]")
	}
	body := map[string]string{"board_url": args[0]}
	if len(args) > 1 {
		body["name"] = args[1]
	}
	b, err := c.do("POST", "/tracker/companies", body)
	if err != nil {
		return err
	}
	var co struct {
		Name      string `json:"name"`
		BoardType string `json:"board_type"`
		BoardID   string `json:"board_id"`
	}
	json.Unmarshal(b, &co)
	fmt.Printf("following %s (%s/%s); a poll is queued\n", co.Name, co.BoardType, co.BoardID)
	return nil
}

func cmdApplications(c *client) error {
	var as []struct {
		Company   string `json:"company"`
		Title     string `json:"title"`
		AppliedOn string `json:"applied_on"`
		Status    string `json:"status"`
		Posting   string `json:"posting_state"`
	}
	if err := c.get("/tracker/applications", &as); err != nil {
		return err
	}
	w := tw()
	fmt.Fprintln(w, "APPLIED\tCOMPANY\tTITLE\tSTATUS\tPOSTING")
	for _, a := range as {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", a.AppliedOn, truncate(a.Company, 24), truncate(a.Title, 40), a.Status, a.Posting)
	}
	return w.Flush()
}

func cmdDigest(c *client) error {
	b, err := c.do("GET", "/tracker/digest", nil)
	if err != nil {
		return err
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, b, "", "  ") == nil {
		fmt.Println(pretty.String())
		return nil
	}
	fmt.Println(string(b))
	return nil
}

// cmdFit streams the SSE fit analysis to stdout as it arrives.
func cmdFit(c *client, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fit POSTING_ID")
	}
	req, err := http.NewRequest("POST", strings.TrimRight(c.base, "/")+"/tracker/postings/"+args[0]+"/fit", nil)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	event := ""
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data := strings.TrimPrefix(line, "data: ")
			switch event {
			case "":
				var d struct {
					Text string `json:"text"`
				}
				if json.Unmarshal([]byte(data), &d) == nil {
					fmt.Print(d.Text)
				}
			case "error":
				var d struct {
					Error string `json:"error"`
				}
				json.Unmarshal([]byte(data), &d)
				fmt.Println()
				return errors.New(d.Error)
			case "done":
				fmt.Println()
				return nil
			}
			event = ""
		}
	}
	fmt.Println()
	return sc.Err()
}

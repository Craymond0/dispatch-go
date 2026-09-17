package tracker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"dispatch/internal/queue"
)

// TestFeedETagCommitsWithThePostings pins down an invariant that is easy to
// break and hard to notice: the stored ETag is a claim that everything the
// feed listed is already in the database, so it must be written in the same
// transaction as those rows. Saved separately, a poll that fails after the
// fetch leaves the ETag behind, the retry is served a 304, and the postings
// are never stored at all -- a silently empty tracker that looks healthy.
func TestFeedETagCommitsWithThePostings(t *testing.T) {
	tr, reg, ctx := setup(t)

	var notModified int32
	body := feedJSON(
		entry("f1", "Acme", "SWE New Grad", "https://acme.example/jobs/1", true),
		entry("f2", "Globex", "BOOM", "https://globex.example/jobs/9", true),
	)
	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const etag = `"v1"`
		if r.Header.Get("If-None-Match") == etag {
			atomic.AddInt32(&notModified, 1)
			w.WriteHeader(304)
			return
		}
		w.Header().Set("ETag", etag)
		w.Write(body)
	}))
	defer feed.Close()
	tr.FeedURL = feed.URL

	// Make one row unstorable, so the poll fails partway through writing.
	// Any mid-transaction failure would do; a constraint is just the one a
	// test can arrange deterministically.
	drop := `ALTER TABLE postings DROP CONSTRAINT IF EXISTS no_boom`
	tr.db().Exec(ctx, drop)
	t.Cleanup(func() { tr.db().Exec(ctx, drop) })
	if _, err := tr.db().Exec(ctx, `ALTER TABLE postings ADD CONSTRAINT no_boom CHECK (title <> 'BOOM')`); err != nil {
		t.Fatal(err)
	}

	poll := func(n int) {
		t.Helper()
		if _, _, err := tr.Q.Enqueue(ctx, queue.EnqueueRequest{Type: "feed.poll", Payload: []byte(`{}`), Key: fmt.Sprintf("poll-%d", n)}); err != nil {
			t.Fatal(err)
		}
		tr.db().Exec(ctx, `UPDATE jobs SET available_at=now() WHERE state='queued'`)
		j, err := tr.Q.Claim(ctx)
		if err != nil {
			t.Fatal(err)
		}
		tr.Q.RunOne(ctx, reg, j)
	}

	poll(1)
	if n := count(t, tr, ctx, `SELECT count(*) FROM postings`); n != 0 {
		t.Fatalf("%d postings stored by a poll that failed", n)
	}
	if got := tr.getState(ctx, "feed.etag"); got != "" {
		t.Fatalf("feed.etag = %q after a failed poll; the retry would get a 304 and store nothing", got)
	}

	// Whatever broke is fixed. The retry must refetch, not short-circuit.
	if _, err := tr.db().Exec(ctx, drop); err != nil {
		t.Fatal(err)
	}
	poll(2)
	if atomic.LoadInt32(&notModified) != 0 {
		t.Error("second poll was served a 304; it had no ETag to send, having stored nothing")
	}
	if n := count(t, tr, ctx, `SELECT count(*) FROM postings`); n != 2 {
		t.Errorf("postings=%d; want 2", n)
	}
	if got := tr.getState(ctx, "feed.etag"); got != `"v1"` {
		t.Errorf("feed.etag = %q; want the ETag of the poll that committed", got)
	}

	// Having actually stored the rows, the ETag may now be used.
	poll(3)
	if atomic.LoadInt32(&notModified) != 1 {
		t.Error("third poll refetched; the ETag from a committed poll should short-circuit it")
	}
}

func count(t *testing.T, tr *Tracker, ctx context.Context, sql string) int {
	t.Helper()
	var n int
	if err := tr.db().QueryRow(ctx, sql).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

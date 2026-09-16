# Application Tracker — Specification

Status: draft for review. Nothing built yet.
Date: 2026-09-15

---

## 1. Why this exists

### The problem
Raymond is running a full-time new grad SWE search with a large application volume. Today:

- Tailored resumes are generated one at a time by **Resume Workspace** and saved into per-job folders under `Tailored/Tailored Resumes/`, dragged into place manually.
- There is no single place that answers "what have I applied to, when, and what happened."
- Postings go dead without notice. A posting closed three weeks ago looks identical to one closed yesterday.
- When a target company opens a new role, he finds out by manually checking, or not at all.
- Follow-up timing is guesswork.

### The two halves
This is deliberately **two systems with one seam**, not one merged application.

| | Resume Workspace (exists) | Tracker (new) |
|---|---|---|
| Job | Posting in, tailored PDF out | Find, watch, and record postings |
| Work shape | One at a time, human in the loop | Hundreds of scheduled fetches |
| Runs | On demand, while you watch | Continuously, unattended |
| Network | None (you paste the posting) | Constant third-party HTTP |
| Correct engine | Local persisted queue | dispatch-go |

**Resume Workspace does not change.** Its local queue is the right design for what it does: one generation at a time, on one Mac, with a human reviewing every result. Replacing that with Postgres and multi-worker would be over-engineering, and an interviewer would say so.

The tracker is the opposite shape. Polling 50+ company boards every few hours, rechecking every tracked posting daily, against third-party services that rate limit, time out, and return garbage. That is genuinely queue-shaped work: retries, backoff, scheduling, per-domain politeness, fan-out and fan-in.

### Why it's worth building

**Practical:** Raymond opens it every day for the remainder of the search. That is the test any portfolio project should pass and most fail.

**Technical:** It gives dispatch-go real cargo. Today its jobs regex-match 21 hardcoded words against a string, which is not work. Scheduled HTTP fetches against flaky external APIs are work. It also forces the one capability dispatch-go's own README lists as missing: a workflow dependency graph.

### The honest risk
If this only ever watches ten URLs, none of the machinery is justified and it reads as over-engineering. **Mitigation:** seed it with 50 to 100 company boards from the start so the scale is real, and never claim numbers that were not measured.

---

## 2. What it does, from the user's side

**Morning, one page:**

```
3 new roles at companies you follow
  Ramp — Software Engineer, New Grad — posted 4h ago
  Figma — Backend Engineer (University Grad) — posted yesterday
  Vanta — Software Engineer I — posted yesterday

2 postings you're tracking went dead
  Stripe — New Grad SWE — 404 since this morning (applied Aug 28)
  Rippling — Backend Engineer — removed from board

Waiting on you
  DoorDash — applied 14 days ago, no response — follow up?
  Palantir — OA received 3 days ago, not started
```

**Core flows:**

1. **Follow a company.** Enter a name or board URL. The tracker resolves its board and starts polling.
2. **Track a posting.** Paste a URL, or click a discovered role. It gets fetched, parsed, stored, and rechecked on a schedule.
3. **Generate a resume.** One click hands the posting to Resume Workspace. Workspace does what it already does.
4. **Record an application.** Mark applied, with date and which resume version went out. Starts the follow-up clock.
5. **Track outcome.** Move through states as things happen.
6. **Get told what changed.** The daily digest is the product.

---

## 3. Data model

```
companies      id, name, board_type (greenhouse|lever|manual), board_id, followed, added_at
postings       id, company_id, external_id, url, title, location, remote, posted_at,
               first_seen_at, last_seen_at, state (open|closed|unknown),
               raw JSONB, content_hash
applications   id, posting_id, applied_at, resume_run_id, resume_path,
               status (applied|oa|phone|onsite|offer|rejected|ghosted),
               last_contact_at, notes
events         id, entity_type, entity_id, kind, detail JSONB, at
watches        id, posting_id, cadence, next_check_at, consecutive_failures
```

`content_hash` is how change detection works: hash the fields that matter, and a differing hash means the posting was edited.

`events` is the audit log and also what the digest reads.

---

## 4. What becomes a job

This is the part that runs on dispatch-go.

| Job type | What it does | Cadence |
|---|---|---|
| `board.poll` | Fetch one company's board, diff against last seen, insert new postings | Every 6h per company |
| `posting.fetch` | Fetch and parse one posting's detail page | Once on discovery |
| `posting.recheck` | Is it still live? Did the text change? | Daily per tracked posting |
| `sweep` | **Fan-out parent.** Spawns one `board.poll` per followed company | Daily |
| `digest` | **Fan-in.** Waits for every sweep child, summarizes, notifies | After sweep completes |
| `followup.scan` | Find applications past their follow-up threshold | Daily |

The `sweep → board.poll × N → digest` shape is the dependency graph. The digest must not run until every poll has finished, or it reports a half-empty picture. That is a real requirement, not a contrived one.

**Failure semantics per job type:**

- `board.poll` — timeout or 5xx is retryable. 404 means the board moved, terminal, flag the company.
- `posting.recheck` — 404 means closed, which is a *successful* result, not a failure. Worth noting: the interesting outcome is the one that looks like an error.
- `digest` — must still run if some polls failed. It reports partial results and says which companies it could not reach. A fan-in that blocks forever on one bad child is a bug.

---

## 5. Changes needed in dispatch-go

Ordered by dependency.

1. **Handler registry.** `type Handler interface { Run(ctx, payload) ([]byte, error) }`, a registry keyed by type, and a `type` column on jobs. Today `worker()` hardcodes one function.
2. **Typed errors.** `RetryableError` vs `TerminalError`. Today every failure retries three times, which wastes attempts on permanent 404s.
3. **Scheduling.** Jobs with a future `available_at` and a recurrence rule. The claim query already filters on `available_at`, so this is mostly a scheduler that enqueues.
4. **Leader election.** With multiple workers, exactly one must fire each scheduled job. Postgres advisory locks, same mechanism already used for migrations.
5. **Dependencies.** A `job_dependencies` table plus a `pending_deps` counter. The claim query gains one condition: never claim a job with unfinished dependencies. Decrement on child completion.
6. **Per-domain rate limiting.** A token bucket keyed by hostname so a sweep does not fire 50 simultaneous requests at one host.
7. **Dedup by natural key.** Reuse the existing idempotency key mechanism so a posting is not fetched twice in one sweep.
8. **Dead letter queue.** A `dead` state and an endpoint, so permanently failed jobs are visible rather than buried.

Items 1, 2, 5 are the core. 3, 4 make it a scheduler. 6, 7, 8 are operational polish.

---

## 6. Data sources

**Use:** Greenhouse and Lever public board APIs. Both publish documented JSON endpoints per company board. Clean structured data, no scraping, no terms-of-service problem. A large share of startups and mid-size tech companies use one or the other.

**Also fine:** individual posting URLs the user pastes, fetched once, parsed conservatively.

**Do not touch:** LinkedIn, Indeed, Glassdoor. Aggressive bot detection and terms that prohibit it. Not worth the weekend or the legal ambiguity.

**Consequence to accept:** Google, Meta, and most large companies do not use Greenhouse or Lever, so their roles come in as manually pasted URLs. The tracker covers startups automatically and big tech manually. That is a real limitation and the README should say so.

---

## 7. Explicitly out of scope for v1

- Auto-applying to anything. The tracker records, it never submits.
- Resume generation. Resume Workspace owns that entirely.
- Multi-user, accounts, or auth beyond the existing bearer token.
- Email or SMS notification. v1 notifies on the dashboard.
- Scraping any site that prohibits it.
- Any ML or LLM ranking of postings.

---

## 8. Integration with Resume Workspace

**Loosest possible coupling. The codebases do not merge.**

The tracker exposes `GET /postings/{id}` returning the posting as JSON. Resume Workspace gains one small addition: accept a posting by URL or ID instead of only by paste. That is a single new input path in a 1,181-line app that already has a working paste flow.

Going the other way, when Workspace finishes a run it can POST the run id and export path back so the application record knows which resume version went out. Optional, nice, not required for v1.

If either system is down, the other keeps working. That is the point of the seam.

---

## 9. Build order

**Phase 1 — dispatch-go: handler registry and typed errors.** No tracker yet. Purely refactoring the engine, with tests. Small commits.

**Phase 2 — dispatch-go: dependencies.** The `sweep → children → digest` shape, tested with fake jobs. This is the hardest part and it's worth doing before any real cargo exists, so failures are easy to read.

**Phase 3 — tracker: schema and Greenhouse/Lever clients.** Fetch one board, parse it, store postings. No scheduling yet, triggered by hand.

**Phase 4 — tracker: the sweep.** Scheduling, leader election, rate limiting. Now it runs on its own.

**Phase 5 — dashboard.** A single HTML page served from the Go binary. The digest view, the tracked list, the applications table. No framework.

**Phase 6 — Workspace handoff.** The one new input path.

**Phase 7 — measurement and README.** Chaos test (kill a worker mid-sweep, prove no posting is lost or double-fetched), load numbers, architecture diagram, honest limitations section.

Phases 1 and 2 are worth doing regardless of whether the tracker gets finished, because they improve dispatch-go on their own.

---

## 10. What makes this defensible in an interview

- "Jobs that spawn jobs and a join that waits for all of them" is a real distributed systems problem with real edge cases: partial failure, cycle detection, orphaned children.
- "A 404 is a success, not a failure" shows the domain modeling is thought through.
- "The digest must not block forever on one unreachable board" is the kind of detail that separates a design from a demo.
- It is used daily by its author, which is the answer to "why did you build this."
- The reliability machinery (leases, fencing, at-least-once) already exists and is tested, so the story is about extending a working system rather than starting one.

**What not to claim:** any throughput or latency number that has not been measured, and any suggestion that this required the machinery before it actually did.

---

## 11. Decisions (2026-09-15)

| Question | Decision |
|---|---|
| Hosting | **Deployed, always on.** Not local-only. |
| Discovery | **Both.** Follow companies via Greenhouse/Lever, plus manually pasted URLs. |
| Notification | **Dashboard + email digest.** |
| Workspace coupling | **One-way handoff.** Workspace gains one new input path; nothing else changes. |

### What these decisions add to the build

- **Hosting target and cost.** Fly.io or a small VPS, with managed Postgres (Neon has a usable free tier). Budget roughly $5-15/month. Needs env-based config and secrets from day one, no hardcoded Mac paths.
- **Dashboard auth.** Deployed means publicly reachable. The existing bearer token covers the API, but the dashboard needs its own login. Simplest sufficient answer: a single password setting a signed cookie.
- **Email becomes a job type.** `notify.email` joins the handler registry. Good fit: retryable, needs idempotency so a retry does not double-send. Provider: Resend or Postmark free tier, starting on their sandbox domain.
- **Handoff direction is settled.** Tracker is in the cloud, Workspace is on the Mac, so Workspace pulls from the tracker over HTTPS with the bearer token. No inbound access to the Mac required.

### Still open

1. **Seed company list.** Needs roughly 50-100 companies to follow. Can be drafted from the existing target list and expanded.
2. **Email provider account.** Requires signup; sandbox domain is fine to start.
3. **Budget ceiling.** What monthly number is acceptable before the design should change.

---

## 12. Revisions (2026-09-16)

### Data source: use SimplifyJobs/New-Grad-Positions

The repo publishes a structured file, not just a README table:
`https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/dev/.github/scripts/listings.json`

Verified 2026-09-16:

- 20,138 total entries, **3,065 active and visible**, across **1,147 distinct companies**
- Newest `date_posted` was 2026-09-15, so it is maintained daily
- Fields: `company_name, title, url, locations, category, active, is_visible, date_posted, date_updated, sponsorship, degrees, company_url`
- Active categories: AI/ML/Data 1,183 · Software 1,030 · Hardware 555 · Quant 163 · Product 119
- Application link hosts: Workday 854 · Greenhouse 344 · iCIMS 273 · Ashby 208 · Lever 196 · SmartRecruiters 194 · other 839

This removes the seed-list problem entirely. No hand-curated company list needed.

**Honest consequence:** polling one JSON file is a single job, not fan-out. The tiering below is what keeps the dependency graph justified rather than decorative.

**Three tiers:**

1. `feed.poll` — fetch `listings.json`, diff against last snapshot by entry id, emit new/changed/deactivated. One job, broad coverage, cheap. Also note the feed carries its own `active` flag, so it already detects some closures; the tracker's value over it is speed, filtering, and covering postings the feed misses.
2. `board.poll` — for companies Raymond explicitly follows, hit their Greenhouse / Lever / Ashby board directly. Fresher than the feed and more reliable. **This is where the fan-out lives.** Ashby should be added alongside Greenhouse and Lever, since the data shows 208 active listings on it.
3. `posting.recheck` — daily recheck of postings he is personally tracking, including Workday and iCIMS links the feed covers poorly.

`sweep → (feed.poll + board.poll × N + posting.recheck × M) → digest` is the dependency graph, and with tiers 2 and 3 it has real width.

**zero2sudo Instagram stories are not automatable.** Instagram has no usable API for this and scraping it is out of scope. Those stay a manual paste.

### Dashboard: React + TypeScript, not vanilla HTML

This supersedes section 9, phase 5. Reason: TypeScript is a repeatedly flagged gap in Raymond's resume and he has no shipped TypeScript. A vanilla HTML dashboard would work fine technically and close nothing on the resume.

Scope: Vite + React + TypeScript, talking to the existing Go API. Views: digest, tracked postings, applications table, job/queue status with live state, retries and metrics. The queue-status view doubles as the reliability demo.

### Optional: LLM fit analysis (decide separately)

A streaming "how well does this posting match me" panel in the dashboard, backed by Claude, grounded in the master resume. This is the only thing on the table that addresses the "LLM product features: streaming responses, prompt evals, tool use in a UI" gap, and it would carry over the genuinely good ideas from Resume Workspace (evidence grounding, validation checks) into something with a real UI.

It is also real added scope and should be decided on its own, not smuggled in.

### Optional: `dispatchctl` CLI

A small Go CLI over the same API: enqueue, list, watch, retry, drain. Perhaps a day of work, gives a clean developer-tools bullet, and is genuinely convenient for operating the tracker.

### Budget

Ceiling: **$30/month, prefer well under.** Target well under $10:

- Postgres: Neon free tier is sufficient at this data volume
- Compute: one small Fly.io machine, API and worker in the same image with different `ROLE`
- Email: Resend free tier, sandbox domain
- Feed polling costs nothing

The paid three-resource Render blueprint in the existing README should not be used.

### What this project does and does not fix

**Does fix:** backend architecture, databases, reliability engineering, daily-use ownership story, TypeScript and Node (only with the React dashboard), evidence of building with AI coding agents (only once the repo is public and the README says so), developer tooling (with the CLI).

**Does not fix:** consumer scale — no side project produces "40M users," and this should be prepared as a talking point rather than built. Product and design collaboration — a solo project is if anything negative evidence here. LLM product features — only fixed if the optional fit-analysis panel is built.

**Nothing on this list counts until the repo is public.** That is the cheapest, highest-value action available and it is currently blocked on nothing.

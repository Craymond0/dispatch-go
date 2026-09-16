# Dispatch

A Go/PostgreSQL job queue with leases, fencing, retries, idempotency and a dependency graph, and an application tracker built on it that watches job boards and records applications. The tracker is the real workload; the queue is the engine under it.

[![ci](https://github.com/Craymond0/dispatch-go/actions/workflows/ci.yml/badge.svg)](https://github.com/Craymond0/dispatch-go/actions/workflows/ci.yml)

## Run locally

Install/start Docker Desktop, then run `docker compose -p raymond-dispatch up --build -d --scale worker=2` in this folder.

API: http://localhost:8088/healthz. Prometheus: http://localhost:9098. Submit a job with POST /jobs; poll GET /jobs/{id}. GET /jobs lists the latest 100 jobs. GET /metrics exports job counts by type and state plus retry totals.

A job is `{"type": "<handler name>", "payload": {...}}`. `type` defaults to `analyze`. Each handler validates its own payload at submission, so a bad payload is a 400, not a failed job. Unknown types are rejected with the list of registered handlers.

Example (local demo):

```sh
curl -X POST http://localhost:8088/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: example-1' \
  -d '{"type":"analyze","payload":{"company":"Example","title":"Software Engineer","description":"Go, Python, PostgreSQL, Docker and Linux"}}'
```

## Layout

```
cmd/dispatch/       the binary: API by default, worker with ROLE=worker
internal/queue/     the engine: schema, claim/finish, dependencies, handler registry
internal/analyze/   the demo handler (term matching)
internal/api/       HTTP surface over the queue
internal/tracker/   the application tracker: sources, job handlers, its own tables and routes
```

## Handlers

Work is pluggable. A handler implements `Validate(payload)` and `Run(ctx, job)` and is registered by name in `newRegistry()`. The worker looks up the handler by the job's `type`; a job whose type has no handler in the running binary fails terminally rather than retrying against the same binary forever.

`analyze` is the built-in demo handler: it finds a fixed list of technology terms in a text in a single pass (see `matchTerms`).

## Tracker

The tracker turns the queue into something used daily. Every few hours a `sweep` job fans out into:

- `feed.poll`: the SimplifyJobs New-Grad-Positions listing (one 13 MB JSON file, fetched with `If-None-Match` so an unchanged feed costs a 304). About 3,000 active postings across 1,100 companies.
- `board.poll` per followed company: its Greenhouse, Lever or Ashby public board API, fresher than the feed.
- `posting.recheck` per tracked posting: does the URL still exist. A 404 here is a *result* (closed), not a failure.

and a `digest` that depends on all of them. The digest runs when every child has finished, whether or not it succeeded, and reports what changed since the last digest plus which sources failed. A fan-in that waited forever on one dead board would hide the problem; one that runs can name it.

The sweep runs on a schedule (`SWEEP_INTERVAL`, default 6h). Every worker ticks the scheduler, but on each tick exactly one wins a transaction-scoped advisory lock and enqueues whatever is due, so there is no long-lived leader to fail over and no duplicate sweeps. After an outage the schedule fires once and advances to the next future slot rather than replaying every missed interval.

Outbound requests go through a per-host token bucket (2 req/s, burst 4), so a sweep that fans out into fifty board polls cannot hammer one API.

When a digest has anything to report it queues a `notify.email` job. With `RESEND_API_KEY`, `DIGEST_FROM` and `DIGEST_TO` set it sends through Resend, passing the same idempotency key to the provider so a retried send cannot deliver twice; unset, the job succeeds as skipped so a missing key never poisons a sweep.

Child jobs use idempotency keys derived from the sweep's own job id, so a sweep retried after a partial failure re-finds its children instead of duplicating them.

Each source reconciles against what is stored: new postings are recorded, changed ones updated, and postings a source no longer lists are closed. A failed poll closes nothing: absence of evidence is not evidence of absence.

Routes (all under the API token):

```
GET    /tracker/postings?state=open&tracked=1&category=Software&q=backend
POST   /tracker/postings            {url, title, company}       paste a posting; tracked immediately
PATCH  /tracker/postings/{id}       {tracked}
GET    /tracker/companies?followed=1
POST   /tracker/companies           {name, board_url}           follow a Greenhouse/Lever/Ashby board; polled right away
PATCH  /tracker/companies/{id}      {followed}
GET    /tracker/applications
POST   /tracker/applications        {posting_id, applied_on, resume_ref, notes}
PATCH  /tracker/applications/{id}   {status, last_contact_on, notes, resume_ref}
GET    /tracker/digest              latest digest
GET    /tracker/status              counts, next sweep, last digest
POST   /tracker/sweep               run a sweep now
```

Not covered: LinkedIn, Indeed and other sites that prohibit automated access. Companies that use Workday or a custom careers site come in through the feed or by pasting a URL, and are rechecked by status code only.

## Dependencies

A job may list `depends_on: [ids]` of jobs that already exist. It is not claimable until every one of them has reached a terminal state. Dependents are released on failure as well as success: a fan-in that never runs because one input failed hides the failure, whereas one that runs can see exactly which inputs failed (`GET /jobs/{id}` lists each dependency with its state). Handlers that need all-success semantics check that themselves.

Because a job can only depend on jobs that already exist, the graph is acyclic by construction. The dependency rows are locked while the pending count is computed, so a dependency finishing at the same instant cannot be missed. Retries do not release dependents; only `succeeded` and `failed` do, whichever path produced them (a handler result, a terminal error, or the expired-lease sweep).

The analyzer uses a small literal technology dictionary. It does not score candidacy, use an LLM, or imply that matching a word establishes proficiency.

## Reliability design

- PostgreSQL persists payloads, results, retry state and completion events.
- Atomic claims use row locks and SKIP LOCKED across independent workers.
- 45-second leases reclaim abandoned work after a crash. The current workload is bounded to 30 seconds; there is no lease renewal for arbitrary long-running jobs.
- Attempt numbers fence stale workers from committing results after reassignment.
- Failures retry with exponential delays up to three attempts, then remain visible as failed jobs. A handler can return `Terminal(err)` for failures that retrying cannot fix (malformed input, a permanent 404); those fail on the spot with a `failed_terminal` event and no further attempts.
- Idempotency keys deduplicate submissions and reject a different type or payload using the same key. Without a key, one is derived from the type and the canonicalised payload, so key order in the JSON does not matter.
- Delivery is at least once. Database result writes are fenced; external side effects would need their own idempotency design.
- Structured JSON logs include job ID and attempt. Prometheus metrics are durable database-derived state and retry counts. Distributed tracing and a Grafana dashboard are future work.

Local demo inputs `demo_delay_seconds` (0–30) and `demo_fail_attempts` (0–3) let you exercise failure paths. These inputs are rejected on Render unless DEMO_MODE is explicitly enabled. Never expose the unauthenticated local-demo configuration publicly.

## Tests

`go test -race ./...` runs unit tests. Set TEST_DATABASE_URL to a dedicated disposable Postgres database to run integration tests; those tests TRUNCATE that database's jobs, events and job_dependencies tables. They cover concurrent claims, expired leases, stale-worker fencing, retries, terminal failure, successful completion, and submission deduplication.

## Deploy on Render

1. Publish the contents of this folder as a new GitHub repository named dispatch-go. Do not nest the files inside another directory.
2. In Render select New > Blueprint, select that repository, and use render.yaml.
3. The blueprint requests a paid web service, paid worker, and paid PostgreSQL database. Review the current combined price before Apply. No paid resources have been created for you.
4. Wait for all three resources to be healthy. Open the API service URL with /healthz appended; it should return {"status":"ok"}.
5. In the API service's Environment page, reveal API_TOKEN locally. Keep it private. Send `Authorization: Bearer YOUR_TOKEN` with API requests (except /healthz). Do not paste the token into chat or commit it.
6. Submit POST /jobs with a JSON description, then GET /jobs/{id}; state should become succeeded and result should contain technical_terms.

The local Prometheus container is not hosted by this blueprint. For hosted collection, configure a Prometheus instance to scrape the API's /metrics path with bearer authentication. The endpoint and structured Render logs work without that extra service. Scale the worker to two instances in Render only if you accept the additional charge.

## Scope

This version has no cancellation, user accounts, priority scheduler, arbitrary code execution, or browser dashboard. Next steps are request-duration metrics, richer telemetry, versioned migrations, pagination, retention, and an authenticated application-tracker frontend. No benchmark claims should be added to a resume before measurement.

## How this was built

The first commit was scaffolded in a ChatGPT session. Everything after it (the handler registry, the single-pass matcher, the dependency graph, the tracker, CI) was developed with Claude Code, with design decisions and their reasoning recorded in `TRACKER_SPEC.md`. Direction, review, and the decisions are mine.

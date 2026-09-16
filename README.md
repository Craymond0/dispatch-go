# Dispatch

[![ci](https://github.com/OWNER/dispatch-go/actions/workflows/ci.yml/badge.svg)](https://github.com/OWNER/dispatch-go/actions/workflows/ci.yml)

A Go/PostgreSQL background-job service for analyzing saved job descriptions. This is an API project, not yet a full application tracker or browser dashboard.

## Run locally

Install/start Docker Desktop, then run `docker compose -p raymond-dispatch up --build -d --scale worker=2` in this folder.

API: http://localhost:8088/healthz. Prometheus: http://localhost:9098. Submit a job with POST /jobs; poll GET /jobs/{id}. GET /jobs lists the latest 100 jobs. GET /metrics exports job counts by type and state plus retry totals.

A job is `{"type": "<handler name>", "payload": {...}}`. `type` defaults to `analyze`. Each handler validates its own payload at submission, so a bad payload is a 400, not a failed job. Unknown types are rejected with the list of registered handlers.

Example (local demo):

```sh
curl -X POST http://localhost:8088/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: example-1' \
  -d '{"type":"analyze","payload":{"company":"Example","title":"Software Engineer","description":"Go, Python, PostgreSQL, Docker and Linux"}}'
```

## Handlers

Work is pluggable. A handler implements `Validate(payload)` and `Run(ctx, job)` and is registered by name in `newRegistry()`. The worker looks up the handler by the job's `type`; a job whose type has no handler in the running binary fails terminally rather than retrying against the same binary forever.

`analyze` is the built-in demo handler: it finds a fixed list of technology terms in a text in a single pass (see `matchTerms`).

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

`go test -race ./...` runs unit tests. Set TEST_DATABASE_URL to a dedicated disposable Postgres database to run integration tests; those tests TRUNCATE that database's jobs/events tables. They cover concurrent claims, expired leases, stale-worker fencing, retries, terminal failure, successful completion, and submission deduplication.

## Deploy on Render

1. Publish the contents of this folder as a new GitHub repository named dispatch-go. Do not nest the files inside another directory.
2. In Render select New > Blueprint, select that repository, and use render.yaml.
3. The blueprint requests a paid web service, paid worker, and paid PostgreSQL database. Review the current combined price before Apply. No paid resources have been created for you.
4. Wait for all three resources to be healthy. Open the API service URL with /healthz appended; it should return {"status":"ok"}.
5. In the API service's Environment page, reveal API_TOKEN locally. Keep it private. Send `Authorization: Bearer YOUR_TOKEN` with API requests (except /healthz). Do not paste the token into chat or commit it.
6. Submit POST /jobs with a JSON description, then GET /jobs/{id}; state should become succeeded and result should contain technical_terms.

The local Prometheus container is not hosted by this blueprint. For hosted collection, configure a Prometheus instance to scrape the API's /metrics path with bearer authentication. The endpoint and structured Render logs work without that extra service. Scale the worker to two instances in Render only if you accept the additional charge.

## Scope

This first version intentionally has no workflow dependency graph, cancellation, user accounts, priority scheduler, arbitrary code execution, or browser dashboard. Next steps are request-duration metrics, richer telemetry, versioned migrations, pagination, retention, and an authenticated application-tracker frontend. No benchmark claims should be added to a resume before measurement.

## How this was built

The first commit was scaffolded in a ChatGPT session. Everything after it (the handler registry, the single-pass matcher, the dependency graph, the tracker, CI) was developed with Claude Code, with design decisions and their reasoning recorded in `TRACKER_SPEC.md`. Direction, review, and the decisions are mine.

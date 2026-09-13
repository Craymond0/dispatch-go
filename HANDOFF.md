# Dispatch — 2026-09-12
Resume from this permanent OneDrive folder, not /private/tmp.

Implemented: Go API, PostgreSQL queue, multiple workers, idempotency, 45-second leases, fencing, retries, JSON logs, Prometheus metrics, Docker and Render configurations. This is an API service, not a finished application-tracker UI.

Verified: TestPostgresReliability and TestAnalyzeBoundaries passed with go test -race against isolated dispatch_test database. Covers concurrent claims, expired lease recovery, stale completion rejection, retry exhaustion, success, and idempotency conflicts. No load benchmark or process-kill test yet.

Local Docker Compose project: raymond-dispatch. API localhost:8088; Prometheus localhost:9098. Rebuild from this directory before further testing because running containers may be an older build. See README.md.

Pending: GitHub remote creation/push, Render deployment and live checks. Browser automation cannot initialize. Blueprint creates paid API, worker and database; cost approval is required. No hosted URL exists. Never use production as TEST_DATABASE_URL because tests truncate jobs/events.

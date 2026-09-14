# Queued import latency

Queued imports are committed to `token_import_jobs` and `token_import_items`
before the API returns 202. Completion still means credentials were validated
and published; accepting a job does not report it as completed.

## Scheduling

Both the embedded and standalone worker listen for PostgreSQL `NOTIFY` on
`oaix_import_jobs`. New-job enqueue sends an empty notification inside its transaction;
PostgreSQL delivers it only after commit. The listener uses one dedicated
connection per worker, separate from the request pool. It scans on startup and
reconnect, drains all available batches after a wake-up, and retains a once per
minute recovery/fallback sweep. Listener failures reconnect with bounded retry.
Legacy retry-item/job operations remain covered by the periodic fallback.
The durable rows, existing queue ordering and `FOR UPDATE SKIP LOCKED` determine
ownership; notifications do not contain credentials or authorize imports.

Import processing no longer waits for request-log aggregation, retention, cost
repair or Sub2API synchronization. OAuth validation concurrency and claim batch
size retain their existing configuration. No database migration is required.

Each item update returns its actual job ID. Progress aggregation reads only
those jobs using the existing job/item index, and leaves unrelated job
heartbeats unchanged. Cost changes from work proportional to all historical
items/jobs to work proportional to the affected jobs' items.

## Timing evidence

`import batch processed` logs include `job_ids`, `queue_max_ms`, `claim_ms`,
`validation_ms`, `validation_write_ms`, `publish_ms`, `snapshot_ms` and
`duration_ms`. Values are milliseconds with sub-millisecond precision.
`publish_ms` includes account publication, item/progress persistence and the
existing marking of Sub2API targets due. The event reports counts and stage
durations, never credentials. Queue timestamps are database times;
stage durations are monotonic process measurements. A claimed batch can contain
multiple jobs; validation is concurrent, so summed per-item times are not the
batch wall time.

Read job submission/start/finish and item validation/publication timestamps to
distinguish queue time from processing. `finished_at` may precede final item
status persistence and snapshot refresh; use the final log to measure the whole
worker pass. Older synchronous imports can have identical job timestamps and
must not be interpreted as measured zero-duration imports.

## September 14, 2026 incident

Production revision `98de548` processed job 3160 (one refresh-only item):

| Event (UTC) | Time / duration |
| --- | --- |
| Submitted | 14:05:15.012478 |
| Claimed | 14:06:07.790444 |
| Queue | 52,777.966 ms |
| Per-item validation | 1,341 ms |
| Job finished | 14:06:09.544955 |
| Final worker log | 14:06:10.046716 |

The former worker ran imports only during the default 60-second maintenance
tick. Its two progress updates also scanned all items and rewrote 2,995 jobs;
production slow-query logs measured 315 ms and 495 ms. Job 3161 had a locally
usable access token and recorded 0 ms validation, but still queued for
7,382.437 ms, with the same global progress-update pattern.

10 ms is a target for the local fast path, not a maximum for all requests.
Refresh-only imports require a real OAuth round trip; job 3160's validation
alone exceeded that target. Local isolated PostgreSQL measurements with
synthetic access tokens are benchmarks, not production latency guarantees.

## Verification

Use an isolated PostgreSQL database in `OAIX_TEST_DATABASE_URL`:

```
go test ./...
go test -race ./internal/store ./internal/runtime -p 1 \
  -run 'TestImportNotification|TestImportProgress|TestImportWorkersClaim|TestImportLoop'
go test ./internal/runtime -run TestImportWorkerPostgresNotificationLatency -v
```

The tests cover commit versus rollback notification, listener disconnect and
reconnect, coalesced wake-ups, fallback sweep, cancellation, duplicate claims
across two workers, unrelated stale-job heartbeats, and single-connection pool
progress. The latency fixture measures enqueue, queue and persisted publication
separately across connections. Never run these mutation fixtures on production.

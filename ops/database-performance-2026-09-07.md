# Database performance changes, 2026-09-07

The account detail API continues to return complete current data. No stale fee
cache, deferred fee response, accounting precision change, or shorter retention
is introduced.

* `gateway_current_token_costs` receives OLD/NEW cost deltas in the request-log
  transaction, including late request-ID reuse and repricing. Initialization
  uses the same per-account advisory lock and one MVCC snapshot. Accounts still
  awaiting initialization keep the existing exact reader. Retention does not
  subtract lifetime costs. Old accounts retain their historical aggregates.
* Sub2API daily monetary values are updated only when values change. Successful
  poll timestamps live in a narrow HOT-update-friendly relation. Account value
  changes atomically invalidate their rollup; readers compute exact source
  totals until a bounded worker rebuilds it. Finalized date ranges avoid
  expanding already settled account/day combinations on every maintenance run.
* A full token-pool refresh also builds the active owners' snapshots from that
  same authoritative result. Explicit scoped refresh and failure fallback stay
  available. In-flight concurrency counters remain shared.
* Status totals and plan facets use one scan. Request outbox inserts use pgx
  batching. New idempotency claims need one durable statement; expired record
  cleanup skips locked records. Completed repairs avoid repeated write locks.

Schema 26 is additive. Startup DDL has a 500 ms lock timeout. Backfill runs after
readiness with bounded steps; individual fee initialization has a 500 ms SQL
timeout. There is no startup history scan or large index build. Rollback to the
previous Go binary remains possible without dropping accounting data.

The optional `2026-09-07-unused-log-indexes.sql` is deliberately separate from
startup. It drops only three exact legacy, non-unique, single-column hash
indexes with zero observed scans. The live catalog reported about 960 MB each;
the Go gateway has no predicates for these three columns. The script does not
remove any request data and must run outside a transaction.

## Verification

* Go full suite, `go vet ./...`, and Linux gateway build passed.
* PostgreSQL integration checks cover late costs, duplicate retries, repricing,
  rollback, historical retention, canonical merges, concurrent initialization,
  concurrent Sub2API writes/backfill, unchanged-value freshness, and facet parity.
* Token/proxy race checks passed. Sub2API concurrency checks passed 10 repeats.
* Store suite passed except the Fast reprice fixture, which requires an isolated
  empty database and passed there. HTTP integration suite passed in a clean DB
  excluding two failures also reproduced on the previous `f23f235` release:
  image compatibility fixture 503 and a fixture reading a closed response body.
* Opt-in isolated benchmark: 5,000 accounts / 230,000 daily rows / 4 MB work_mem.
  Settled-history query: 314.12 ms -> 6.36 ms; both return zero. Complete cost
  summation: 14.21 ms -> 4.62 ms; both return 230,000. These local query timings
  are not a claim of 90% reduction in total production database CPU.

Run the workload with `OAIX_RUN_PERFORMANCE_BENCHMARK=1` and a disposable
`OAIX_TEST_DATABASE_URL`, then `go test ./internal/store -run
TestPerformanceSettledHistoryBenchmark -count=1 -v`.

## PostgreSQL 18 rollout follow-up

Production verification found that the initial cost-backfill cursor exceeded
int4; pgx rejected it before execution and all accounts correctly remained on
the exact fallback. The query now explicitly binds the cursor as bigint, with
an integration test that starts at zero and completes the cursor pass.

EXPLAIN ANALYZE on PostgreSQL 18 found 220 ms of JIT work for a zero-result
settled-history query: two SRFs inflated the estimate to 500 million rows. Schema
27 encapsulates the exact fallback and missing-date expansion with bounded row
estimates. Same-size PostgreSQL 18 benchmark: 433.08 ms -> 11.63 ms, both zero;
complete costs 35.04 ms -> 6.39 ms, both 230,000. No global JIT setting is changed.

Production token metadata had the same cost-estimation problem through its
correlated identity EXISTS. A single left join gives the same readiness result
and avoids JIT: measured 894.57 ms -> 109.07 ms on the live pool. These individual
measurements do not establish a 90% reduction in total CPU.

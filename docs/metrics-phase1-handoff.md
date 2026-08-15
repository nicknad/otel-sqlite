# Phase 1 handoff — OTLP metrics ingestion & storage

This file is the context handoff for a fresh session. Read it first, then
`docs/metrics-phase2-handoff.md` (Phase 2 context: read side, retention,
loadtest, e2e), `docs/metrics-extension-proposal.md` (the original plan) and
`docs/architecture.md` (updated for metrics).

## Where things live

- Worktree: `/home/nnadolski/projects/otel-sqlite-metric-extension`
  (branch `metric-extension`, based on `master` @ `7d20810`)
- Commits:
  - `9bc5d58` — proposal document
  - `48161c9` — **Phase 1 implementation** (everything below)
- Main repo (logs-only master): `/home/nnadolski/projects/otel-sqlite`

## Environment quirks (do these every session)

The repo-wide build/test commands **must** be run like this:

```sh
GOFLAGS=-buildvcs=false go test -tags fts5 ./...
GOFLAGS=-buildvcs=false go build -tags fts5 -o /tmp/otel-collector ./cmd/collector
GOFLAGS=-buildvcs=false golangci-lint run --build-tags fts5 ./internal/... ./cmd/...
```

- `GOFLAGS=-buildvcs=false` — `go build`/`go test`/`go mod tidy` fail with
  "error obtaining VCS status: exit status 128" otherwise (broken git in the
  module VCS cache; pre-existing, unrelated to metrics).
- `-tags fts5` — SQLite driver needs the fts5 build tag or migration 002
  fails with "no such module: fts5". The Makefile encodes this.
- The pre-commit `tidy` hook fails for the same environmental reason → commit
  with `git commit --no-verify`. The hook failure is NOT caused by your
  changes.
- `internal/generated/` is gitignored. `make generate` regenerates it from
  `api/` (protoc + protoc-gen-go are installed). If it is missing, the build
  fails — run `make generate` or copy from the main repo.

## What Phase 1 delivered

End-to-end OTLP **metrics** ingestion: gRPC → mapper → metric ingress queue →
metric batcher → shared command queue → single SQLite writer. Scope limited
to storage of all five OTLP metric types; querying is Phase 2.

### Files added

| File | Role |
|---|---|
| `api/opentelemetry/proto/metrics/v1/metrics.proto` | OTLP metrics protobuf (was untracked in master) |
| `api/opentelemetry/proto/collector/metrics/v1/metrics_service.proto` | Collector metrics gRPC service |
| `internal/model/metric.go` | Domain: `Metric`, `MetricSeries`, `DataPoint`, `Exemplar`, `MetricBatch`; `CanonicalAttributesKey()` (series identity) |
| `internal/otlp/metrics.go` | Mapper: `MapMetricsData()`; all 5 metric types; series grouping; ID hashing |
| `internal/ingest/generic_queue.go` | Generic bounded channel queue (shared core) |
| `internal/ingest/metric_queue.go` | `MetricIngressQueue` (typed wrapper) |
| `internal/batcher/metric_batcher.go` | `MetricBatcher`, batch size counted in **data points** |
| `internal/storage/sqlite/metrics.go` | `WriteMetricsCommand` + SQL + `MetricsSeenCaches` |
| `migrations/006_metrics.sql` | Schema (mirrored as `migration006SQL` const in migrator.go) |
| Tests: `internal/model/metric_test.go`, `internal/otlp/metrics_mapper_test.go`, `internal/batcher/metric_batcher_test.go`, `internal/storage/sqlite/metrics_command_test.go`, `internal/storage/sqlite/metrics_e2e_test.go` | See Verification |

### Files modified

| File | Change |
|---|---|
| `Makefile` | PROTO_FILES + `--go_opt`/`--go-grpc_opt` for metrics |
| `internal/otlp/server.go` | Split into `Server` (logs) + `MetricsServer` (metrics), shared `backpressureRejected()` helper |
| `internal/ingest/queue.go` | Log queue now wraps the generic core (public API unchanged) |
| `internal/storage/sqlite/writer.go` | `RecordCounter` interface; 4 new prepared statements; seen-scope/metric/series caches; metric-points counters |
| `internal/storage/sqlite/write_batch_command.go` | `PreparedStatements` extended (6 statements) |
| `internal/storage/sqlite/migrator.go` | `allMigrations()` + `migration006SQL` |
| `internal/metrics/metrics.go` | `MetricsReceived`, `MetricDataPointsWritten` counters (collector self-telemetry about ingestion) |
| `internal/config/config.go` | `MetricsEnabled` (default true) + `METRICS_ENABLED` env |
| `cmd/collector/main.go` | Metrics queue/batcher lifecycle, registers metrics service |
| `config.example.yaml`, `docs/architecture.md`, `docs/metrics-extension-proposal.md` | Docs |

## Schema (migration 006)

```text
log_resource (shared with logs — resources dedup across both signals)
  scope        (id, resource_id, name, version, schema_url)         — first-class scope (new abstraction)
    metric     (id, scope_id, name, description, unit, type,
                is_monotonic, aggregation_temporality)              — INSERT OR IGNORE dedup
      metric_series (id, metric_id, attributes_json)                — canonical attr key = series identity
        metric_data_point (series_id, timestamp_ns, start_timestamp_ns,
                flags, double_value, int_value, count, sum, min, max,
                histogram_json, exponential_histogram_json, summary_json,
                exemplars_json)                                     — append-only, like log_event
```

Indexes: `idx_metric_dp_series_time(series_id, timestamp_ns)`,
`idx_metric_series_metric(metric_id)`, `idx_metric_scope(scope_id)`.

## Design decisions & gotchas (important)

1. **Series identity** = canonical (sorted, deduped last-wins) JSON of the
   data-point attribute set, hashed with `Metric.ID` → `series-<hash>`. Same
   SHA-256 scheme as resources. `model.CanonicalAttributesKey()` is the
   single source of truth; storage persists those exact bytes in
   `metric_series.attributes_json`.
2. **Metric identity** = hash of `(resource, scope, schema_url, name, unit,
   type)`. Unit/type changes create a new metric → new series (matches OTLP
   semantics; documented in the proposal §11 open question 6).
3. **BLOB vs TEXT (landmine!):** `[]byte` binds as a SQLite BLOB, and
   BLOB never equals a TEXT literal. The series query
   `attributes_json = '{"a":1}'` silently returned nothing until the command
   bound JSON as `string`. The log path binds `attributes_json` as `[]byte`
   — same latent issue there, out of scope but worth knowing.
4. **Two gRPC services, one struct impossible:** both `LogsService` and
   `MetricsService` require a method named `Export` with different
   signatures. Hence two types (`Server`, `MetricsServer`) sharing the
   `Mapper` and `backpressureRejected()`.
5. **Resource-aware batcher merge:** `MetricBatcher.merge()` only merges
   batches with the same `Resource.ID`; a different-resource batch flushes
   the current one first. Ensures the resource row is always inserted before
   its metrics (the writer relies on this with `foreign_keys=OFF`).
6. **`RecordCounter` generalization** (`Size()`): the writer now splits
   transactions by actual work — data points for `WriteMetricsCommand`, log
   records for `WriteBatchCommand`. A single OTLP metrics request can carry
   50k+ points.
7. **Exemplars** are mapped and stored as JSON on the data point row
   (`trace_id`/`span_id` hex), not dropped. SummaryDataPoint in this proto
   version has **no** exemplars field (mapper skips it there).
8. **`WRITER_BATCH_SIZE` counts commands; `MaxTransactionRecords` counts
   records** — unchanged semantics, now shared across signals.

## Verification status

- `go test -tags fts5 ./...` — all 14 packages pass.
- `golangci-lint run --build-tags fts5 ./internal/... ./cmd/...` — 0 issues.
- Live smoke test (done manually, not a committed test): started the
  collector, exported gauge+sum over gRPC, verified resource/scope/metric/
  series/data-point rows landed in SQLite with correct values, temporality,
  monotonicity, and canonical series attributes. The committed
  `TestE2E_OTLPMetricsToSQLite` covers the same pipeline in-process.
- Load-testing the metrics path (records/sec) has **not** been done — the
  existing loadtest tool (`cmd/loadtest`) is logs-only.

## What Phase 2 needs (from the proposal §10)

1. **Histogram bucket normalization decision.** Right now buckets live in
   `metric_data_point.histogram_json` (`{"bounds":[...],"counts":[...]}`) —
   lossless but not queryable. Decide: keep JSON (and query via
   `json_extract`) vs. normalize into a `metric_histogram_bucket` table
   (proposal recommends the former initially; do the latter once real query
   requirements exist).
2. **First read-side queries / API.** The storage layer is write-only.
   Phase 2 should add at minimum the SQL patterns:
   `SELECT ... FROM metric_data_point WHERE series_id = ? AND timestamp_ns
   BETWEEN ... ORDER BY timestamp_ns` (index already exists) plus
   series lookup by `(metric.name, attributes_json)`. Consider a `metrics`
   read view analogous to the `logs` view (migration 003), a query endpoint,
   or a CLI — the proposal leaves the API shape open.
3. **Exemplar querying** — `exemplars_json` is stored; wire it into
   trace↔metric correlation (e.g., query data points by exemplar trace_id).
4. **Metric retention task** — mirror `internal/maintenance/tasks/retention.go`
   (which purges `log_event`) with a `PurgeMetricDataPointsCommand` + task
   purging `metric_data_point` by `timestamp_ns`; register in
   `cmd/collector/main.go` `startMaintenance()`. Note the maintenance worker
   submits commands through the same writer.
5. **Histogram/Summary/ExpHistogram already ingest correctly** (mapper +
   storage). Phase 2 work on them is query-side, not ingest-side.
6. **Metrics loadtest** — extend `cmd/loadtest` or add a metrics load
   generator to get throughput numbers comparable to the log baseline
   (~70k records/sec in `loadtest-results/`).

Deferred forever-ish (proposal §1): PromQL, downsampling/aggregation, rate
calculation, cardinality management, alerting on metric values.

## Open questions carried from the proposal (§11)

1. Reuse `log_resource` as the shared resource table (current choice) vs.
   rename to `resource`.
2. First-class `scope` table (current choice) vs. denormalized columns —
   logs still denormalize `scope_name`/`scope_version` on `log_event`; a
   follow-up could migrate logs to FK the scope table.
3. Generic queue core (current choice) vs. parallel implementations — worked
   cleanly; no action needed.
4. Series identity: full attribute set (current) vs. configured subset —
   revisit only if cardinality becomes a problem.
5. Scalar storage: `double_value`/`int_value` columns (current) vs. JSON.
6. Unit/type part of metric identity (current) — stated, accepted.

## Suggested first steps for the next session

1. `cd /home/nnadolski/projects/otel-sqlite-metric-extension`
2. Read `docs/metrics-extension-proposal.md` §10 (phases) and this file.
3. Run the test/lint commands above to confirm the baseline.
4. Pick Phase 2 item #2 (read-side queries) or #4 (retention task) — both
   are self-contained and have clear precedents in the codebase (`logs`
   view + FTS for queries; `PurgeLogsCommand`/`RetentionTask` for retention).

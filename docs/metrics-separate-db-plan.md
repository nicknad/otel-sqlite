# Plan: separate SQLite database for metrics (`metrics_sqlite_path`)

Status: implemented
Related: `docs/metrics-extension-proposal.md`, `docs/metrics-phase2-handoff.md`,
`docs/loadtest-baseline.md` (mixed-load contention numbers)

## 1. Goal

Add a config entry that stores OTLP metrics in their **own SQLite database
file**, with **its own writer goroutine and its own batcher** spawned at
startup. When the entry is empty (default), metrics keep sharing the log
database and the single shared writer — today's behavior, unchanged.

Motivation (from the load-test baselines): metric data points are ~3× more
expensive to insert than log events (23.6k/s writer ceiling vs 69.4k/s for
logs), and mixed load is sub-additive (30.5k/s combined). A separate metrics
DB isolates that write load from the log path: a metrics-heavy workload can
no longer starve log persistence, and vice versa. It also lets operators
back up / prune / move the two datasets independently.

## 2. Current architecture (recap)

```
OTLP logs    -> logs ingress queue  -> log batcher  ─┐
                                                      ├─> shared command queue -> ONE sqlite.Writer -> one DB file
OTLP metrics -> metrics ingress q. -> metric batcher ─┘        (a.cmdQueue)         (a.writer)
```

- `MetricBatcher` today takes a `storage.CommandQueue` (`cmdQueue`) and calls
  `cmdQueue.Send(ctx, cmd)`; it has no reference to the writer.
- All maintenance tasks submit through one `maintenance.Worker` whose single
  `CommandSubmitter` is `a.writer` — including `MetricRetentionTask`, which
  submits `PurgeMetricDataPointsCommand`.
- The read side (`internal/query`, `cmd/metrics-query`) opens its own
  connection to whatever path it is given; it is writer-agnostic.

## 3. Config surface

```yaml
# metrics_sqlite_path: optional. When set (and metrics_enabled: true), OTLP
# metric data points are stored in their own SQLite database with their own
# writer + batcher pipeline, isolating metrics write load from the log path.
# When empty (default), metrics share sqlite_path with logs (single shared
# writer, current behavior).
metrics_sqlite_path: ""
```

| Source | Key | Default |
|---|---|---|
| YAML | `metrics_sqlite_path` | `""` (shared) |
| Env | `METRICS_SQLITE_PATH` | unset (shared) |

Changes:
- `internal/config/config.go` — add `MetricsSQLitePath string` to `Config`
  (plain field, no mapstructure needed for env) + env binding
  `METRICS_SQLITE_PATH` → `parseString`.
- `internal/config/file.go` — add `MetricsSQLitePath` to `FileConfig`
  (`yaml:"metrics_sqlite_path"`) and apply it in `loadCoreConfig` (only when
  non-empty, matching the existing string-field pattern).
- `config.example.yaml` — document the new key next to `metrics_enabled`.
- Validation (`Config.Validate`): `metrics_sqlite_path` requires
  `metrics_enabled` — recommend a startup **warning** (not an error, so
  existing configs that disable metrics don't break); strict error is an
  option if fail-fast is preferred.
- Helper: `func (c *Config) MetricsSeparateDB() bool { return c.MetricsEnabled && c.MetricsSQLitePath != "" }`.

Note: while touching `FileConfig`, fix a pre-existing gap — YAML
`metrics_enabled` is currently **not** applied by `LoadFile` (the struct has
the mapstructure tag but YAML loading is manual); only the default (`true`)
and `METRICS_ENABLED` env work. Wire it properly so
`metrics_sqlite_path` cannot be silently ignored.

## 4. Component changes

### 4.1 MetricBatcher takes the writer handle at init (required by this plan)

Replace the batcher's queue dependency with the writer (executor), so the
batcher knows exactly where its data rows go:

```go
// internal/batcher/metric_batcher.go
type MetricBatcher struct {
    ingressQueue ingest.MetricIngressQueue
    executor     storage.CommandExecutor // the *sqlite.Writer (was: cmdQueue storage.CommandQueue)
    ...
}

func NewMetricBatcher(ingressQueue ingest.MetricIngressQueue,
    executor storage.CommandExecutor, config *MetricBatcherConfig) *MetricBatcher
```

- `flushLocked()`: `_ = b.executor.Submit(context.Background(), cmd)` instead
  of `b.cmdQueue.Send(...)`. `sqlite.Writer.Submit` is exactly
  `cmdQueue.Send`, so backpressure semantics are unchanged.
- The batch-queue-depth gauge (`b.aMetrics.UpdateBatchQueueDepth(...)`)
  currently reads `b.cmdQueue.Len()`. With the queue hidden inside the
  writer, either:
  - add `func (w *Writer) QueueDepth() int { return w.cmdQueue.Len() }` and
    have the batcher assert a small interface
    (`interface { storage.CommandExecutor; QueueDepth() int }`), or
  - drop the per-batcher depth update (the writer already reports
    `command_queue_depth` itself).
  Recommend the first (keeps the existing gauge).
- The log `Batcher` can keep taking the queue for now; unifying it to the
  executor pattern is a trivial follow-up if desired.

### 4.2 `sqlite.Writer`

No structural change needed — a second instance is just a second goroutine +
connection:

```go
metricWriter, err := sqlite.NewWriter(metricCmdQueue, &sqlite.WriterConfig{
    Path:          cfg.MetricsSQLitePath,
    BatchSize:     cfg.WriterBatchSize,      // same tuning initially
    FlushInterval: cfg.WriterFlushInterval,
    WALMode:       true,
    Metrics:       a.metrics,                // shared instance -> combined counters
})
```

- Add `QueueDepth() int` (see 4.1).
- `MaxTransactionRecords` is a package-level var — both writers share the
  same configured value; fine.
- Optional optimization (not required): `WriterConfig.MetricsOnly bool` to
  skip preparing the log statements (`sqlInsertEvent`) in `initPreparedStatements`
  when the writer will only ever see `WriteMetricsCommand`s. Both writers
  today prepare all six statements; correctness is unaffected.

### 4.3 Collector wiring (`cmd/collector/main.go`)

`Application` gains fields (all nil in shared mode):

```go
metricCmdQueue  storage.CommandQueue
metricWriter    *sqlite.Writer
```

`initialize()` — when `cfg.MetricsSeparateDB()`:
1. create `metricCmdQueue = storage.NewCommandQueue(cfg.BatchQueueCapacity)`
2. create `metricWriter` (config above); on error, fail startup with a clear
   message (don't leave a half-initialized app)
3. build the metric batcher with the **writer** as its executor:

```go
a.metricBatcher = batcher.NewMetricBatcher(
    a.metricIngressQueue, a.metricWriter, &batcher.MetricBatcherConfig{...})
```

Shared mode stays exactly as today: `metricBatcher` wired to `a.cmdQueue`
via the shared writer's queue — with the batcher now receiving the (single)
`a.writer` handle instead of the raw queue, so the code path is uniform.

`start()` — start order: metric batcher before metric writer (same rule as
the log pair today).

`cleanup()` — reverse order:
1. `a.metricBatcher.Stop()` (flushes its current batch into the metrics
   writer's queue)
2. `a.metricWriter.Stop(); a.metricWriter.Wait()` (drains + executes the
   queue, then closes its DB)
3. close `a.metricCmdQueue`
4. then the existing log batcher → maintenance → log writer sequence.

The metric writer's own `RunMigrations(db)` runs the full 001–007 set on the
new file (see §7 limitation 3).

### 4.4 Maintenance routing (metric retention must hit the metrics writer)

Today `MetricRetentionTask` submits `PurgeMetricDataPointsCommand` through the
worker's single submitter (`a.writer`). With two writers, retention must go
to the metrics writer or it would purge the wrong DB.

Recommend a **per-task submitter override** — minimal, keeps the worker
generic:

```go
// internal/maintenance/task.go — optional interface
type SubmitterTask interface {
    MaintenanceTask
    Submitter() CommandSubmitter // nil = use the worker's default
}

// internal/maintenance/worker.go — in evaluateAndRun:
s := w.submitter
if st, ok := task.(SubmitterTask); ok && st.Submitter() != nil {
    s = st.Submitter()
}
err := task.Run(w.ctx, s)
```

`MetricRetentionTask` gains `WithSubmitter(s CommandSubmitter)`; collector
main calls it with `a.metricWriter` when `MetricsSeparateDB()`.

Alternative (rejected for now): a command-type routing submitter
(`PurgeMetricDataPointsCommand → metricWriter`, else `logWriter`). It works
for retention but cannot fan checkpoint/vacuum/optimize out to both DBs, and
it couples `main` to concrete command types. The per-task override is
simpler and composes.

## 5. Behavior matrix: shared vs separate

| Concern | Shared (default, `""`) | Separate (`metrics_sqlite_path` set) |
|---|---|---|
| Writers | 1 (`a.writer`) | 2 (`a.writer` + `metricWriter`) |
| Metric batcher executor | `a.writer` | `metricWriter` |
| Metric retention target | `a.writer` | `metricWriter` |
| Dedup caches (resources/scopes/metrics/series) | shared with log path | per-writer (each DB has its own rows; correct by construction) |
| Prometheus counters | combined | combined (shared `Metrics` instance — no re-registration, no new metric names) |
| Migrations | on one file | full 001–007 on both files |
| Backpressure | both batchers share one queue | independent queues per signal |
| Same service in both signals | one `log_resource` row | two rows, one per DB (expected) |

## 6. Read side / tools

- `cmd/metrics-query -db <metrics_sqlite_path>` works unchanged — it opens
  whatever path it is given and only reads `metrics`/`metric_buckets`.
- `cmd/loadtest` is unchanged; the load-test compose file can add
  `METRICS_SQLITE_PATH` to exercise the separate-DB pipeline.
- `scripts/e2e-mixed.sh` uses the default (shared) config, so it stays valid
  as a regression test for the shared path. A separate-DB e2e variant is a
  follow-up (see §9).

## 7. Decisions & limitations (documented, accepted)

1. **Checkpoint/vacuum/optimize/fts-rebuild run only against the log DB.**
   These are DB-bound `NonTransactionalCommand`s. The metrics DB is still
   safe: `openDatabase` already sets `wal_autocheckpoint=1000` and
   `journal_size_limit=64MB`, so the WAL self-manages. Extending these tasks
   to a second DB (two workers, or task fan-out) is follow-up work.
2. **Combined Prometheus counters.** Both writers share the one `Metrics`
   instance, so `metric_data_points_written_total` etc. count across DBs.
   Splitting by DB would require converting counters to `CounterVec` with a
   `db` label — a breaking change to existing dashboards; defer unless asked.
3. **The metrics DB carries the full schema** (001–007), including
   `log_event`/`logs_fts` tables it never writes. Harmless (no rows), and
   `log_resource` is genuinely needed (scope references it). A metrics-only
   migration set is a possible cleanup but adds divergence between migration
   paths — not worth it now.
4. **Same writer tuning for both pipelines** initially
   (`writer_batch_size`, `batcher_batch_size`, flush intervals). Per-signal
   knobs (`metrics_writer_batch_size`, …) are a natural follow-up if the
   load-test numbers justify them.

## 8. Testing plan

1. **Config unit tests** (`internal/config/config_test.go`):
   - `metrics_sqlite_path` round-trips through YAML and
     `METRICS_SQLITE_PATH` env; `MetricsSeparateDB()` true only when enabled
     AND path set.
2. **Writer/batcher wiring test**: with a separate metrics DB, a metric batch
   written via the metrics writer lands in the metrics file and is visible
   through `query.Open(metricsPath)`; the log file is untouched.
3. **Retention routing test**: `PurgeMetricDataPointsCommand` submitted via
   `MetricRetentionTask.WithSubmitter(metricWriter)` purges the metrics DB
   only; log retention still targets the log DB.
4. **Extend `TestE2E_OTLPMetricsToSQLite`** (or add a sibling) with
   `MetricsSQLitePath` set — assert two DB files exist, metrics rows in the
   metrics file, log rows in the log file.
5. **Load-test baselines** (`make loadtest-baseline-*` with
   `METRICS_SQLITE_PATH` added to the compose env): re-run log-only,
   metric-only and mixed and compare against `docs/loadtest-baseline.md` —
   the headline expectation is that **log-only throughput is unchanged or
   better** (no shared-writer contention from metrics) and mixed no longer
   drags logs down to ~4k/s per client.

## 9. Implementation order

1. Config field + env + YAML + validation + helper (with tests).
2. `MetricBatcher` constructor change to take `storage.CommandExecutor`;
   `sqlite.Writer.QueueDepth()`; update `cmd/collector/main.go` wiring
   (shared mode still compiles and passes `TestE2E_OTLPMetricsToSQLite`).
3. Second writer + queue in `initialize()`, start/cleanup ordering.
4. Per-task submitter override in maintenance + `WithSubmitter` on
   `MetricRetentionTask`; wire in `startMaintenance()`.
5. Tests (§8.1–8.4) and load-test baselines (§8.5).
6. Docs: `config.example.yaml`, `docs/architecture.md`, this plan → status
   "implemented".

## 10. Open questions

- Warning vs error when `metrics_sqlite_path` is set with
  `metrics_enabled: false`?
- Should the separate-DB mode also get per-signal tuning knobs now, or after
  the first load-test comparison?
- Do operators want the metrics DB's `VACUUM`/checkpoint scheduled too
  (two maintenance workers), or is WAL auto-checkpoint enough?

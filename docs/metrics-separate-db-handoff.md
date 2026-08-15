# Session prompt: implement the separate-metrics-DB feature

> Paste this file (or its content) into a fresh session. It is the complete
> brief for implementing `docs/metrics-separate-db-plan.md`. **Open questions
> at the end of the plan are explicitly out of scope — do not resolve them,
> do not implement them; leave them for a later session.**

## Task

Implement the "separate SQLite database for metrics" feature exactly as
specified in `docs/metrics-separate-db-plan.md` (read it first — it is the
source of truth for §3–§8 below). When `metrics_sqlite_path` is empty
(default) the behavior must be byte-for-byte identical to today: metrics
share the log DB and the single shared writer.

Scope: plan §4 (component changes), §5 (behavior matrix), §8.1–§8.4 (tests)
and the config/docs parts of §8.6. **Everything in plan §7 (decisions) and
§10 (open questions) is already decided or deferred — do not revisit it.**

## Worktree / environment

- Worktree: `/home/nnadolski/projects/otel-sqlite-metric-extension`
  (branch `metric-extension`).
- The repo already contains: the metrics extension (Phases 1–2), the
  nil-resource fix (mapper gives resource-less senders a deterministic ID),
  `-signal mixed` loadtest mode, `make loadtest-baseline-{logs,metrics,mixed}`,
  and the plan doc. Do not regress any of it.
- **Environment quirks (mandatory):**
  - Build/test/lint MUST use: `GOFLAGS=-buildvcs=false` and `-tags fts5`
    (the SQLite driver needs the fts5 tag or migration 002 fails with
    "no such module: fts5"; `go build` fails on VCS otherwise).
  - `internal/generated/` is gitignored; if missing, run `make generate`
    (protoc + protoc-gen-go are installed).
  - The pre-commit `tidy` hook fails environmentally → commit with
    `git commit --no-verify`.
  - Lint: `GOFLAGS=-buildvcs=false golangci-lint run --build-tags fts5 ./internal/... ./cmd/... ./scripts/...` — must stay at 0 issues.

## What to implement (in order)

### 1. Config surface (plan §3)

- `internal/config/config.go`: add `MetricsSQLitePath string` to `Config`;
  env binding `METRICS_SQLITE_PATH` → `parseString`; helper
  `func (c *Config) MetricsSeparateDB() bool { return c.MetricsEnabled && c.MetricsSQLitePath != "" }`.
- `internal/config/file.go`: add `MetricsSQLitePath` to `FileConfig`
  (`yaml:"metrics_sqlite_path"`), apply in `loadCoreConfig` when non-empty.
- **Also fix the pre-existing gap noted in the plan:** YAML
  `metrics_enabled` is currently ignored by `LoadFile` (only the env var
  works). Wire it so `metrics_sqlite_path` cannot be silently ignored.
- `config.example.yaml`: document `metrics_sqlite_path` next to
  `metrics_enabled`.
- Validation: if `metrics_sqlite_path` is set while `metrics_enabled` is
  false, log a startup **warning** (do not error).
- Tests in `internal/config/config_test.go`: YAML + env round-trip;
  `MetricsSeparateDB()` true only when enabled AND path set; disabled+path
  set → warning path covered.

### 2. MetricBatcher takes the writer handle (plan §4.1)

- Change `NewMetricBatcher(ingressQueue, cmdQueue storage.CommandQueue, …)`
  to take `executor storage.CommandExecutor` (the `*sqlite.Writer`).
- `flushLocked()` submits via `b.executor.Submit(context.Background(), cmd)`.
- Keep the batch-queue-depth gauge: add `func (w *Writer) QueueDepth() int`
  to `internal/storage/sqlite/writer.go` and have the batcher read depth
  through a small interface (e.g.
  `interface { storage.CommandExecutor; QueueDepth() int }`). Do NOT drop
  the gauge.
- Update the shared-mode wiring so the metric batcher receives `a.writer`
  (uniform code path). The log `Batcher` keeps taking the queue — do not
  change it.

### 3. Second writer + wiring (plan §4.3)

- `cmd/collector/main.go` `Application`: add `metricCmdQueue` +
  `metricWriter` fields (nil in shared mode).
- `initialize()`: when `cfg.MetricsSeparateDB()` create the second queue +
  `sqlite.NewWriter` (same `WriterBatchSize`/`WriterFlushInterval`/`WALMode`,
  shared `a.metrics`). Fail startup with a clear error if it cannot open.
  Wire `a.metricBatcher` to `a.metricWriter`.
- `start()`: metric batcher before metric writer.
- `cleanup()`: stop order — metric batcher → metric writer (Stop + Wait) →
  close metric command queue → then the existing log batcher → maintenance →
  log writer sequence.
- Both writers share the package-level `MaxTransactionRecords` — no change.

### 4. Maintenance routing (plan §4.4)

- `internal/maintenance/task.go`: add optional interface
  `type SubmitterTask interface { MaintenanceTask; Submitter() CommandSubmitter }`.
- `internal/maintenance/worker.go`: in `evaluateAndRun`, use
  `task.Submitter()` when the task implements `SubmitterTask` and returns
  non-nil; otherwise the worker's default submitter.
- `internal/maintenance/tasks/metric_retention.go`: add
  `WithSubmitter(s maintenance.CommandSubmitter)` and return it from
  `Submitter()`.
- `cmd/collector/main.go` `startMaintenance()`: call
  `MetricRetentionTask.WithSubmitter(a.metricWriter)` when
  `MetricsSeparateDB()`.
- Note: checkpoint/vacuum/optimize/fts-rebuild remain log-DB-only — this is
  a decided limitation (plan §7.1), do not change it.

### 5. Tests (plan §8.1–§8.4)

- Config tests (step 1).
- A wiring test: with a separate metrics DB, a metric batch written via the
  metrics writer lands in the metrics file and is visible through
  `query.Open(metricsPath)`; the log file is untouched.
- Retention routing: `PurgeMetricDataPointsCommand` submitted via
  `WithSubmitter(metricWriter)` purges the metrics DB only; log retention
  still targets the log DB.
- An e2e sibling of `TestE2E_OTLPMetricsToSQLite` with
  `MetricsSQLitePath` set: two DB files exist, metrics rows in the metrics
  file, log rows in the log file.

## Explicitly out of scope (defer for later — do NOT implement or resolve)

- Per-signal tuning knobs (`metrics_writer_batch_size`, …) — plan §10.
- Per-DB Prometheus labels (`db` label on counters) — plan §7.2 (decided:
  keep combined counters).
- Checkpoint/vacuum/optimize against the metrics DB — plan §7.1 (decided:
  log-DB-only, WAL auto-checkpoint covers the metrics DB).
- Metrics-only migration set — plan §7.3 (decided: full schema on both).
- `WriterConfig.MetricsOnly` statement-preparation optimization — plan §4.2
  (optional, not required).
- Load-test baselines with the new config — plan §8.5 (verification step,
  not part of this implementation; the loadtest harness is already in place).
- Unifying the log `Batcher` to the executor pattern — plan §4.1 (follow-up).
- Warning vs error for disabled-metrics + path (decided in step 1: warning).

## Definition of done

1. All of the above implemented with tests; `GOFLAGS=-buildvcs=false go test -tags fts5 ./...` green; golangci-lint 0 issues.
2. Default (shared) config path: no behavior change — the existing
   `TestE2E_OTLPMetricsToSQLite` and `TestE2E_NilResourceMetricsStayVisible`
   pass unchanged.
3. New tests cover: config round-trip, separate-DB wiring, retention
   routing, separate-DB e2e.
4. `config.example.yaml` and `docs/architecture.md` updated to mention
   `metrics_sqlite_path`.
5. Commit with `git commit --no-verify` (message style: `feat(config): …`).
6. Update the plan doc's status line to "implemented" and leave the open
   questions section untouched for the next session.

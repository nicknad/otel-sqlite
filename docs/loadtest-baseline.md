# Load Test & Throughput Baseline

This document records the throughput baseline for the OTLP SQLite collector and
describes how to reproduce it with the containerized load-test harness.

## Tooling

| Component | Path | Purpose |
|-----------|------|---------|
| Load generator | `cmd/loadtest/main.go` | Spins up N "mock APIs" (concurrent gRPC clients) that stream OTLP log export requests, counts acknowledged records/requests, and scrapes Prometheus `/metrics` to separate **ingest** vs **process** rates. |
| Containerized server | `docker-compose.loadtest.yml` | Runs only the collector (no Prometheus/Grafana) with throughput-oriented env tuning and an isolated data volume. Exposes gRPC on `14317` and metrics on `19090`. |
| Make targets | `Makefile` (`loadtest-*`, `loadtest`) | Build / up / run / down orchestration. |

> Ports `14317`/`19090` are used so the load test does not collide with a
> collector already running on the host's `4317`/`9090`.

### Quickstart

```bash
make loadtest-up                       # build + start collector container, wait for health
make loadtest-run                      # run cmd/loadtest with default flags (30s, 32 clients)
# or tune:
make loadtest-run LOADTEST_CLIENTS=64 LOADTEST_RECORDS=2000 LOADTEST_DURATION=60s
make loadtest-down                     # stop + remove container + volume
# one-shot:
make loadtest                          # up + run + down
```

### loadtest flags

```
-addr        collector gRPC address            (default localhost:14317)
-metrics     collector /metrics URL            (default http://localhost:19090/metrics)
-clients     number of mock-API clients        (default 32)
-records     log records per export request    (default 1000)
-duration    load test duration                (default 30s)
-rps-per-client  max requests/second per client; 0 = uncapped  (default 0)
-attrs       attributes per log record         (default 4)
-resources   distinct resources/services       (default 8)
```

## Measured Baseline

The numbers below are the pre-rewrite historical baseline. Keep them intact and
append new runs after the inline-attributes/native-driver work so the workload
and environment remain auditable.

Container config (env in `docker-compose.loadtest.yml`):
`INGRESS_QUEUE_CAPACITY=200000 BATCH_QUEUE_CAPACITY=20000 BATCH_SIZE=500 FLUSH_INTERVAL=1s`,
SQLite WAL, `synchronous=NORMAL`, single writer goroutine, 4 string attributes
per record (=> 1 `log_event` + 4 `log_attr` inserts per record + 1 dedup'd
`log_resource` insert per batch).

| Scenario | Ingest (ack) | Process (written) | Errors | Backlog |
|----------|-------------:|------------------:|-------:|---------|
| **Burst** (32 clients × 1000 rec, 30s, uncapped) | **277,889 rec/s** | ~0 at scrape (draining) | 0.30% | batch queue filled to ~16k batches |
| **Capped burst** (32 × 1000 × 2 req/s ≈ 64k rec/s, 60s) | 63,470 rec/s | 5,342 rec/s (commit rate) | 0% | grew continuously |
| **Sustained** (32 × 150 × 1 req/s ≈ 4.72k rec/s, 60s) | 4,720 rec/s | **4,717 rec/s** | 0% | **flat (~10 batches)** |

### Phase 0 capture for the rewrite

A fresh local container run was captured before changing the schema or driver:

- Command: `make loadtest-run LOADTEST_CLIENTS=32 LOADTEST_RECORDS=150 LOADTEST_DURATION=60s LOADTEST_RPS=1`
- Environment: Podman-backed Docker Compose, current Dockerfile, WAL,
  `synchronous=NORMAL`, batch size 500, four attributes per record.
- Client result: 283,200 records sent, 0 export errors, 4,720.20 records/sec.
- Final persisted result after stopping the collector: 283,200 events and
  1,132,800 attribute rows (four per event), 123,863,040 logical database
  bytes, 437.37 logical bytes/event.
- `dbstat` at capture: `log_event` 43,057,152 bytes, `log_attr` 45,527,040
  bytes, `idx_log_attr_event_id` 15,405,056 bytes, event timestamp/resource
  indexes 19,820,544 bytes combined. The collector was stopped before copying
  the database, so the reported database file excludes separate WAL/shm files.

This capture is the comparison point for the fixed-workload post-rewrite run.

### Post-rewrite capture

The same sustained command was run after migration 005, inline JSON writes, and
native `go-sqlite3`:

- Client result: 283,200 records sent, 0 export errors, 4,720.21 records/sec;
  all 283,200 records were persisted after shutdown.
- Final database: 87,691,264 logical bytes, 283,200 events, and no `log_attr`
  rows/table; 309.64 logical bytes/event.
- SQLite CLI `dbstat`: `log_event` 67,657,728 bytes, timestamp/resource
  indexes 19,988,480 bytes combined, and no attribute table/index.
- The Go density reporter reported `dbstat` unavailable for the bundled native
  build, so the post-rewrite object figures above were collected with the
  system `sqlite3` CLI. Page-level figures remain available from the Go tool.

Compared with the pre-rewrite 437.37 logical bytes/event, this workload is
1.41x denser overall (29.2% fewer logical database bytes), not the aspirational
2x target. The event table itself is larger because compact JSON replaces the
small scalar EAV row payload, but removing the EAV table and attribute index
reduces total storage substantially. Throughput was effectively unchanged at
this capped 4.72k records/sec workload; further performance claims require a
higher-rate burst/drain run.

### Driver microbenchmark

To isolate the driver on the inline-JSON schema, the same
`BenchmarkWriterInsert` benchmark was run at commit `f330adb` (modernc) and
again with the native driver. Each benchmark operation writes 100 events.

| Benchmark | modernc | native `go-sqlite3` | Native speedup |
|---|---:|---:|---:|
| IndividualInserts | 3.075 ms/op | 0.994 ms/op | 3.09x |
| BatchInsert | 3.587 ms/op | 1.203 ms/op | 2.98x |
| PreparedBatchInsert | 3.362 ms/op | 1.004 ms/op | 3.35x |
| FewerIndexes | 1.876 ms/op | 0.687 ms/op | 2.73x |

Commands used:

```bash
# At f330adb in a temporary worktree:
CGO_ENABLED=0 go test ./internal/storage/sqlite -run '^$' \\
  -bench 'BenchmarkWriter(Insert|WithFewerIndexes)$' -benchtime=3s -count=1

# Current tree:
CGO_ENABLED=1 go test -tags fts5 ./internal/storage/sqlite -run '^$' \\
  -bench 'BenchmarkWriter(Insert|WithFewerIndexes)$' -benchtime=3s -count=1
```

This demonstrates a driver-level improvement in the synthetic benchmark. The
capped 4.72k keep-up workload does **not** bound production throughput.

### Interpretation (corrected)

- **Ingest intake ceiling** is high: the gRPC server + bounded ingress queue
  absorb **≥278k records/sec** in bursts; the backpressure design lets the
  pipeline hold millions of records in queues.
- **The 4.72k figure is a client rate cap, not a process ceiling.** The
  sustained profile uses `32 × 150 × 1 req/s ≈ 4.8k rec/s` client budget.
  Matching 4.72k before and after the rewrite only proves keep-up under that
  budget. Historical uncapped runs already wrote ~6–9k rec/s on modernc;
  native-driver writer benches on this host reach tens of thousands of rec/s.
- **Real process ceiling** must be read from `logs_written_total` under an
  uncapped profile (see Measurement Matrix below), never from a capped run.

### Config bug found during throughput investigation

`docker-compose.loadtest.yml` historically set `BATCH_SIZE=500` /
`FLUSH_INTERVAL=1s`, but the collector only applied those legacy vars when the
corresponding config fields were still zero. `DefaultConfig()` always sets
non-zero defaults, so the compose values were silently ignored and the
loadtest ran with `BatcherBatchSize=250` and `WriterBatchSize=100`.

Fixed: `config.ApplyLegacyEnv` keys off whether the **specific env var was
present**, and the loadtest compose now uses explicit
`BATCHER_BATCH_SIZE` / `WRITER_BATCH_SIZE` / flush / `WRITER_MAX_TRANSACTION_RECORDS`
names. Startup logs print the effective pipeline sizes.

## Measurement Matrix

Use these three profiles. Process rate always comes from Prometheus
`otel_collector_storage_logs_written_total` deltas, not client send rate.

| Profile | Make target | Purpose |
|---|---|---|
| **A. Keep-up (capped)** | `make loadtest-keepup` | Regression: no backlog at ~4.72k rec/s |
| **B. Process ceiling** | `make loadtest-process` | Max written rec/s while queues are fed |
| **C. Burst + drain** | `make loadtest-burst-drain` | Intake ceiling + time-to-drain after clients stop |

```bash
make loadtest-up
make loadtest-keepup
make loadtest-process
make loadtest-burst-drain
make loadtest-down
```

`cmd/loadtest` accepts `-drain-timeout` (default 2m). After clients stop it
prints:

- **Process rate (client window)** = written-during-client-window / client duration
- **Drain after clients stopped** = time until `written >= received` (or timeout)

### Host writer microbenchmarks (native driver, post-rewrite)

Recorded on AMD Ryzen 5 4500U during the throughput investigation. Re-run after
each perf change:

```bash
CGO_ENABLED=1 go test -tags fts5 ./internal/storage/sqlite -run '^$' \
  -bench 'BenchmarkWriter_' -benchtime=3s -count=3
```

| Benchmark | Approx rec/s (single sample) |
|---|---:|
| `BenchmarkWriter_OptimizedPragmas` (fresh DB, 250-rec tx, 5 attrs) | ~45k |
| `BenchmarkWriter_LargeDB_Optimized` (after 500k prefill) | ~19–20k |
| `BenchmarkWriter_E2E_Pipeline` (queue + writer, fixed drain) | ~100k+ short-run |

### Post-fix uncapped baseline (Phase 0 of throughput plan)

Captured 2026-08-07 on AMD Ryzen 5 4500U after the legacy-env fix and explicit
loadtest compose sizes (`batcher=500`, `writer_commands=50`,
`max_tx_records=10000`). Startup log confirmed:
`pipeline config: ... batcher_batch_size=500 ... writer_batch_size=50 ...`.

| Profile | Process rec/s (client window) | Ingest rec/s | Export errors | Drain | Notes |
|---|---:|---:|---:|---:|---|
| A keep-up | kept up (final written = 283200) | 4,720.19 | 0% | 507ms complete | flat; not a ceiling |
| **B process** | **58,298.67** | 257,210.81 | 0.06% | 1m7.9s complete | **baseline to beat** |
| C burst+drain | (same shape as B at higher fan-in; re-run when comparing) | ≥278k historical | — | — | use `make loadtest-burst-drain` |

Profile B detail (`make loadtest-process`):

- 8 clients × 250 records/req, 30s, uncapped
- Client sent 7,716,500 records; collector received/wrote 7,717,250
- Written during client window: 1,749,000 → **58,298.67 rec/s process**
- Full drain after clients stopped: 67.9s (complete within 2m timeout)
- `write_errors_total` delta 0 (one pre-existing counter from startup checkpoint race)

This replaces any reading of ~4.7k as the process ceiling. Phase 2+ changes
must beat **~58k written rec/s** on the same profile B command, or explain a
regression with profiling.

## Write-path hot spots (Phase 1 profile)

Captured with:

```bash
CGO_ENABLED=1 go test -tags fts5 ./internal/storage/sqlite -run '^$' \
  -bench 'BenchmarkWriter_OptimizedPragmas$' -benchtime=3s -count=1 \
  -cpuprofile=/tmp/writer_cpu.prof -memprofile=/tmp/writer_mem.prof
go tool pprof -top -cum /tmp/writer_cpu.prof
go tool pprof -top -alloc_space /tmp/writer_mem.prof
```

Host: AMD Ryzen 5 4500U, ~43.8k rec/s on that bench sample.

### CPU (cumulative share of bench time)

| Area | Approx cum % | Notes |
|---|---:|---|
| `WriteBatchCommand.Execute` / `insertEventRecord` | ~66% | whole per-row path |
| `database/sql` `Stmt.ExecContext` → go-sqlite3 `exec` / `sqlite3_step` | ~45% / ~24% step | dominant; CGO boundary `runtime.cgocall` ~40% flat |
| `marshalEventAttrs` → `encoding/json.Marshal` | ~19% / ~14% | map encode + key sort |
| go-sqlite3 bind path | ~13% | per-column bind |
| `Tx.Commit` | ~9% | WAL commit |

### Allocations (alloc_space)

| Area | Share | Notes |
|---|---:|---|
| `marshalEventAttrs` | ~50% | map + JSON buffer + `string([]byte)` |
| `database/sql.driverArgsConnLocked` | ~32% | per-Exec args slice |
| `encoding/json.Marshal` / map encode | ~25% overlapping with marshal | reflection + key sort |
| `fmt.Sprintf` in bench setup | ~6% | bench-only body/attr formatting |

### Statement mix (hook proof)

`TestWriteBatchInsertMix` registers go-sqlite3 `RegisterUpdateHook` and asserts:

- 1 `log_resource` insert on first batch, 0 on repeat resource (`INSERT OR IGNORE`)
- N `log_event` inserts for N records
- 0 `log_attr` (or other) inserts

### Phase 2 priority implied by this profile

1. Keep SQLite work per row down (tx sizing, multi-row insert, optional FK-off,
   resource ID cache) — largest CPU bucket is still `sqlite3_step` + bind.
2. Replace `map`+`encoding/json` marshal — ~19% CPU and ~50% allocs, fully under
   our control.
3. Reduce `database/sql` per-Exec overhead (multi-row VALUES, fewer Exec calls).

## Optimizations Identified and Applied

1. **Observability was broken** — the writer/batcher never updated Prometheus
   counters, so `logs_written_total`, `write_latency_seconds`, queue-depth
   gauges, `batches_*_total` were all stuck at 0, making a process baseline
   impossible to measure.
   - Fix: wired `*metrics.Metrics` (nil-safe) through `WriterConfig` /
     `BatcherConfig`; writer now reports written records, batches, write
     latency, write errors, and batch-queue depth; batcher reports batches
     created/observed and ingress/batch queue depths.
2. **Resources were never deduplicated** — `Mapper.mapResource` returned a
   `Resource` with `ID == ""`, so `Writer.ensureResource` generated a *new*
   `res-<unixnano>` per batch and `INSERT OR IGNORE` never matched. Every
   batch inserted a fresh `log_resource` row.
   - Fix: derive a deterministic `Resource.ID` from `sha256(service.name + host.name + schema_url)`,
     so identical resources collapse to a single row.
3. **Dockerfile** would not build under podman/OCI (`EXPOSE ... # comment`
   parsing) and pinned `protoc-gen-go-grpc@latest` which needs Go 1.25 while
   the base was Go 1.24; also the data volume was root-owned so the non-root
   `otel` user could not create the SQLite file.
   - Fix: bumped builder to `golang:1.25-alpine`, pinned
     `protoc-gen-go@v1.36.11` / `protoc-gen-go-grpc@v1.5.1`, removed inline
     `EXPOSE` comments, and chowned the data dir to the `otel` user.

## Further Optimization Opportunities

Tracked in `plan.md` (throughput plan). Summary:

- Measure uncapped process rate (profile B) before claiming wins.
- Raise batcher/writer transaction sizing now that EAV is gone.
- Cut per-record JSON marshal allocs (`marshalEventAttrs`).
- Optional: writer-side resource ID cache, FK-off on writer connection,
  multi-row inserts, WAL autocheckpoint tuning.
- Single writer remains the architectural ceiling; sharding is a later plan.

## Reproduce

```bash
# Density after stop + checkpoint:
CGO_ENABLED=1 go run -tags fts5 ./scripts/db_density_report ./path/to/otel-logs.db

# Full matrix:
make loadtest-up
make loadtest-keepup
make loadtest-process
make loadtest-burst-drain
make loadtest-down

# Host writer benches:
CGO_ENABLED=1 go test -tags fts5 ./internal/storage/sqlite -run '^$' \
  -bench 'BenchmarkWriter_' -benchtime=3s -count=1
```
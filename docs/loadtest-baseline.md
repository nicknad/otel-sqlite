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

### Interpretation

- **Ingest intake ceiling** is high: the gRPC server + bounded ingress queue
  absorb **≥278k records/sec** in bursts; the backpressure design lets the
  pipeline hold millions of records in queues.
- **SQLite persistence is the hard ceiling at ~4,700 records/sec** sustained.
  Once the ingest rate exceeds this, the batch queue grows without bound and
  writes fall behind (the burst committed ~250k rows per transaction at
  **~16–22s commit latency**).
- Sustained (rows in ≈ rows out, flat queue) is achievable only at rates
  ≤ ~4.7k rec/s under the current schema/PRAGMA settings.

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

## Further Optimization Opportunities (not implemented)

- **Process ceiling (~4.7k rec/s) is the real bottleneck**, dominated by
  SQLite: 5 inserts/record plus the `log_attr(key)` and `log_event(body)`
  indexes. Levers (each trades durability/space for speed):
  - `PRAGMA synchronous=OFF` (or `EXTRA`) and `temp_store=MEMORY`;
  - defer/drop `log_event(body)` and `log_attr(key)` indexes during ingest,
    build post-load;
  - reduce per-record insert count (e.g. JSON-encode attrs into a single column
    instead of one row per attribute).
- **Batcher has no time-based flush.** It flushes only when a batch reaches
  `BatchSize` records; `Config.FlushInterval` is used only by the writer's
  ticker. A low-rate producer therefore experiences unbounded end-to-end latency
  until the batcher is stopped. Add a flush ticker to the batcher tied to
  `FlushInterval`.
- **Single writer goroutine** serializes all persistence; SQLite WAL allows a
  separate reader, but writes are inherently single-threaded for this driver.
  Sharding by resource/tenant across multiple databases would scale writes
  horizontally if persistence-latency requirements exceed ~4.7k rec/s.

## Reproduce

```bash
# Sustained process baseline (flat queue):
make loadtest-up
make loadtest-run LOADTEST_CLIENTS=32 LOADTEST_RECORDS=150 \
                   LOADTEST_DURATION=60s LOADTEST_RPS=1
make loadtest-down

# Burst intake ceiling:
make loadtest-run LOADTEST_CLIENTS=32 LOADTEST_RECORDS=1000 \
                   LOADTEST_DURATION=30s LOADTEST_RPS=0
```
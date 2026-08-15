# Load-test baseline

This document records host-specific measurements and the reproducible load-test
profiles. Throughput is not a production capacity guarantee.

## Run it

Requirements: Docker Compose, `curl`, Go, CGO toolchain, and a working Docker
runtime.

```bash
make loadtest-up
make loadtest-keepup       # capped regression profile
make loadtest-process      # uncapped writer profile
make loadtest-burst-drain  # burst intake and drain
make loadtest-down
```

Or run the default client directly:

```bash
make loadtest-run
```

The load-test collector uses host ports `14317` (gRPC) and `19090` (metrics),
an isolated volume, and explicit pipeline settings:

```text
ingress=200000  command_queue=20000  batcher=500  writer_commands=50
batcher_flush=1s  writer_flush=1s  max_transaction_records=10000
```

Override Make variables, for example:

```bash
make loadtest-run LOADTEST_CLIENTS=64 LOADTEST_RECORDS=2000 \
  LOADTEST_DURATION=60s LOADTEST_RPS=2
```

The client flags are `-addr`, `-metrics`, `-clients`, `-records`, `-duration`,
`-rps-per-client`, `-attrs`, `-resources`, and `-drain-timeout`. Defaults are
shown by `go run ./cmd/loadtest -h`.

## Profiles

| Profile | Make target | Purpose |
|---|---|---|
| Keep-up | `make loadtest-keepup` | Capped rate; checks that the queue stays drained |
| Process | `make loadtest-process` | Estimates the writer rate while clients feed it |
| Burst/drain | `make loadtest-burst-drain` | Measures intake, backlog, and drain time |

Use `otel_collector_storage_logs_written_total` for process rate. Client
acknowledgements measure intake, not persistence.

## Recorded results

These samples were captured on an AMD Ryzen 5 4500U and are historical. Repeat
the profiles after code or hardware changes.

| Profile | Process rate | Ingest rate | Result |
|---|---:|---:|---|
| Keep-up, 32 clients × 150 records, 1 request/s, 60s | 4,720/s | 4,720/s | drained; no export errors |
| Process, 8 clients × 250 records, uncapped, 30s (Phase 0) | 58,299/s | 257,211/s | drained in 1m 8s; 0.06% export errors |
| Process, same workload (Phase 2) | 70,258/s | 250,402/s | 0.03% export errors |
| Historical burst, 32 clients × 1,000 records, uncapped, 30s | — | 277,889/s | backlog remained to drain |

## Metric-extension baselines (added with the metrics feature)

The metric feature added a second ingestion path (OTLP metrics → same command
queue → same single-writer SQLite connection) and changed migration 005-era
numbers only through contention: metric data points are wider (15 columns +
JSON payloads) and ~3× more expensive per insert than log events, so mixed
load splits the writer's bandwidth.

Workload shape for all three rows below: 8 clients × 250 records-or-points per
request, uncapped, 30s (`loadtest-baseline-{logs,metrics,mixed}`). The logs row
is the Phase-2 profile re-measured on the same machine (AMD Ryzen 5 4500U)
after the metrics feature landed — it confirms no log-ingress regression.

| Signal | Make target | Ingest rate | Process rate (client window) | Drain | Export errors |
|---|---|---:|---:|---|---:|
| Logs (run 1) | `make loadtest-baseline-logs` | 263,875/s | 69,431/s | complete, 57s | 0.16% |
| Logs (run 2) | `make loadtest-baseline-logs` | 262,317/s | 69,746/s | complete, 59s | 0.03% |
| Metrics | `make loadtest-baseline-metrics` | 198,954/s | 23,584/s | complete, 2m52s | 0.07% |
| Mixed (4 log + 4 metric) | `make loadtest-baseline-mixed` | 219,957/s | logs 16,068/s + metrics 14,402/s = 30,470/s combined | incomplete at 3m (≈250 points, one request, still draining; 0.008% gap) | 0.03% |

Interpretation:

- **Logs did not regress.** 69,431–69,746/s vs the 70,258/s Phase-2 baseline is
  −1%, run-to-run noise. Drain is faster (57–59s vs 1m8s).
- **Metrics ceiling is ~23.6k/s** for this shape — the writer, not ingestion,
  is the bottleneck (ingest accepts ~199k/s and queues the rest).
- **Mixed throughput is sub-additive**: 30.5k/s combined vs 69.4k/s log-only,
  because metric inserts dominate the shared writer. Per-client log rate drops
  from ~8.7k/s (solo) to ~4.0k/s next to metrics.
- Drain completeness: give metrics/mixed runs a ≥3m drain timeout; the metric
  backlog drains at the same slow writer rate.

The keep-up result is a capped regression check, not a process ceiling. The
process profile is also workload- and hardware-dependent.

## Separate-DB verification (`metrics_sqlite_path`)

The separate-metrics-DB feature (`metrics_sqlite_path` set, `metrics_enabled:
true`) gives the metrics signal its own SQLite database with its own writer +
batcher pipeline. This section re-measures the same three profiles against
that config on the same machine (AMD Ryzen 5 4500U) and compares with the
shared-writer rows above.

Reproduce with the compose override:

```bash
make loadtest-separate-up          # adds METRICS_SQLITE_PATH to the stack
make loadtest-baseline-logs
make loadtest-baseline-metrics
make loadtest-baseline-mixed
make loadtest-separate-down
# or all at once: make loadtest-separate-baselines
```

The container then holds two database files (`otel-logs.db` and
`otel-metrics.db`) and logs
`metrics pipeline: separate SQLite database enabled at ...`.

| Signal | Shared writer (above) | Separate DB | Delta |
|---|---:|---:|---:|
| Logs solo | 69,431 / 69,746 /s | 71,147 /s | +2% (no regression) |
| Metrics solo | 23,584 /s | 23,567 /s | ≈0 (same per-writer ceiling) |
| Mixed: logs | 16,068 /s (~4.0k/s per client) | 53,890 /s (~13.5k/s per client) | +235% |
| Mixed: metrics | 14,402 /s | 19,861 /s | +38% |
| Mixed: combined | 30,470 /s | 73,751 /s | +142% |
| Mixed drain | incomplete at 3m | complete, 59s | — |

Ingest rates and error rates are unchanged (263k/s and 198k/s solo ingest;
0.02–0.03% export errors in all runs; zero data loss — written == received
for every signal). The headline result is the mixed run: log clients are no
longer starved by metric inserts on the shared writer. Per-client log rate
recovers from ~4.0k/s (shared) to ~13.5k/s (separate) — actually above the
solo per-client rate (8.9k/s) because only 4 log clients feed the log writer
in the mixed shape. The mixed backlog drains completely in ~59s instead of
remaining at the 3m timeout.

Interpretation:

- **Log-only is unchanged or slightly better** (+2%, within noise): a
  dedicated metrics writer cannot regress the log path.
- **Metric-only is unchanged** (~23.6k/s): that is the per-writer ceiling
  for this shape; the separate DB does not make a single writer faster.
- **Mixed is no longer sub-additive per signal**: each writer runs near its
  own ceiling (53.9k/s logs vs 71k/s solo, 19.9k/s metrics vs 23.6k/s solo)
  instead of sharing one ~30k/s budget. Combined 73.8k/s vs 30.5k/s shared.

Limitations (unchanged from the shared-DB runs): per-signal tuning knobs are
not yet configurable, Prometheus counters remain combined across DBs, and
checkpoint/vacuum/optimize/FTS-rebuild still target the log DB only.

## Storage note

Migration 005 stores event attributes in `log_event.attributes_json` and removes
the legacy `log_attr` table. Older baseline measurements that count
`log_attr` inserts are retained only as historical comparisons; they do not
describe the current write path.

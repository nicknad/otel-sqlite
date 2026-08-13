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

The keep-up result is a capped regression check, not a process ceiling. The
process profile is also workload- and hardware-dependent.

## Storage note

Migration 005 stores event attributes in `log_event.attributes_json` and removes
the legacy `log_attr` table. Older baseline measurements that count
`log_attr` inserts are retained only as historical comparisons; they do not
describe the current write path.

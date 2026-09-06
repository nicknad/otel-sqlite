# OTLP SQLite Collector

A small OpenTelemetry log collector that receives OTLP over gRPC and stores
logs in SQLite.

Pipeline: `gRPC ingress → bounded ingest queue (all-or-nothing admission) →
insert batcher → bounded command queue → single SQLite writer (WAL)`.

- Benchmarks & performance docs: [docs/performance.md](docs/performance.md)
  (`cargo run --release -p otel-sqlite-e2e -- --scenario baseline`)
- Architecture, design contract & invariants: [docs/architecture.md](docs/architecture.md)
- Open work & roadmap: [TODO.md](TODO.md)
- Local CI (run `.forgejo/workflows/ci.yml` on your machine with `act`):
  [local-ci/](local-ci/) (`.\local-ci\run.ps1` or `./local-ci/run.sh`)

## Project scope

Active development targets the **log** pipeline end to end — ingest, storage,
durability and performance work all focus on logs first.

Metrics are a **future extension**: OTLP metrics are accepted and persisted
today, but they are not an active workstream and receive no dedicated feature
or optimization effort. Storage and performance optimizations are deliberately
shaped so the metric write path stays untouched, leaving it free to evolve
when metrics work resumes (see `docs/performance.md`, PERF-006/007).

The OTLP **traces** signal is **unsupported and out of scope**: no
`TraceService` is served on the gRPC endpoint, and `ResourceSpans` payloads
are not handled. A call to the trace export RPC returns `UNIMPLEMENTED`; send
operational telemetry through the log pipeline instead.

## Mapping fidelity

OTLP values are preserved into SQLite with no silent conversion:

- **Structured log bodies** (`ArrayValue` / `KvlistValue`) and **nested
  attribute values** are persisted as canonical JSON in the existing `body` /
  `attributes_json` columns: arrays become JSON arrays, kvlists become
  sorted-key JSON objects (duplicate keys collapse to the last value), byte
  arrays are hex-encoded, and non-finite doubles become `null`.
- **Exemplars** on gauge/sum, histogram and exponential-histogram points are
  persisted into `metric_data_point.exemplars_json` (sorted-key objects,
  hex-encoded trace/span ids).
- **Scope attributes and schema URLs** are persisted for both signals
  (denormalized onto `log_event` for logs, on the shared `scope` table for
  metrics), and the group-level schema URL is recorded on the resource.
- **Metric metadata** is persisted on the `metric` table.

Malformed trace/span ids — wrong length, or present-but-all-zeroes — are
**rejected** with `INVALID_ARGUMENT` for the whole request (all-or-nothing),
never silently zero-filled.

### Intentional, documented limitations

Every remaining conversion is either rejected visibly or counted as an
explicit loss in the `otlp_mapping_loss_total` metric (`reason` label), never
silently turned into an unrelated value:

- Unknown `SeverityNumber` / `aggregation_temporality` enum values are counted
  (`reason="unknown_severity"` / `reason="unknown_temporality"`) and mapped to
  the `UNSPECIFIED` value.
- The Profiling-only `AnyValue.string_value_strindex` / `KeyValue.key_strindex`
  fields carry no non-Profiling semantic content per the OTLP proto; they are
  treated as absent (`null`).
- Bytes **log bodies** are rendered as lossy UTF-8 text (the pre-existing
  behavior; attribute and exemplar byte values are hex-encoded instead).
- The OTLP **traces** signal is out of scope (`ResourceSpans` is not handled).
  `SummaryDataPoint` carries no exemplars in the pinned OTLP proto, so summary
  points persist no exemplar column data.

## Architecture

The pipeline follows a strict **single-writer** design:

```text
OTLP/gRPC
   │
   ▼
Ingress ────────────┐
                    │  Command Queue  ────▶ SQLite Writer ────▶ SQLite
Maintenance Worker ─┘  (bounded mpsc)
```

Exactly one component — the SQLite writer (`otel-sqlite-storage`) — owns and
mutates the SQLite connection. Everything else, including the maintenance
worker (`otel-sqlite-runtime`), is a **producer** of the same bounded command
queue.

### Maintenance worker

The maintenance worker runs on its own thread and owns scheduling only:

- periodically evaluates which maintenance operations are due (retention
  purge, WAL checkpoint, `ANALYZE`, `VACUUM`, optional search-index rebuild);
- enqueues them as ordinary write commands onto the existing command queue;
- contains no SQL and holds no database connection — every statement lives in
  the storage writer.

Frequencies are explicit in `MaintenanceConfig` and conservative by default:
purge every 15 min, checkpoint every 5 min, `ANALYZE` twice a day, `VACUUM`
once a day; retention is disabled unless configured. Retention windows are
per signal — one uniform window covers logs _and_ metrics:

```toml
[maintenance]
retention = "7d"            # uniform window for both signals

# …or per-signal windows; keys left out keep their defaults:
[maintenance.retention]
logs = "7d"
metrics = "24h"
```

A purge deletes expired log events and metric data points in one transaction
and garbage-collects the dimension rows the deletions orphan (series,
metric definitions, scopes, resources) — so nothing grows unboundedly once
retention is configured. The full-text search index is maintained
incrementally by triggers, so pruned events leave no stale search hits.
`rebuild_fts_interval` schedules a full index rebuild as recovery for a
corrupted index; it re-reads every log row and is off by default.

### Durability & acknowledgement policy

By default (`durability.mode = "commit"`), an OTLP export response is only
sent **after** the SQLite writer has committed every record the request had
accepted. Each accepted chunk carries a commit ticket; the writer publishes a
contiguous watermark after each transaction and handlers wait on it — so a
2xx means the data survives an application crash. The cost is bounded ack
latency: responses wait out batching plus queue drain instead of returning
from memory.

```toml
[durability]
mode = "commit"        # "commit" (default) | "enqueue" (ack at ingest-queue entry)
synchronous = "normal" # SQLite synchronous pragma: "normal" | "full"
```

- `"enqueue"` acknowledges as soon as records enter the bounded ingest queue:
  lowest latency, but acknowledged records may be lost to a crash.
- `synchronous = "full"` additionally fsyncs WAL on every commit, extending
  the durability guarantee from application crashes to power failures.
- If the writer or batcher dies before a ticket commits, pending requests
  fail immediately with `UNAVAILABLE` (clients retry) instead of hanging.

Backpressure is **all-or-nothing**. The ingress sender wraps the bounded
ingest queue in an **admission guard**: it reserves room for an entire
request's chunks before any of them are enqueued, and every other producer
(including control messages like Flush barriers) must pass the same gate, so
no two requests can interleave. If the queue cannot fit the whole request, the
server returns `UNAVAILABLE` and _nothing_ is accepted — a retrying exporter
resends a request that was never partially applied, so duplicate telemetry is
impossible even though log-event and data-point rows have no idempotency key.
Rejected requests issue no commit tickets and never stall later commits.

### Observability

- **Prometheus `/metrics`** is served on `metrics_address`
  (default `127.0.0.1:8888`; set `"off"` in the TOML to disable). It exposes
  OTLP request/record counters and histograms, ingest/storage queue depths,
  batcher buffer occupancy, transaction durations, writer errors, drop
  counters, and the watchdog's `sidecar_health_state`.
  **Exposure stance:** the endpoint is plain HTTP by design — bind it to
  localhost (the default) and terminate TLS/auth on a reverse proxy if you
  must scrape remotely. Do not point `metrics_address` at a routable
  interface without such a front; it carries no authentication of its own
  (`[tls]` applies only to the OTLP listener).
- **gRPC health** (`grpc.health.v1.Check`/`Watch`) runs on the same port as
  OTLP. The overall status (empty service name) plus both OTLP services track
  pipeline evidence sampled every second: anything other than a running
  writer _and_ batcher reports `NOT_SERVING`, and shutdown flips to
  `NOT_SERVING` before draining so load balancers stop routing.
- **Startup readiness**: `Storage::open` blocks until the database is open and
  the schema migrated; a broken storage backend fails process startup instead
  of surfacing on first traffic.
- **Failure policy** (`storage` writer): SQLite errors are classified per
  batch — _transient_ (`BUSY`/`LOCKED`) retries up to 4× with exponential
  backoff; _poisonous_ data errors (constraint violations, row-local binding
  failures) trigger a salvage pass that commits healthy rows and
  drops-and-counts only the offending ones (`quarantined_records`,
  warn-logged per row); anything unknown or environmental (disk full, IO,
  corruption, internal misuse) is fatal: the writer halts, closes the
  durability ledger (pending acks fail with `UNAVAILABLE`) and hands over to
  the watchdog/supervisor.
  **Alerting guidance:** page on any increase of
  `storage_quarantined_records_total` (dropped rows never come back) and on
  `storage_errors_total` growth — both indicate senders producing malformed
  batches or a degrading disk.

```toml
listen_address = "0.0.0.0:4317"
metrics_address = "127.0.0.1:8888"   # "off" disables /metrics
max_records_per_request = 100000
# allow_remote_metrics = true         # explicit opt-in; prefer a protected proxy
```

Configuration is validated before any listener or worker starts. Queue sizes,
message and record limits, stream counts, durations, addresses, security files,
and watchdog thresholds are bounded and checked. The effective configuration is
printed as JSON at startup. Remote Prometheus exposure is rejected unless
`allow_remote_metrics = true` is explicitly configured.

### Running with Docker

The image runs as non-root user `otel-sqlite` with `WORKDIR /data`, so the
default relative database path lands inside the declared `/data` volume —
mount a volume or the data lives on the ephemeral container layer:

```sh
docker run -d -v otel-sqlite-data:/data -p 4317:4317 otel-sqlite
```

The image ships a `HEALTHCHECK` backed by `otel-sqlite healthcheck`, which
runs `grpc.health.v1.Check` against localhost and exits non-zero unless the
pipeline reports `SERVING`. Override the probed endpoint with
`OTEL_SQLITE_HEALTHCHECK_ENDPOINT` when the port differs.

### Backups, restore and upgrades

Online backups use the SQLite Online Backup API via the built-in subcommands —
no external `sqlite3` is needed (the slim image ships none) and ingestion is
never paused:

```sh
otel-sqlite backup   --db otel-logs.db --out /backups --keep 30
otel-sqlite backup   --db otel-logs.db --out /backups --key-file /keys/backup.key
otel-sqlite restore  --backup /backups/otel-logs.db.<UTC>.otsb --dir /data/restored --key-file /keys/backup.key
otel-sqlite verify   --db otel-logs.db
```

`backup` opens the live database read-only, copies a consistent snapshot
through the WAL, normalizes it to a single self-contained file, verifies
integrity/foreign-keys/row-counts, and prunes beyond `--keep`. `restore`
refuses a non-empty directory and verifies the result before and after the
copy. `verify` exits non-zero on corruption — the hook cron/systemd/Kubernetes
alert on. Encryption is optional AES-256-GCM (32-byte key file).

- Full runbook (WAL/sidecar handling, restore, integrity, encryption and
  access control, disk capacity, retention, alerting):
  [docs/operations/backup-restore.md](docs/operations/backup-restore.md)
- Step-by-step setup guides: **systemd**
  ([docs/operations/backup-systemd.md](docs/operations/backup-systemd.md)) and
  **Kubernetes**
  ([docs/operations/backup-kubernetes.md](docs/operations/backup-kubernetes.md))
- Schema migration, incompatible changes and rollback limits (upgrades are
  forward-only; rollback is restore-from-backup):
  [docs/operations/upgrades.md](docs/operations/upgrades.md)
- Deployment examples (systemd units + timer; Kubernetes StatefulSet + backup
  CronJob): [deploy/systemd/](deploy/systemd/), [deploy/kubernetes/](deploy/kubernetes/)

### Security: TLS/mTLS and bearer tokens

Everything is in-house — no external certificate authority is involved.
Bootstrap a private PKI with one command:

```sh
otel-sqlite gen-certs --host otel.internal --client collector-a --out ./certs
```

This writes `ca.pem/ca.key` (your private root CA — move `ca.key` offline),
a server identity (SANs cover your hosts plus `localhost`/`127.0.0.1`/`::1`),
and one client pair per `--client`. Wire it up:

```toml
[tls]
cert = "./certs/server.pem"
key = "./certs/server.key"
client_ca = "./certs/ca.pem"   # presence enables mandatory mTLS

[auth]
mode = "token"
token_file = "/etc/otel-sqlite/clients.txt"   # one token per line, all valid
```

- **mTLS**: clients must present a certificate signed by `client_ca`.
  Rotation = reissue a client cert from the CA (`gen-certs --client x`
  again); the CA itself stays stable for years.
- **Tokens** travel as `authorization: Bearer <token>` headers, are stored
  hashed (constant-time compare) and reloaded from disk every few seconds —
  rotate with zero downtime by appending a new line to `clients.txt`,
  migrating senders, then removing the old line. Deleting the file revokes
  everything immediately.
- The gRPC health service deliberately stays unauthenticated so probes and
  load balancers work; under mTLS the built-in healthcheck authenticates
  with the server's own identity (its certificate carries both serverAuth
  and clientAuth).
- Binding a non-loopback address without `[tls]` prints a loud warning at
  startup; `mode = "token"` without a readable token file refuses to start.

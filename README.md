# OTLP SQLite Sink

Small OpenTelemetry log sink: receives OTLP over gRPC, stores logs in
SQLite. Terminates the pipeline — nothing is forwarded.

Pipeline: `gRPC ingress → bounded ingest queue (all-or-nothing admission) →
insert batcher → bounded command queue → single SQLite writer (WAL)`.

- Benchmarks: [docs/performance.md](docs/performance.md)
  (`cargo run --release -p otel-sqlite-e2e -- --scenario baseline`)
- Architecture & invariants: [docs/architecture.md](docs/architecture.md)
- Local CI (run `.github/workflows/ci.yml` with `act`):
  [tools/local-ci/](tools/local-ci/)

## Project scope

**Logs** are the supported end-to-end pipeline (ingest, storage,
durability, performance).

- **Metrics: unsupported since migration 005.** Service removed from the
  gRPC endpoint; all metric tables/views/indexes dropped. Exports return
  `UNIMPLEMENTED`.
- **Traces: unsupported and out of scope.** No `TraceService` served;
  `ResourceSpans` not handled. Calls return `UNIMPLEMENTED`; send
  operational telemetry as logs.

## Mapping fidelity

OTLP values persist with no silent conversion:

- Structured bodies (`ArrayValue` / `KvlistValue`) and nested attributes
  persist as canonical JSON in `body` / `attributes_json`: arrays stay
  arrays, kvlists become sorted-key objects (duplicate keys collapse to
  last), bytes hex-encode, non-finite doubles become `null`.
- Scope attributes and schema URLs persist (denormalized onto `log_event`);
  group schema URL persists on the resource.
- Malformed trace/span ids (wrong length or all-zeroes) map to **no trace
  context** and count under `otlp_mapping_loss_total`
  (`invalid_trace_id` / `invalid_span_id`); per OTLP `SHOULD`, the request
  is accepted, not rejected.

Intentional limitations (all rejected visibly or counted in
`otlp_mapping_loss_total`, never silently converted):

- Unknown `SeverityNumber` → counted (`unknown_severity`), mapped to
  `UNSPECIFIED`.
- Profiling-only `AnyValue.string_value_strindex` / `KeyValue.key_strindex`
  carry no non-Profiling content per proto — treated as absent (`null`).
- Bytes log bodies render as lossy UTF-8 text (attribute bytes hex-encode).
- Traces and metrics signals are out of scope.

## Architecture

Strict **single-writer**:

```text
OTLP/gRPC ──▶ Ingress ──▶ ingest queue ──▶ insert batcher ─┐
                                                           ├─▶ command queue ──▶ SQLite Writer ──▶ SQLite
Maintenance Worker ────────────────────────────────────────┘
```

Only the SQLite writer (`otel-sqlite-storage`) owns the SQLite connection.
The insert batcher (same crate) and maintenance worker
(`otel-sqlite-runtime`) are **producers** on the shared bounded command
queue; ingress reaches the batcher via the bounded ingest queue.

### Maintenance worker

Own thread, scheduling only: evaluates due operations (retention purge,
WAL checkpoint, `ANALYZE`, `VACUUM`, optional FTS rebuild) and enqueues
them as ordinary write commands. No SQL, no DB connection — statements
live in the storage writer.

Defaults: purge every 15 min, checkpoint every 5 min, `ANALYZE` twice
daily, `VACUUM` daily; retention off unless configured:

```toml
[maintenance]
retention = "7d"            # or [maintenance.retention] logs = "7d"
```

Purge deletes expired events plus orphaned resources, so nothing grows
unbounded. FTS index is trigger-maintained, so pruned events leave no
stale hits. `rebuild_fts_interval` (off by default) re-reads every row —
recovery for a corrupted index only.

### Durability & acknowledgement

Default (`durability.mode = "commit"`): the export response waits until the
writer has committed every accepted record. Each chunk carries a commit
ticket; the writer publishes a contiguous watermark per transaction and
handlers wait on it — a 2xx survives an application crash. Cost: bounded
ack latency (batching + queue drain).

```toml
[durability]
mode = "commit"        # "commit" (default) | "enqueue" (ack at queue entry)
synchronous = "normal" # "normal" | "full" (fsync WAL per commit)
```

- `"enqueue"`: lowest latency, but acked records may be lost on crash.
- `synchronous = "full"`: extends guarantee from app crashes to power loss.
- Writer/batcher death fails pending requests with `UNAVAILABLE` (retryable)
  instead of hanging.

Backpressure is **all-or-nothing**: the ingress admission guard reserves
room for a whole request before enqueueing any chunk; control messages pass
the same gate, so requests never interleave. If the queue can't fit the
request, the server returns `UNAVAILABLE` and accepts **nothing** — safe to
retry, no duplicates (log rows have no idempotency key). Rejections issue
no tickets and never stall later commits.

### Observability

- **Prometheus `/metrics`** disabled by default; set `metrics_address` to
  `host:port` to enable. Exposes request/record counters, queue depths,
  batcher occupancy, transaction durations, writer errors, drops, and
  `sidecar_health_state`. **Plain HTTP by design** — bind localhost and put
  TLS/auth on a reverse proxy for remote scraping. No built-in auth
  (`[tls]` covers only the OTLP listener).
- **gRPC health** (`grpc.health.v1`) on the OTLP port. Overall + logs
  service report `NOT_SERVING` unless writer *and* batcher are running;
  shutdown flips to `NOT_SERVING` before draining.
- **Startup readiness**: `Storage::open` blocks until DB open + migrated; a
  broken backend fails startup, not first traffic.
- **Failure policy**: per batch — transient (`BUSY`/`LOCKED`) retries 4×
  with backoff; poisonous rows (constraints, binding failures) salvage:
  healthy rows commit, offenders drop-and-count (`quarantined_records`,
  warn-logged); unknown/environmental (disk full, IO, corruption) is fatal:
  writer halts, ledger closes, watchdog/supervisor takes over.
  **Page on** `storage_quarantined_records_total` (dropped rows never return)
  **and** `storage_errors_total` growth.

```toml
listen_address = "0.0.0.0:4317"
allow_insecure_remote = true         # isolated/trusted net only; prefer [tls]
# metrics_address = "127.0.0.1:8888" # off by default
max_records_per_request = 100000
# allow_remote_metrics = true        # explicit opt-in; prefer protected proxy
```

Config validates before listeners/workers start; effective config prints as
JSON. Remote metrics need explicit opt-in.

### Running with Docker

Image runs as non-root `otel-sqlite`, `WORKDIR /data` — mount a volume or
data stays ephemeral. Default bind is loopback, so publishing a port needs
a wildcard bind (cleartext needs `allow_insecure_remote`, or configure
`[tls]`):

```sh
docker run -d -v otel-sqlite-data:/data \
  -v "$PWD/otel-sqlite.toml:/data/otel-sqlite.toml:ro" \
  -e OTEL_SQLITE_CFG_PATH=/data/otel-sqlite.toml \
  -p 4317:4317 otel-sqlite
```

Built-in `HEALTHCHECK` (`otel-sqlite healthcheck`) probes
`grpc.health.v1.Check` on localhost; override with
`OTEL_SQLITE_HEALTHCHECK_ENDPOINT` for custom ports.

### Backups, restore and upgrades

Online backups via the SQLite Online Backup API — no external `sqlite3`
needed, ingestion never pauses:

```sh
otel-sqlite backup   --db otel-logs.db --out /backups --keep 30
otel-sqlite backup   --db otel-logs.db --out /backups --key-file /keys/backup.key
otel-sqlite restore  --backup /backups/otel-logs.db.<UTC>.otsb --dir /data/restored --key-file /keys/backup.key
otel-sqlite verify   --db otel-logs.db
```

`backup` copies a consistent snapshot through the WAL, normalizes to one
self-contained file, verifies integrity/foreign-keys/row-counts, prunes
beyond `--keep`. `restore` refuses non-empty dirs and verifies before/after.
`verify` exits non-zero on corruption. Encryption is optional AES-256-GCM
(32-byte key file).

- Runbook: [docs/operations/backup-restore.md](docs/operations/backup-restore.md)
- Setup: [systemd](docs/operations/backup-systemd.md),
  [Kubernetes](docs/operations/backup-kubernetes.md)
- Migrations & rollback (forward-only; rollback = restore):
  [docs/operations/upgrades.md](docs/operations/upgrades.md)
- Examples: [deploy/systemd/](deploy/systemd/),
  [deploy/kubernetes/](deploy/kubernetes/)

### Security: TLS/mTLS and bearer tokens

Private PKI, no external CA:

```sh
otel-sqlite gen-certs --host otel.internal --client collector-a --out ./certs
```

Writes `ca.pem/ca.key` (move `ca.key` offline), a server identity (SANs
plus `localhost`/`127.0.0.1`/`::1`), and one client pair per `--client`:

```toml
[tls]
cert = "./certs/server.pem"
key = "./certs/server.key"
client_ca = "./certs/ca.pem"   # presence enables mandatory mTLS

[auth]
mode = "token"
token_file = "/etc/otel-sqlite/clients.txt"   # one token per line
```

- **mTLS**: clients need a `client_ca`-signed cert. Reissue per-client with
  `gen-certs --client x` (reuses CA); rotating the CA (`--force`)
  invalidates everything.
- **Tokens**: `authorization: Bearer <token>`, stored hashed
  (constant-time compare), reloaded every few seconds — rotate by appending,
  migrating, then removing. Deleting the file revokes all.
- Health stays unauthenticated for probes/LBs; under mTLS the built-in
  healthcheck uses the server's own cert (serverAuth + clientAuth).
- Non-loopback bind without `[tls]` refuses to start (or set
  `allow_insecure_remote = true` on trusted nets). `mode = "token"`
  without a readable token file refuses to start.

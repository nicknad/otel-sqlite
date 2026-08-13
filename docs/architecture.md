# Architecture

The collector accepts OTLP Logs over gRPC, maps protobuf messages to internal
models, and persists them through a single SQLite writer.

```text
gRPC OTLP
  -> mapper
  -> bounded ingress queue (batches)
  -> batcher
  -> bounded command queue
  -> single SQLite writer
  -> SQLite (WAL)

batcher --non-blocking--> optional notification worker --> notifiers
HTTP /metrics and /health run on a separate listener.
```

## Boundaries

| Package | Responsibility |
|---|---|
| `internal/otlp` | gRPC service and protobuf-to-domain mapping |
| `internal/model` | Transport- and storage-independent log types |
| `internal/ingest` | Bounded ingress queue |
| `internal/batcher` | Combine incoming batches and create commands |
| `internal/storage` | Queue and command interfaces |
| `internal/storage/sqlite` | Migrations, SQLite commands, and writer |
| `internal/maintenance` | Scheduled database maintenance |
| `internal/notifications`, `internal/rules`, `internal/alerts` | Alert matching, state, delivery, retry, and DLQ |
| `internal/metrics` | Prometheus instrumentation |

OTLP protobuf types must remain in `internal/otlp`; other packages use the
internal domain model.

## Write path

1. The gRPC handler maps each request to one or more internal batches.
2. Batches enter the bounded ingress queue. A full queue blocks the request;
   an optional fullness threshold rejects it with `Unavailable`.
3. The batcher combines records and submits immutable `WriteBatchCommand`
   values to the command queue.
4. The writer consumes commands in one goroutine and commits transactions.
   `WRITER_BATCH_SIZE` counts commands; `WRITER_MAX_TRANSACTION_RECORDS`
   limits records.
5. Resources are inserted before their events. The writer uses WAL mode and
   prepared statements by default.

The storage path prefers backpressure to loss, but requests can still fail when
the client context is cancelled or early rejection is enabled. Clients should
retry `Unavailable` responses.

## Commands

```go
type Command interface {
    Execute(context.Context, *sql.Tx) error
}
```

Implemented commands include `WriteBatchCommand`, `PurgeLogsCommand`,
`CheckpointCommand`, `OptimizeCommand`, `VacuumCommand`, and
`RebuildFtsCommand`. Maintenance tasks submit commands rather than accessing
SQLite directly.

## Notifications

The batcher forwards records at or above the configured severity threshold to a
bounded, non-blocking notification queue. A full notification queue returns
`ErrQueueFull` and does not block or fail SQLite ingestion.

The worker evaluates rules in order (first match wins), aggregates each
rule/resource pair in a sliding window, and stores alert state in bbolt:

```text
pending -> firing -> resolved
```

Log and HTTP webhook notifiers are built in. Delivery retries use exponential
backoff; exhausted or non-retryable deliveries go to the persistent DLQ.
Alert and delivery state use separate bbolt files.

## Database

Migrations are applied in order at startup:

- `001`: base resource, event, and legacy attribute tables
- `002`: legacy trigger-based FTS and search indexes
- `003`: `logs` view and contentless `logs_fts`; removes legacy triggers
- `004`: removes unused indexes
- `005`: backfills attributes into `log_event.attributes_json` and removes
  `log_attr`

The `logs` view joins `log_event` and `log_resource`. `logs_fts` indexes only
`body` and `service_name`; it is rebuilt with `DROP`/`CREATE` by maintenance,
not updated by triggers. It can be empty until the first rebuild.

The supported deployment shape is one collector per writable database file.
SQLite permits concurrent readers in WAL mode, but multiple collector writers
sharing a file are not a scaling mechanism.

## Operational endpoints

- OTLP gRPC: `LISTEN_ADDRESS` (default `:4317`)
- Prometheus and HTTP health: `METRICS_ADDRESS` (default `:9090`)
- `/metrics`: Prometheus exposition
- `/health`: `200 OK` liveness response

There is no gRPC health service. The listeners are plaintext and unauthenticated
by default; deploy behind an appropriate network or proxy when exposed.

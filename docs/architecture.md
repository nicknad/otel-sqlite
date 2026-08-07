# OTLP SQLite Collector Architecture

## Overview

The OTLP SQLite Collector is a high-performance log collector that receives OpenTelemetry OTLP logs over gRPC, processes them through a bounded ingestion pipeline, and persists them to SQLite for efficient querying.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         OTLP Collector                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐    │
│  │  gRPC Server │────▶│ OTLP Mapper  │────▶│ Ingress Queue│    │
│  └──────────────┘     └──────────────┘     └──────────────┘    │
│          ▲                    │                       │            │
│          │                    │                       ▼            │
│  OTLP/gRPC                OTLP Protobuf          ┌──────────────┐    │
│  (transport layer)        (isolated)             │   Batcher    │    │
│                                            │──────▶│ (builds     │    │
│                                            │       │  WriteBatch │    │
│                                            │       │  Command)   │    │
│                                            │       └──────────────┘    │
│                                            │              │            │
│                                            ▼              ▼            │
│                                         ┌──────────────┐     ┌─────────┐ │
│                                         │ Command Queue│────▶│ SQLite  │ │
│                                         │ (bounded,    │     │ Writer  │ │
│                                         │  FIFO,       │     │ (executes│ │
│                                         │  single      │     │ commands)│ │
│                                         │  consumer)   │     └─────────┘ │
│                                         └──────────────┘          │    │
│                                                                   ▼    │
│                                                              ┌─────────┐ │
│                                                              │ SQLite  │ │
│                                                              │ DB      │ │
│                                                              └─────────┘ │
│                                                                     │
│  ┌──────────────┐     ┌──────────────┐                            │
│  │ Metrics      │◀────│ Prometheus   │                            │
│  │ Server       │     │ Endpoint     │                            │
│  └──────────────┘     └──────────────┘                            │
│                                                                     │
└─────────────────────────────────────────────────────────────────┘
```

## Component Ownership Rules

### 1. Transport Layer (`internal/otlp/`)
- **Owns**: OTLP protobuf types, gRPC service implementation
- **Responsibilities**:
  - Receive OTLP logs over gRPC
  - Convert OTLP protobuf messages to internal domain models
  - Handle transport-level errors and backpressure
- **Must NOT**:
  - Import internal domain models into protobuf files
  - Perform business logic beyond mapping
  - Access storage directly

### 2. Domain Model (`internal/model/`)
- **Owns**: Internal domain types (`LogRecord`, `LogBatch`, `Resource`, `AttributeValue`)
- **Responsibilities**:
  - Define the canonical representation of log data
  - Provide type-safe access to log data
  - Be independent of OTLP protobuf types
- **Must NOT**:
  - Import OTLP protobuf types
  - Contain storage or transport logic

### 3. Ingestion Pipeline (`internal/ingest/`)
- **Owns**: Queue abstractions for log batches
- **Responsibilities**:
  - Provide bounded ingress queue for backpressure
  - Ensure thread-safe access to queues
  - Block when queues are full
- **Must NOT**:
  - Perform mapping or transformation logic
  - Access storage directly

### 4. Batcher (`internal/batcher/`)
- **Owns**: Batch building and command construction
- **Responsibilities**:
  - Collect individual log records into batches
  - Wrap completed batches in WriteBatchCommand objects
  - Submit commands to the command queue
  - Manage batch size and timing
- **Must NOT**:
  - Access storage directly
  - Import SQLite packages
  - Perform mapping logic

### 5. Storage (`internal/storage/`)
- **Owns**: Storage abstractions and the Command/CommandExecutor interfaces
- **Responsibilities**:
  - Define `Command` interface for database mutations
  - Define `CommandExecutor` interface for submission
  - Provide `CommandQueue` bounded FIFO queue implementation
  - Define legacy `LogStorage`, `LogWriter`, `LogReader` interfaces
- **Must NOT**:
  - Import OTLP protobuf types
  - Perform business logic beyond persistence

### 6. SQLite Writer (`internal/storage/sqlite/`)
- **Owns**: SQLite-specific write logic via command execution
- **Responsibilities**:
  - Single dedicated goroutine for all SQLite commands
  - Transaction lifecycle (begin, commit, rollback)
  - Command execution within transactions
  - Retries and error handling
  - Batched transactions for performance
  - WAL mode configuration
- **Must NOT**:
  - Allow concurrent writes from multiple goroutines
  - Perform mapping logic
  - Begin/commit/rollback transactions from within commands

### 7. Notification Pipeline (`internal/notifications/`, `internal/rules/`, `internal/alerts/`, `internal/events/`)
- **Owns**: Event model, rule engine, alert state machine, notifier implementations, worker goroutines
- **Responsibilities**:
  - Receive error-level events from the batcher via non-blocking channel
  - Evaluate rules (severity threshold, regex filters on resource/body/attributes)
  - Maintain alert state per rule×resource pair (pending → firing → resolved)
  - Count events within configurable sliding time windows per rule
  - Fire alerts when count exceeds threshold; auto-resolve after silence window
  - Persist alert state in bbolt KV store (`alert-state.db`)
  - Track notification delivery state (cooldown, retries) in bbolt (`notify-state.db`)
  - Deliver alert notifications via pluggable notifiers (HTTP webhook, log)
  - Retry failed deliveries with exponential backoff (capped at 2^10)
  - Move exhausted or non-retryable deliveries to dead-letter queue
  - Garbage-collect resolved alerts older than the configurable `resolved_alert_retention` (default 24h)
- Evict idle Pending/Firing alerts after `alert_idle_ttl`
- Compact bbolt stores periodically (when enabled)
  - Drain event queue on graceful shutdown
- **Must NOT**:
  - Block the batcher hot path (non-blocking send, returns ErrQueueFull when full)
  - Leak OTLP or SQLite types
  - Depend on specific notifier implementations

### 8. Metrics (`internal/metrics/`)
- **Owns**: Prometheus metrics collection
- **Responsibilities**:
  - Define and expose application metrics
  - Provide instrumentation hooks for other components
- **Must NOT**:
  - Contain business logic
  - Access storage directly

## Command Architecture

### Command Interface

All database mutations are expressed as `Command` objects:

```go
type Command interface {
    Execute(ctx context.Context, tx *sql.Tx) error
}
```

Properties:
- **Immutable**: Command data is fully initialized before submission.
- **Self-contained**: Each command encapsulates its execution logic.
- **Transaction-safe**: Commands receive a `*sql.Tx` and must not call `Begin`/`Commit`/`Rollback`.

### Command Executor Interface

The SQLite Writer implements `CommandExecutor`:

```go
type CommandExecutor interface {
    Submit(ctx context.Context, cmd Command) error
}
```

The rest of the system depends only on this interface.

### Initial Commands

| Command | Purpose |
|---------|---------|
| `WriteBatchCommand` | Write a LogBatch to SQLite |

### Implemented Commands

| Command | Interface | Purpose |
|---------|-----------|---------|
| `WriteBatchCommand` | `Command` | Write a LogBatch to SQLite (hot path) |
| `PurgeLogsCommand` | `Command` | Delete logs older than retention threshold |
| `CheckpointCommand` | `NonTransactionalCommand` | Force WAL checkpoint (runs outside transaction) |
| `OptimizeCommand` | `NonTransactionalCommand` | Run PRAGMA optimize (runs outside transaction) |
| `VacuumCommand` | `NonTransactionalCommand` | Rebuild database file to reclaim space |
| `RebuildFTSCommand` | `Command` | Drop and recreate contentless FTS5 index |

## Queue Flow and Backpressure

### Pipeline Stages

1. **gRPC Server**: Receives OTLP requests, maps to internal domain models (using pooled `GetRecord()`)
2. **Ingress Queue**: Bounded queue for log batches (batches, not individual records — 1 channel op per batch)
3. **Batcher**: Collects batches, groups into larger batches, wraps in WriteBatchCommand
4. **Command Queue**: Bounded FIFO queue for Command objects
5. **SQLite Writer**: Single goroutine that executes commands within SQLite transactions and returns records to pool via `PutRecord()`

### Backpressure Behavior

- **Ingress Queue Full**: gRPC server blocks on `Send()` to ingress queue
  - gRPC request handling blocks
  - Client experiences increased latency
  - No data loss, but reduced throughput

- **Command Queue Full**: Batcher blocks on `Send()` to command queue
  - Ingress queue fills up
  - Eventually causes backpressure to gRPC server
  - No data loss

- **SQLite Writer Slow**: Command queue fills up
  - Batcher blocks
  - Ingress queue fills up
  - Backpressure propagates to gRPC server
  - No data loss

### Bounded Queue Properties

- All queues are implemented as bounded channels
- `Send()` blocks when queue is full
- `Receive()` blocks when queue is empty
- Queue capacity is configurable
- Queue depth is exposed via Prometheus metrics

## SQLite Writer Model

### Single Writer Principle

- **Only one goroutine** may write to SQLite at any time
- This prevents:
  - Concurrent write conflicts
  - Database lock contention
  - Transaction serialization issues
- The writer goroutine:
  - Receives commands from the command queue
  - Groups commands into transactions
  - Executes each command's `Execute(ctx, tx)` within the transaction
  - Handles all database errors

### Transaction Batching

- Multiple commands are grouped into a single transaction
- Configurable batch size for transactions
- Configurable flush interval for time-based flushing
- Maximum record cap per transaction (`maxTransactionRecords = 5000`)
- Prepared statements for efficiency

### Writer Responsibilities

The SQLite Writer owns:
- SQLite connection
- Transaction lifetime (begin, commit, rollback)
- Command execution
- Retries (transaction-level)
- Metrics and logging

It does NOT own command construction.

### Performance Optimizations

- **WAL Mode**: Enables concurrent reads while writing
- **Prepared Statements**: Reduces SQL parsing overhead; single compilation at startup, reused via `tx.Stmt()` per transaction
- **Batched Transactions**: Reduces commit overhead
- **Single Connection**: Avoids connection pool overhead for write-heavy workload
- **Object Pooling**: `LogRecord` objects are reused via `sync.Pool` — zero allocations in the hot path
- **Fixed-Size Arrays**: TraceID (`[16]byte`) and SpanID (`[8]byte`) avoid heap allocation for trace context
- **Inline Attributes**: `[]Attribute` slice replaces `map[string]AttributeValue` — eliminates map overhead
- **Batch-Level Ingress**: Batches flow through the ingress queue instead of individual records, reducing channel operations from N per batch to 1
- **Minimized Indexes**: Unused indexes removed (migration 004) to reduce write overhead by ~33%
- **Large Page Cache**: 64 MB page cache (`cache_size=-65536`) for B-tree efficiency on established databases
- **Memory-Mapped I/O**: 256 MB mmap (`mmap_size=268435456`) for zero-copy page access
- **In-Memory Temp Store**: `temp_store=MEMORY` avoids temporary file I/O for sorts and indices
- **WAL Size Cap**: `journal_size_limit=64MB` prevents runaway WAL growth
- **Lock Retry**: `busy_timeout=5000ms` avoids spurious failures during maintenance operations

## OTLP Boundary

### Isolation Rules

- OTLP protobuf types **must not** leave the `internal/otlp/` package
- Internal packages **must not** import OTLP protobuf types
- Mapping from OTLP to internal models happens in `internal/otlp/`
- All other packages work only with internal domain models

### Benefits

- **Decoupling**: Internal logic is independent of OTLP version
- **Testability**: Easy to test with mock data
- **Maintainability**: Changes to OTLP don't affect internal code
- **Type Safety**: Internal models can evolve independently

## Database Schema

### Tables

- `log_resource`: Resource metadata (service.name, host.name, etc.)
- `log_event`: Log record metadata plus compact event attributes in
  `attributes_json` (one JSON object per event)
- `log_attr`: Historical EAV table removed by migration 005 after typed rows are
  backfilled into `log_event.attributes_json`.

### Indexes

- Primary keys on all tables
- Foreign keys for referential integrity
- Active indexes (prioritizing write performance):
  - `timestamp_ns` for time-range queries
  - `resource_id` for resource filtering
  - no attribute indexes; individual keys are queried from JSON only when a
    deliberate `json_extract`/expression-index decision is made
- **Removed indexes** (migration 004/005): `severity_number`, `trace_id`,
  `severity_text`, `body`, `event_name`, composite `(resource_id,
  timestamp_ns)`, composite `(trace_id, timestamp_ns)`, and all attribute
  indexes — justified by benchmark results and the removal of per-attribute
  writes.

### Future Enhancements

- Full-text search on log body
- Composite indexes for common query patterns
- Materialized views for aggregated data

## Observability

### Metrics

- **Ingestion**: Logs received, queue depths
- **Batching**: Batches created, batch sizes
- **Commands**: Command queue depth, execution duration, executed/failure totals
- **Storage**: Logs written, write latency, write errors
- **Resources**: Active resources, total resources
- **Notification**: Events received, matched (by rule), delivered (by destination), failed (by destination), dead-lettered, queue depth, retry queue depth

All command metrics are labelled by command type for granular observability.

### Health Checks

- gRPC health check endpoint
- HTTP health check endpoint
- Database connectivity check

### Logging

- Structured logging for operational events
- Command type information included in log messages
- Error logging with context
- Performance logging for slow operations

## Configuration

### Environment Variables

- `LISTEN_ADDRESS`: gRPC server listen address
- `SQLITE_PATH`: Path to SQLite database file
- `GRPC_MAX_RECV_MSG_SIZE`: Max gRPC receive message size in bytes (default: 16777216 / 16 MB)
- `GRPC_MAX_SEND_MSG_SIZE`: Max gRPC send message size in bytes (default: 16777216 / 16 MB)
- `GRPC_MAX_CONCURRENT_STREAMS`: Max concurrent gRPC streams (default: 100)
- `INGRESS_QUEUE_BACKPRESSURE_THRESHOLD`: Queue fullness fraction (0.0–1.0) for early rejection (default: 0 = disabled; recommends 0.8)
- `GO_MEMORY_LIMIT_MB`: Go runtime memory limit in MB (default: 0 = disabled; recommends ~80% of container limit)
- `INGRESS_QUEUE_CAPACITY`: Maximum ingress queue size
- `BATCH_QUEUE_CAPACITY`: Maximum command queue size
- `BATCHER_BATCH_SIZE`: Number of records per batcher batch
- `BATCHER_FLUSH_INTERVAL`: Maximum time between batcher flushes
- `BATCHER_ERROR_SEVERITY_THRESHOLD`: Minimum severity forwarded to notification worker
- `WRITER_BATCH_SIZE`: Number of commands per writer transaction
- `WRITER_FLUSH_INTERVAL`: Maximum time between writer flushes
- `WRITER_MAX_TRANSACTION_RECORDS`: Maximum records per SQLite transaction
- `METRICS_ADDRESS`: Prometheus metrics server address
- `NOTIFICATION_ENABLED`: Enable notification pipeline (default: false)
- `NOTIFICATION_EVENT_QUEUE_DEPTH`: Notification event queue capacity
- `NOTIFICATION_STORE_PATH`: Path to notification delivery state store (bbolt)
- `NOTIFICATION_ALERT_STORE_PATH`: Path to alert state store (bbolt)
- `NOTIFICATION_RETRY_INTERVAL`: Retry scan interval
- `NOTIFICATION_ALERT_IDLE_TTL`: Max idle time for Pending/Firing alerts before GC eviction
- `NOTIFICATION_RESOLVED_ALERT_RETENTION`: How long Resolved alerts are retained before GC
- `NOTIFICATION_DLQ_RETENTION`: Max age of DLQ entries before purge
- `NOTIFICATION_BBOLT_COMPACTION_ENABLED`: Enable periodic bbolt compaction
- `NOTIFICATION_BBOLT_COMPACTION_INTERVAL`: Interval between bbolt compactions

### Configuration File

See `config.example.yaml` for YAML configuration format.

## Error Handling

### Backpressure

- Prefer backpressure over data loss
- Block when queues are full
- Propagate backpressure to clients

### Database Errors

- On command failure: rollback transaction, log error, increment failure metrics
- Writer continues processing subsequent command batches
- Log and count permanent errors

### gRPC Errors

- Return appropriate gRPC status codes
- Include error details in responses
- Handle cancellation gracefully

## Notification Pipeline

### Architecture

The notification pipeline is an optional subsystem that sends alerts when error-level log records are ingested. It sits as a sidecar to the main ingestion path, receiving events from the batcher via a non-blocking buffered channel.

```
Batcher (severity >= threshold)
  → Notification Worker (event queue, non-blocking)
    → Rule Engine (first-match-wins, regex filters)
      → Alert Pipeline:
          Time Window (sliding window: count events within alert_window)
          → Counter (count >= alert_threshold?)
          → State Machine (pending → firing → resolved)
          → Alert Store (bbolt: persisted per rule×resource)
        → Notifier (HTTP webhook or log, sends Alert JSON)
          → Dead-Letter Queue (after max retries or 4xx)
```

### Package Layout

| Package | Purpose |
|---------|---------|
| `internal/events/` | Event type, fingerprinting, LogRecord conversion |
| `internal/rules/` | Rule definition, regex matching engine |
| `internal/alerts/` | Alert struct, sliding window, counter, state machine, AlertStore |
| `internal/notifications/` | Notifier interface, HTTP/log implementations, delivery state, retry/DLQ, worker |

### Alert Pipeline

Each event goes through the pipeline:
1. **Rule evaluation**: Check severity, resource, body, and attribute filters
2. **Time window**: Add event timestamp to a sliding window (configurable per rule via `alert_window`)
3. **Counter**: Count events currently in the window; check against `alert_threshold`
4. **State machine**: Determine next alert state
   - **Pending**: counting but below threshold. Stale pending alerts (above window, no recent hits) are garbage-collected.
   - **Firing**: threshold exceeded. Notification sent on transition. Subsequent events increment count but don't re-notify.
   - **Resolved**: was firing, now below threshold for `alert_resolve_window`. Resolution notification sent.
5. **Alert persistence**: Alert object (ID, rule, resource, status, severity, count, timestamps, window) stored in bbolt
6. **Notification delivery**: Alert JSON delivered via configured notifier with retry backoff

### Alert Model

Each rule×resource pair creates one `Alert` with a composite ID (`ruleID:resourceID`). Multiple distinct error messages from the same resource under the same rule aggregate into a single alert. The alert carries the current count, status, severity, and timing metadata.

### Delivery State

Notification delivery state (cooldown, retry count, next retry time) is tracked separately from alert state in a dedicated bbolt store. A retry index bucket enables O(retryable) scanning. Delivery state is garbage-collected after 24h of successful delivery.

### Non-Retryable Errors

HTTP 4xx responses (401, 403, 404) are treated as non-retryable and sent directly to the dead-letter queue. HTTP 5xx and network errors follow the standard retry backoff. Notifier implementations can mark errors as non-retryable by wrapping them with `NewNotRetryableError()`.

### Garbage Collection

**Garbage Collection**:
- **Resolved alerts**: Deleted after `resolved_alert_retention` (default 24h) via the GC goroutine.
- **Idle pending/firing alerts**: Evicted after `alert_idle_ttl` of inactivity.
- **DLQ entries**: Purged after `dlq_retention` (default 30d).
- **Bbolt compaction**: Optionally compact bbolt stores to reclaim freed pages to the OS.

## Extensibility

Adding a new maintenance command requires:
1. Implement the `Command` or `NonTransactionalCommand` interface
2. The SQLite Writer needs no changes
3. The command queue needs no changes
4. Submit the command via `CommandExecutor.Submit()`

Adding a new notifier requires:
1. Implement the `Notifier` interface (`Send`, `Name`, `Close`) — `Send` receives an `*alerts.Alert`
2. Register it in `notifications.NewNotifier()` factory
3. Add config parsing in `cmd/collector/main.go`

This is the key benefit of the command architecture: the execution path is generic and extensible without modifying the queue or the writer.

## Testing Strategy

### Unit Tests

- Each package has isolated unit tests
- Mock Command implementations for scheduling tests
- Mock command factory for batcher tests
- Test edge cases and error conditions

### Integration Tests

- Test complete pipeline with mock gRPC client
- Test SQLite persistence via WriteBatchCommand execution
- Test backpressure behavior
- Test command queue FIFO ordering

### Performance Tests

- Benchmark mapping performance
- Benchmark write throughput
- Test under load with various batch sizes

### End-to-End Tests

- Test with real OTLP clients
- Test with various log volumes
- Test failure recovery

## Deployment

### Containerization

- Docker image with multi-stage build
- Minimal base image
- Health checks and readiness probes

### Configuration

- Environment variables for runtime configuration
- Config file for complex settings
- Sensible defaults for all settings

### Monitoring

- Prometheus metrics endpoint (`/metrics`)
- Health check endpoint (`/health`)
- Backpressure rejection counter (`otel_collector_ingest_backpressure_rejections_total`)
- Queue depth gauges for capacity planning
- Structured logging

### Resource Limits & Guardrails

#### Container Limits (Docker)

| Profile | Memory | CPU | Throughput |
|---|---|---|---|
| Conservative | 512 MB | 1 | ~100K rec/s |
| Typical | 2 GB | 2 | ~200K rec/s |
| High-throughput | 4 GB | 4 | ~400K rec/s |

Memory scales with ingress queue capacity. Reducing `INGRESS_QUEUE_CAPACITY` from
the default 10,000 to 2,000–3,000 cuts worst-case memory from ~1.2 GB to ~250 MB
with minimal throughput impact.

#### GOMAXPROCS

When using CPU limits in Docker, set `GOMAXPROCS` to match the container's CPU quota
so Go's scheduler doesn't spawn more OS threads than available CPUs:

```
docker run -e GOMAXPROCS=2 ...
```

Alternatively, use the [automaxprocs](https://github.com/uber-go/automaxprocs)
library to auto-detect the cgroup CPU limit.

#### Ingress Queue Sizing

The ingress queue is the largest memory consumer. Each slot holds a pointer to a
LogBatch (~250 records × ~500 B/record ≈ 125 KB). Default capacity (10,000)
buffers up to ~1.25 GB of records in-flight.

- **Lower** (1,000–3,000): reduces memory, clients see backpressure sooner
- **Higher** (10,000–20,000): absorbs larger load spikes at higher memory cost

#### Backpressure Threshold

`INGRESS_QUEUE_BACKPRESSURE_THRESHOLD` enables early rejection (codes.Unavailable)
when the queue exceeds a fullness fraction, before the gRPC handler blocks.
Recommended: `0.8` (80% full).

**Trade-off:** Some requests are rejected instead of queued. Clients must retry.
Effective throughput drops 5–15% under saturation, but memory stays bounded.

#### Go Memory Limit

`GO_MEMORY_LIMIT_MB` sets `debug.SetMemoryLimit()` at startup, telling the Go GC
to be more aggressive when heap nears the limit. Set to ~80% of your container
memory limit (e.g., 1600 for a 2 GB container).

**Trade-off:** Slightly higher CPU usage from GC (5–10% overhead), but prevents
OOM kills.

#### SQLite WAL Checkpoint

WAL growth is bounded by `wal_autocheckpoint=1000` pages (~4 MB). Enable the
maintenance checkpoint task with a tighter interval for high-throughput workloads:

```yaml
checkpoint:
  enabled: true
  interval: "1h"       # default 24h — tighten to 1h for production
  mode: "PASSIVE"
```

#### gRPC Keepalive & Stream Limits

The collector enforces by default:
- Max 100 concurrent streams per connection (configurable)
- Keepalive with 30-second minimum ping interval
- Connections idle > 5 minutes are closed
- Connections older than 30 minutes are cycled

These prevent resource exhaustion from misconfigured or malicious clients.

### Scaling

- Single instance for most use cases
- Multiple instances with shared storage for high volume
- Read replicas for query-heavy workloads

#### Partitioning by Resource

For horizontal scaling across multiple collector instances, route log traffic by
resource identifier (e.g., service.name). Each collector owns a disjoint subset
of resources, avoiding write contention on the SQLite database.

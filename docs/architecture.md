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
- **Owns**: Queue abstractions for individual log records
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

### 7. Metrics (`internal/metrics/`)
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

### Future Commands (not yet implemented)

| Command | Purpose |
|---------|---------|
| `PurgeLogs` | Delete logs older than a retention threshold |
| `CheckpointWAL` | Force WAL checkpoint |
| `OptimizeDatabase` | Run PRAGMA optimize |
| `VacuumDatabase` | Reclaim storage space |
| `ArchiveLogs` | Move old logs to archival storage |
| `ReindexDatabase` | Rebuild database indexes |

## Queue Flow and Backpressure

### Pipeline Stages

1. **gRPC Server**: Receives OTLP requests, maps to internal models
2. **Ingress Queue**: Bounded queue for individual log records
3. **Batcher**: Collects records into batches, wraps in WriteBatchCommand
4. **Command Queue**: Bounded FIFO queue for Command objects
5. **SQLite Writer**: Single goroutine that executes commands within SQLite transactions

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
- **Prepared Statements**: Reduces SQL parsing overhead
- **Batched Transactions**: Reduces commit overhead
- **Single Connection**: Avoids connection pool overhead for write-heavy workload

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
- `log_event`: Log record metadata (timestamp, severity, trace context, etc.)
- `log_attr`: Log record attributes (key-value pairs)

### Indexes

- Primary keys on all tables
- Foreign keys for referential integrity
- Indexes on frequently queried columns:
  - `timestamp_ns` for time-range queries
  - `severity_number` for severity filtering
  - `trace_id` for trace correlation
  - `resource_id` for resource filtering
  - `key` on attributes for attribute-based queries

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
- `INGRESS_QUEUE_CAPACITY`: Maximum ingress queue size
- `BATCH_QUEUE_CAPACITY`: Maximum command queue size
- `BATCH_SIZE`: Number of records per batch
- `FLUSH_INTERVAL`: Maximum time between batch flushes
- `METRICS_ADDRESS`: Prometheus metrics server address

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

## Extensibility

Adding a new maintenance command requires:
1. Implement the `Command` interface
2. The SQLite Writer needs no changes
3. The command queue needs no changes
4. Submit the command via `CommandExecutor.Submit()`

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

- Prometheus metrics endpoint
- Health check endpoints
- Structured logging

### Scaling

- Single instance for most use cases
- Multiple instances with shared storage for high volume
- Read replicas for query-heavy workloads

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
│                                            │──────▶│              │    │
│                                            │       └──────────────┘    │
│                                            │              │            │
│                                            ▼              ▼            │
│                                         ┌──────────────┐     ┌─────────┐ │
│                                         │ Batch Queue  │────▶│ SQLite  │ │
│                                         └──────────────┘     │ Writer  │ │
│                                                               └─────────┘ │
│                                                                      │    │
│                                                                      ▼    │
│                                                               ┌─────────┐ │
│                                                               │ SQLite  │ │
│                                                               │ DB      │ │
│                                                               └─────────┘ │
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
- **Owns**: Queue abstractions and implementations
- **Responsibilities**:
  - Provide bounded queues for backpressure
  - Ensure thread-safe access to queues
  - Block when queues are full
- **Must NOT**:
  - Perform mapping or transformation logic
  - Access storage directly

### 4. Batcher (`internal/batcher/`)
- **Owns**: Batch building logic
- **Responsibilities**:
  - Collect individual log records into batches
  - Manage batch size and timing
  - Send batches to the batch queue
- **Must NOT**:
  - Access storage directly
  - Perform mapping logic

### 5. Storage (`internal/storage/`)
- **Owns**: Storage abstractions and implementations
- **Responsibilities**:
  - Define storage interfaces
  - Implement SQLite storage backend
  - Manage database connections and transactions
- **Must NOT**:
  - Import OTLP protobuf types
  - Perform business logic beyond persistence

### 6. SQLite Writer (`internal/storage/sqlite/`)
- **Owns**: SQLite-specific write logic
- **Responsibilities**:
  - Single dedicated goroutine for all SQLite writes
  - Batched transactions for performance
  - WAL mode configuration
  - Prepared statements for efficiency
- **Must NOT**:
  - Allow concurrent writes from multiple goroutines
  - Perform mapping logic

### 7. Metrics (`internal/metrics/`)
- **Owns**: Prometheus metrics collection
- **Responsibilities**:
  - Define and expose application metrics
  - Provide instrumentation hooks for other components
- **Must NOT**:
  - Contain business logic
  - Access storage directly

## Queue Flow and Backpressure

### Pipeline Stages

1. **gRPC Server**: Receives OTLP requests, maps to internal models
2. **Ingress Queue**: Bounded queue for individual log records
3. **Batcher**: Collects records into batches
4. **Batch Queue**: Bounded queue for batches
5. **SQLite Writer**: Single goroutine that writes batches to SQLite

### Backpressure Behavior

- **Ingress Queue Full**: gRPC server blocks on `Send()` to ingress queue
  - gRPC request handling blocks
  - Client experiences increased latency
  - No data loss, but reduced throughput

- **Batch Queue Full**: Batcher blocks on `Send()` to batch queue
  - Ingress queue fills up
  - Eventually causes backpressure to gRPC server
  - No data loss

- **SQLite Writer Slow**: Batch queue fills up
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
  - Receives batches from the batch queue
  - Groups batches into transactions
  - Executes batched transactions
  - Handles all database errors

### Transaction Batching

- Multiple batches are grouped into a single transaction
- Configurable batch size for transactions
- Configurable flush interval for time-based flushing
- Uses prepared statements for efficiency
- WAL mode for concurrent read/write access

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
- **Storage**: Logs written, write latency, write errors
- **Resources**: Active resources, total resources

### Health Checks

- gRPC health check endpoint
- HTTP health check endpoint
- Database connectivity check

### Logging

- Structured logging for operational events
- Error logging with context
- Performance logging for slow operations

## Configuration

### Environment Variables

- `LISTEN_ADDRESS`: gRPC server listen address
- `SQLITE_PATH`: Path to SQLite database file
- `INGRESS_QUEUE_CAPACITY`: Maximum ingress queue size
- `BATCH_QUEUE_CAPACITY`: Maximum batch queue size
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

- Retry transient errors
- Log and count permanent errors
- Continue processing other batches on error

### gRPC Errors

- Return appropriate gRPC status codes
- Include error details in responses
- Handle cancellation gracefully

## Future Extensions

### Search API

- HTTP API for querying logs
- Support for time-range queries
- Support for attribute filtering
- Support for full-text search
- Pagination and sorting

### Additional Protocols

- HTTP/JSON OTLP endpoint
- OpenTelemetry Protocol (OTLP) over HTTP
- Legacy protocols (optional)

### Enhanced Storage

- Log rotation and retention policies
- Database compaction
- Backup and restore

### Scaling

- Horizontal scaling with shared storage
- Partitioning by resource or time
- Read replicas for query scaling

## Performance Considerations

### Throughput

- Bounded queues prevent memory exhaustion
- Batch processing reduces per-record overhead
- Single writer eliminates write contention

### Latency

- Ingress queue adds minimal latency
- Batch queue adds latency up to flush interval
- SQLite WAL mode enables concurrent reads

### Resource Usage

- Memory: Proportional to queue capacities
- CPU: Primarily in mapping and serialization
- Disk: SQLite database size grows with data volume
- Network: gRPC and metrics endpoints

## Testing Strategy

### Unit Tests

- Each package has isolated unit tests
- Mock dependencies for testing
- Test edge cases and error conditions

### Integration Tests

- Test complete pipeline with mock gRPC client
- Test SQLite persistence and querying
- Test backpressure behavior

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

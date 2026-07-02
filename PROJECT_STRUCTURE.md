# Project Structure Summary

This document describes the complete project structure for the OTLP SQLite Collector.

## Directory Tree

```
otel-sqlite/
├── api/
│   └── otlp/
│       └── opentelemetry/
│           └── proto/
│               ├── collector/
│               │   └── v1/
│               │       └── logs_service.proto
│               ├── common/
│               │   └── v1/
│               │       └── common.proto
│               ├── logs/
│               │   └── v1/
│               │       └── logs.proto
│               └── resource/
│                   └── v1/
│                       └── resource.proto
├── buf.gen.yaml              # Protobuf generation config for buf
├── buf.yaml                  # buf configuration
├── cmd/
│   └── collector/
│       └── main.go           # Application entry point
├── config.example.yaml       # Example configuration file
├── docker-compose.yml        # Docker Compose configuration
├── Dockerfile                # Docker build configuration
├── docs/
│   └── architecture.md       # Architecture documentation
├── .gitignore                # Git ignore patterns
├── .golangci.yml             # GolangCI-Lint configuration
├── go.mod                    # Go module definition
├── internal/
│   ├── batcher/
│   │   ├── batcher.go        # Batch building + command construction
│   │   └── batcher_test.go
│   ├── config/
│   │   ├── config.go         # Configuration management
│   │   └── config_test.go
│   ├── generated/            # Generated protobuf code (created by make generate)
│   ├── ingest/
│   │   ├── queue.go          # Bounded queue abstractions (ingress only)
│   │   └── queue_test.go
│   ├── metrics/
│   │   ├── metrics.go        # Prometheus metrics
│   │   └── metrics_test.go
│   ├── model/
│   │   ├── logrecord.go      # LogRecord domain model
│   │   ├── resource.go       # Resource domain model
│   │   ├── value.go          # AttributeValue domain model
│   │   └── model_test.go
│   ├── otlp/
│   │   ├── mapper.go         # OTLP to internal model mapper
│   │   ├── mapper_test.go
│   │   └── server.go         # gRPC server implementation
│   └── storage/
│       ├── command.go        # Command + CommandExecutor interfaces
│       ├── queue.go          # CommandQueue (bounded, FIFO, channel-based)
│       ├── queue_test.go
│       ├── storage.go        # Storage interfaces (LogStorage, LogWriter, LogReader)
│       └── sqlite/
│           ├── writer.go                 # SQLite writer (command executor)
│           ├── writer_test.go
│           └── write_batch_command.go    # WriteBatchCommand implementation
├── LICENSE                   # Apache License 2.0
├── Makefile                  # Build targets
├── migrations/
│   ├── 001_initial_schema.sql        # Initial database schema
│   ├── 002_add_search_indexes.sql    # Search indexes migration
│   ├── 003_logs_view_and_fts.sql     # Logs view and contentless FTS5
│   └── 004_remove_unused_indexes.sql # Remove unused indexes (~33% write improvement)
├── prometheus.yml            # Prometheus configuration
├── PROJECT_STRUCTURE.md      # This file
└── README.md                 # Project documentation
```

## Package Responsibilities

### `cmd/collector/`
- **Purpose**: Application entry point and main orchestration
- **Responsibilities**:
  - Load configuration
  - Initialize all components
  - Inject command factory (wires sqlite.NewWriteBatchCommand into the batcher)
  - Start gRPC and metrics servers
  - Manage lifecycle and shutdown
  - Wire together the ingestion pipeline

### `internal/config/`
- **Purpose**: Configuration management
- **Responsibilities**:
  - Define configuration structure
  - Provide default values
  - Validate configuration
  - Support multiple config sources (env, file)

### `internal/model/`
- **Purpose**: Domain models
- **Responsibilities**:
  - Define internal domain types (`LogRecord`, `LogBatch`, `Resource`, `AttributeValue`)
  - Provide type-safe access to domain data
  - Be completely independent of OTLP protobuf types

### `internal/otlp/`
- **Purpose**: OTLP transport layer and mapping
- **Responsibilities**:
  - Implement gRPC server for OTLP logs
  - Convert OTLP protobuf messages to internal domain models
  - Handle transport-level errors and backpressure
  - Isolate OTLP protobuf types to this package

### `internal/ingest/`
- **Purpose**: Bounded queue abstractions for log batches
- **Responsibilities**:
  - Provide `IngressQueue` interface for log batches
  - Implement bounded channel-based ingress queue
  - Provide backpressure through blocking sends

### `internal/batcher/`
- **Purpose**: Batch building and command construction
- **Responsibilities**:
  - Collect individual log records from ingress queue
  - Group records into batches based on size
  - Wrap completed batches in `WriteBatchCommand` objects (via injected factory)
  - Submit commands to the command queue
  - Manage batch timing and flushing
  - **No dependency on SQLite** — the command factory is injected externally

### `internal/storage/`
- **Purpose**: Storage abstractions and command infrastructure
- **Responsibilities**:
  - Define `Command` interface for all database mutation commands
  - Define `CommandExecutor` interface for command submission
  - Provide `CommandQueue` bounded FIFO implementation (channel-based)
  - Define legacy `LogStorage`, `LogWriter`, `LogReader` interfaces

### `internal/storage/sqlite/`
- **Purpose**: SQLite storage implementation via command execution
- **Responsibilities**:
  - Implement SQLite writer as a command executor
  - Provide `WriteBatchCommand` for writing log batches
  - Manage database connections and transactions
  - Configure WAL mode and performance settings
  - Execute batched commands efficiently
  - Handle database schema and migrations

### `internal/metrics/`
- **Purpose**: Prometheus metrics
- **Responsibilities**:
  - Define and expose application metrics
  - Provide instrumentation hooks for other components
  - Track queue depths, batch sizes, write latency, command metrics, etc.

### `internal/generated/`
- **Purpose**: Generated protobuf code
- **Responsibilities**:
  - Contains Go code generated from protobuf definitions
  - Created by `make generate`
  - Not committed to version control (in .gitignore)

## Key Design Decisions

### 1. Command-Based Architecture
- All database mutations are `Command` objects implementing `Execute(ctx, tx)`
- The SQLite Writer is a generic command executor
- New commands (PurgeLogs, VacuumDatabase, etc.) require no changes to the writer
- Commands are immutable and transaction-safe

### 2. OTLP Type Isolation
- OTLP protobuf types are only imported in `internal/otlp/`
- All other packages work with internal domain models
- Enables independent evolution of internal models
- Makes testing easier with mock data

### 3. Single SQLite Writer
- Only one goroutine writes to SQLite
- Prevents concurrent write conflicts
- Eliminates database lock contention
- Simplifies transaction management

### 4. Bounded Queues
- All queues have configurable capacities
- `Send()` blocks when queue is full
- Provides natural backpressure
- Prevents memory exhaustion

### 5. Batched Transactions
- Multiple commands grouped into single transactions
- Configurable batch size and flush interval
- Reduces commit overhead
- Improves write throughput

### 6. WAL Mode
- SQLite WAL (Write-Ahead Logging) mode enabled
- Allows concurrent reads while writing
- Improves performance for write-heavy workloads
- Better for high-concurrency scenarios

### 7. Dependency Injection
- Command factory is injected into the batcher (not hardcoded)
- Enables testing with mock commands
- Keeps the batcher SQLite-agnostic
- No global state

### 8. Object Pooling & Zero-Allocation Hot Path
- `LogRecord` objects are reused via `sync.Pool` (`GetRecord()`/`PutRecord()`)
- TraceID/SpanID use fixed-size arrays (`[16]byte`/`[8]byte`) instead of slices
- Attributes use `[]Attribute` inline struct slice instead of `map[string]AttributeValue`
- Result: 0 allocations per record in the mapper, eliminating GC pressure

## Build Targets

| Target | Description |
|--------|-------------|
| `make generate` | Generate protobuf and gRPC code |
| `make build` | Build the application binary |
| `make test` | Run all tests |
| `make lint` | Run linter |
| `make fmt` | Format code |
| `make run` | Build and run the application |
| `make clean` | Clean build artifacts |
| `make deps` | Install dependencies |

## Configuration Files

| File | Purpose |
|------|---------|
| `config.example.yaml` | Example configuration with all options |
| `.golangci.yml` | GolangCI-Lint configuration |
| `buf.yaml` | buf configuration for protobuf |
| `buf.gen.yaml` | buf generation configuration |
| `Dockerfile` | Docker build configuration |
| `docker-compose.yml` | Docker Compose configuration |
| `prometheus.yml` | Prometheus scrape configuration |

## Database Files

| File | Purpose |
|------|---------|
| `migrations/001_initial_schema.sql` | Initial database schema |
| `migrations/002_add_search_indexes.sql` | Search indexes and FTS |

## Documentation Files

| File | Purpose |
|------|---------|
| `README.md` | Project overview and usage |
| `docs/architecture.md` | Detailed architecture documentation |
| `PROJECT_STRUCTURE.md` | This file - project structure summary |
| `LICENSE` | Apache License 2.0 |

## File Counts

- **Go source files**: 22
- **Protobuf files**: 4
- **Configuration files**: 9
- **Documentation files**: 5
- **Migration files**: 4
- **Script files**: 2
- **Benchmark / loadtest files**: 5
- **Total files**: 50+

## Dependencies

### Direct Dependencies (in go.mod)
- `github.com/prometheus/client_golang` - Prometheus metrics
- `google.golang.org/grpc` - gRPC framework
- `google.golang.org/protobuf` - Protocol Buffers
- `modernc.org/sqlite` - SQLite driver

### Development Dependencies
- `protoc` - Protocol Buffers compiler
- `protoc-gen-go` - Go protobuf generator
- `protoc-gen-go-grpc` - Go gRPC generator
- `golangci-lint` - Linter

## Future Extensions

The command architecture supports easy extension for:

1. **Maintenance Commands**: PurgeLogs, VacuumDatabase, OptimizeDatabase, etc.
2. **Additional Protocols**: HTTP/JSON OTLP, legacy protocols
3. **Search API**: HTTP API for querying logs
4. **Additional Storage Backends**: PostgreSQL, MySQL, etc.
5. **Enhanced Metrics**: Per-command-type observability
6. **Authentication**: gRPC authentication and authorization
7. **TLS**: Secure communication
8. **Log Retention**: Automatic log rotation and cleanup
9. **Horizontal Scaling**: Multiple collector instances

## Verification

To verify the project structure is complete:

```bash
# Check all expected files exist
find . -type f -not -path './.git/*' | sort

# Check Go module
cat go.mod

# Check Makefile targets
make help  # If help target exists

# Check protobuf files
ls -la api/otlp/opentelemetry/proto/*/v1/*.proto

# Check internal packages
ls -la internal/*/
```

The project now uses a command-based architecture that will support future maintenance commands without modifying the queue or the writer.

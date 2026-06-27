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
│   │   └── batcher.go        # Batch building logic
│   ├── config/
│   │   └── config.go         # Configuration management
│   ├── generated/            # Generated protobuf code (created by make generate)
│   ├── ingest/
│   │   └── queue.go          # Bounded queue abstractions
│   ├── metrics/
│   │   └── metrics.go        # Prometheus metrics
│   ├── model/
│   │   ├── logrecord.go      # LogRecord domain model
│   │   ├── resource.go       # Resource domain model
│   │   └── value.go          # AttributeValue domain model
│   ├── otlp/
│   │   ├── mapper.go         # OTLP to internal model mapper
│   │   └── server.go         # gRPC server implementation
│   └── storage/
│       ├── sqlite/
│       │   └── writer.go      # SQLite writer implementation
│       └── storage.go        # Storage interfaces
├── LICENSE                   # Apache License 2.0
├── Makefile                  # Build targets
├── migrations/
│   ├── 001_initial_schema.sql    # Initial database schema
│   └── 002_add_search_indexes.sql # Search indexes migration
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
- **Purpose**: Bounded queue abstractions
- **Responsibilities**:
  - Provide `IngressQueue` interface for individual log records
  - Provide `BatchQueue` interface for batches of log records
  - Implement bounded channel-based queues
  - Provide backpressure through blocking sends

### `internal/batcher/`
- **Purpose**: Batch building logic
- **Responsibilities**:
  - Collect individual log records from ingress queue
  - Group records into batches based on size
  - Send batches to batch queue
  - Manage batch timing and flushing

### `internal/storage/`
- **Purpose**: Storage abstractions
- **Responsibilities**:
  - Define `LogStorage` interface
  - Define `LogWriter` interface for writing
  - Define `LogReader` interface for reading (future)
  - Provide abstract access to storage backends

### `internal/storage/sqlite/`
- **Purpose**: SQLite storage implementation
- **Responsibilities**:
  - Implement SQLite writer with single goroutine
  - Manage database connections and transactions
  - Configure WAL mode and performance settings
  - Execute batched inserts efficiently
  - Handle database schema and migrations

### `internal/metrics/`
- **Purpose**: Prometheus metrics
- **Responsibilities**:
  - Define and expose application metrics
  - Provide instrumentation hooks for other components
  - Track queue depths, batch sizes, write latency, etc.

### `internal/generated/`
- **Purpose**: Generated protobuf code
- **Responsibilities**:
  - Contains Go code generated from protobuf definitions
  - Created by `make generate`
  - Not committed to version control (in .gitignore)

## Key Design Decisions

### 1. OTLP Type Isolation
- OTLP protobuf types are only imported in `internal/otlp/`
- All other packages work with internal domain models
- Enables independent evolution of internal models
- Makes testing easier with mock data

### 2. Single SQLite Writer
- Only one goroutine writes to SQLite
- Prevents concurrent write conflicts
- Eliminates database lock contention
- Simplifies transaction management

### 3. Bounded Queues
- All queues have configurable capacities
- `Send()` blocks when queue is full
- Provides natural backpressure
- Prevents memory exhaustion

### 4. Batched Transactions
- Multiple batches grouped into single transactions
- Configurable batch size and flush interval
- Reduces commit overhead
- Improves write throughput

### 5. WAL Mode
- SQLite WAL (Write-Ahead Logging) mode enabled
- Allows concurrent reads while writing
- Improves performance for write-heavy workloads
- Better for high-concurrency scenarios

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

- **Go source files**: 12
- **Protobuf files**: 4
- **Configuration files**: 8
- **Documentation files**: 4
- **Migration files**: 2
- **Script files**: 1
- **Total files**: 31+

## Dependencies

### Direct Dependencies (in go.mod)
- `github.com/prometheus/client_golang` - Prometheus metrics
- `github.com/spf13/cobra` - CLI command handling
- `github.com/spf13/viper` - Configuration management
- `go.uber.org/zap` - Structured logging
- `google.golang.org/grpc` - gRPC framework
- `google.golang.org/protobuf` - Protocol Buffers
- `modernc.org/sqlite` - SQLite driver

### Development Dependencies
- `protoc` - Protocol Buffers compiler
- `protoc-gen-go` - Go protobuf generator
- `protoc-gen-go-grpc` - Go gRPC generator
- `golangci-lint` - Linter

## Future Extensions

The architecture supports easy extension for:

1. **Additional Protocols**: HTTP/JSON OTLP, legacy protocols
2. **Search API**: HTTP API for querying logs
3. **Additional Storage Backends**: PostgreSQL, MySQL, etc.
4. **Enhanced Metrics**: More detailed observability
5. **Authentication**: gRPC authentication and authorization
6. **TLS**: Secure communication
7. **Log Retention**: Automatic log rotation and cleanup
8. **Horizontal Scaling**: Multiple collector instances

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

The project skeleton is now complete and ready for implementation!
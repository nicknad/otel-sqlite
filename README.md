# OTLP SQLite Collector

A high-performance OpenTelemetry log collector that receives OTLP logs over gRPC and stores them in SQLite using a command-based architecture for efficient, extensible persistence.

## Features

- **OTLP gRPC Support**: Receives OpenTelemetry logs via gRPC protocol
- **Bounded Ingestion Pipeline**: Prevents memory exhaustion with bounded queues at every stage
- **Command-Based Architecture**: All database mutations are `Command` objects — the SQLite Writer is a generic command executor
- **Single SQLite Writer**: Dedicated goroutine for all database writes, no concurrent write conflicts
- **Batched Transactions**: Efficient bulk inserts with configurable batch sizes and flush intervals
- **Prepared Statements**: Hot-path insert SQL compiled once at startup, reused across all transactions
- **WAL Mode**: Concurrent read/write access via Write-Ahead Logging
- **Prometheus Metrics**: Comprehensive observability with built-in metrics for ingestion, batching, commands, storage, and maintenance
- **Maintenance Framework**: Pluggable, schedule-based tasks for retention, WAL checkpoint, optimize, and vacuum
- **Backpressure**: Graceful handling of load spikes without data loss at every pipeline stage
- **Resource Deduplication**: Deterministic resource IDs (SHA-256) so identical resources collapse to single SQLite rows
- **OTLP Type Isolation**: Protobuf types confined to the transport layer; internal packages depend only on domain models
- **Dependency Injection**: Command factory injected into the batcher, keeping it SQLite-agnostic and testable
- **YAML Configuration**: Optional config file support via `CONFIG_FILE` environment variable
- **Health Check**: HTTP `/health` endpoint on the metrics port
- **Load Testing**: Built-in load generator (`cmd/loadtest`) for throughput baseline measurements

## Architecture

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation.

### Pipeline Flow

```
gRPC OTLP Receiver (otlp.Server)
    │
    ▼
OTLP Mapper (converts protobuf → internal domain models)
    │
    ▼
Ingress Queue (bounded channel, per-record, backpressure)
    │
    ▼
Batcher (collects records, groups into batches, wraps in WriteBatchCommand)
    │
    ▼
Command Queue (bounded, FIFO, channel-based Command objects)
    │
    ▼
SQLite Writer (single goroutine, executes Command.Execute within transactions)
    │
    ▼
SQLite Database (WAL mode, prepared statements, indexed)
```

### Command Architecture

All database mutations implement the `Command` interface:

```go
type Command interface {
    Execute(ctx context.Context, tx *sql.Tx) error
}
```

For operations that cannot run inside a transaction (e.g., `VACUUM`), the `NonTransactionalCommand` interface is used:

```go
type NonTransactionalCommand interface {
    Command
    ExecuteNonTransactional(ctx context.Context, db *sql.DB) error
}
```

The SQLite Writer is the sole `CommandExecutor`. New commands (retention, checkpoint, vacuum, optimize, etc.) require no changes to the writer — they only need to implement the `Command` interface.

### Maintenance Framework

Database maintenance is handled by a generic, pluggable worker (`internal/maintenance/`) that schedules and executes `MaintenanceTask` implementations. Tasks have no direct access to SQLite; all side effects are produced by submitting `Command` objects via `CommandSubmitter`.

| Task | Command | Default Schedule | Description |
|------|---------|------------------|-------------|
| Retention | PurgeLogsCommand | 24h | Deletes log records older than a configurable threshold (default 30 days) |
| Checkpoint | CheckpointCommand | 24h | Runs `PRAGMA wal_checkpoint` to bound WAL file size |
| Optimize | OptimizeCommand | 24h | Runs `PRAGMA optimize` to update query planner statistics |
| Vacuum | VacuumCommand | 7d (disabled) | Rebuilds the database file to reclaim disk space |

## Quick Start

### Prerequisites

- Go 1.24+
- protoc (Protocol Buffers compiler)
- protoc-gen-go
- protoc-gen-go-grpc

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://codeberg.org/nicknad/otel-sqlite.git
   cd otel-sqlite
   ```

2. **Install dependencies**:
   ```bash
   go mod download
   ```

3. **Generate protobuf code**:
   ```bash
   make generate
   ```

4. **Build the collector**:
   ```bash
   make build
   ```

5. **Run the collector**:
   ```bash
   make run
   ```

Or directly:
```bash
./bin/otel-collector
```

### Docker

1. **Build the image**:
   ```bash
   docker build -t otel-sqlite-collector .
   ```

2. **Run the container**:
   ```bash
   docker run -p 4317:4317 -p 9090:9090 -v ./data:/var/lib/otel-collector otel-sqlite-collector
   ```

3. **Using docker-compose** (includes Prometheus and Grafana):
   ```bash
   docker-compose up -d
   ```

4. **Load test profile** (collector only, throughput tuning):
   ```bash
   docker compose -f docker-compose.loadtest.yml up -d
   ```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDRESS` | `:4317` | gRPC server listen address |
| `SQLITE_PATH` | `otel-logs.db` | Path to SQLite database file |
| `INGRESS_QUEUE_CAPACITY` | `10000` | Maximum ingress queue size |
| `BATCH_QUEUE_CAPACITY` | `1000` | Maximum command queue size |
| `BATCH_SIZE` | `100` | Number of log records per batch |
| `FLUSH_INTERVAL` | `5s` | Maximum time between batch flushes |
| `METRICS_ADDRESS` | `:9090` | Prometheus metrics server address |
| `SHUTDOWN_TIMEOUT` | `30s` | Graceful shutdown timeout |
| `CONFIG_FILE` | — | Path to YAML configuration file |
| `MAINTENANCE_ENABLED` | `true` | Master switch for the maintenance worker |
| `MAINTENANCE_CHECK_INTERVAL` | `1h` | Worker evaluation interval |
| `RETENTION_ENABLED` | `true` | Enable log retention purge |
| `RETENTION_KEEP_LOGS` | `720h` (30d) | Delete logs older than this |
| `RETENTION_CLEANUP_INTERVAL` | `24h` | How often to run retention |
| `RETENTION_DELETE_BATCH_SIZE` | `10000` | Max records per delete batch |
| `CHECKPOINT_ENABLED` | `true` | Enable WAL checkpoint |
| `CHECKPOINT_INTERVAL` | `24h` | How often to checkpoint |
| `CHECKPOINT_MODE` | `PASSIVE` | Checkpoint mode (PASSIVE, FULL, RESTART, TRUNCATE) |
| `OPTIMIZE_ENABLED` | `true` | Enable PRAGMA optimize |
| `OPTIMIZE_INTERVAL` | `24h` | How often to run optimize |
| `VACUUM_ENABLED` | `false` | Enable VACUUM (requires exclusive lock) |
| `VACUUM_INTERVAL` | `168h` (7d) | How often to vacuum |

### YAML Configuration File

A [`config.example.yaml`](config.example.yaml) file is provided with all options documented. Set `CONFIG_FILE` to use it:

```bash
export CONFIG_FILE=/path/to/config.yaml
./bin/otel-collector
```

Environment variables override file values, which override defaults.

## Usage

### Sending Logs

The collector implements the OpenTelemetry OTLP gRPC protocol. You can send logs using any OTLP-compatible client.

#### Example with OpenTelemetry SDK

```go
import (
    "context"
    "log"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
    "go.opentelemetry.io/otel/sdk/log"
)

func main() {
    // Create OTLP gRPC exporter
    exporter, err := otlploggrpc.New(context.Background(),
        otlploggrpc.WithEndpoint("localhost:4317"),
        otlploggrpc.WithInsecure(),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Create logger provider
    lp := log.NewLoggerProvider(
        log.WithProcessor(log.NewBatchProcessor(exporter)),
    )

    // Set global logger provider
    otel.SetLoggerProvider(lp)

    // Use the logger
    logger := otel.Logger("my-service")
    logger.Info(context.Background(), "Hello, World!")
}
```

### Querying Logs

The collector stores logs in SQLite, so you can query them directly:

```bash
# Connect to the database
sqlite3 otel-logs.db

# Query recent logs (via the logs view)
SELECT * FROM logs ORDER BY timestamp_ns DESC LIMIT 10;

# Query logs by service
SELECT * FROM logs
WHERE service_name = 'my-service'
ORDER BY timestamp_ns DESC;

# Query logs by severity
SELECT * FROM logs
WHERE severity_number >= 17  -- ERROR and above
ORDER BY timestamp_ns DESC;

# Query logs by trace ID
SELECT * FROM logs
WHERE trace_id = x'0102030405060708090a0b0c0d0e0f10';

# Full-text search on log body (contentless FTS5 index)
SELECT * FROM logs WHERE id IN (
    SELECT rowid FROM logs_fts WHERE logs_fts MATCH 'error'
) ORDER BY timestamp_ns DESC;
```

### Load Testing

A built-in load generator simulates multiple concurrent gRPC clients:

```bash
# Run a 30-second burst test (default)
make loadtest

# Tune parameters
make loadtest-run LOADTEST_CLIENTS=64 LOADTEST_RECORDS=2000 LOADTEST_DURATION=60s

# Full lifecycle (up → run → down)
make loadtest
```

See [docs/loadtest-baseline.md](docs/loadtest-baseline.md) for measured throughput baselines and optimization guidance.

## Observability

### Metrics

The collector exposes Prometheus metrics on `METRICS_ADDRESS` (default `:9090`).

#### Ingestion Metrics

- `otel_collector_ingest_logs_received_total` — Total logs received
- `otel_collector_ingest_ingress_queue_depth` — Current ingress queue depth
- `otel_collector_ingest_batch_queue_depth` — Current command queue depth

#### Batching Metrics

- `otel_collector_batcher_batches_created_total` — Total batches created
- `otel_collector_batcher_batch_size` — Batch size histogram

#### Command Execution Metrics

- `otel_collector_command_queue_depth` — Current command queue depth
- `otel_collector_command_execution_duration_seconds` — Command execution batch duration
- `otel_collector_command_executed_total` — Total commands executed
- `otel_collector_command_failures_total` — Total command execution failures

#### Storage Metrics

- `otel_collector_storage_logs_written_total` — Total logs written
- `otel_collector_storage_batches_written_total` — Total batches written
- `otel_collector_storage_write_latency_seconds` — Write latency histogram
- `otel_collector_storage_write_errors_total` — Total write errors
- `otel_collector_storage_active_resources` — Number of active resources
- `otel_collector_storage_total_resources` — Total unique resources

#### Maintenance Metrics

- `otel_collector_maintenance_runs_total` — Total maintenance worker evaluation cycles
- `otel_collector_maintenance_task_runs_total` — Task executions (labeled by task)
- `otel_collector_maintenance_failures_total` — Task failures (labeled by task)
- `otel_collector_maintenance_duration_seconds` — Task execution duration (labeled by task)
- `otel_collector_maintenance_last_run_timestamp` — Last successful run time (labeled by task)

### Health Checks

- **gRPC Health Check**: Available at the gRPC endpoint
- **HTTP Health Check**: `GET /health` on the metrics port

### Logging

The collector uses structured logging for operational events. Logs are written to stdout.

## Project Structure

```
.
├── api/
│   └── otlp/                    # OTLP protobuf definitions
├── cmd/
│   ├── collector/               # Main application entry point
│   └── loadtest/                # Load testing tool
├── internal/
│   ├── batcher/                 # Batch building + command construction
│   ├── config/                  # Configuration management (env, yaml)
│   ├── generated/               # Generated protobuf code
│   ├── ingest/                  # Bounded queue abstractions (ingress)
│   ├── maintenance/             # Generic maintenance framework
│   │   └── tasks/               # Built-in task implementations
│   ├── metrics/                 # Prometheus metrics
│   ├── model/                   # Internal domain models
│   ├── otlp/                    # OTLP transport layer and mapper
│   └── storage/                 # Storage abstractions
│       ├── sqlite/              # SQLite storage implementation
│       └── command.go           # Command + CommandExecutor interfaces
├── docs/                        # Documentation
├── migrations/                  # Database migration files
├── config.example.yaml          # Example YAML configuration
├── docker-compose.yml           # Docker Compose (collector + Prometheus + Grafana)
├── docker-compose.loadtest.yml  # Docker Compose load test profile
├── prometheus.yml               # Prometheus scrape configuration
├── Makefile                     # Build targets
├── go.mod                       # Go module definition
└── Dockerfile                   # Docker build configuration
```

## Database Schema

### Tables

- **log_resource**: Resource metadata (service.name, host.name, schema_url, etc.) with deterministic IDs for deduplication
- **log_event**: Log record metadata (timestamp, severity, trace/span IDs, body, scope, etc.)
- **log_attr**: Log record attributes (key-value pairs with typed values)

### Views

- **logs**: Read-side view joining `log_event` with `log_resource`, exposing `service_name`, `host_name`, `schema_url`, and all event columns

### Full-Text Search

- **logs_fts**: Contentless FTS5 virtual table over the `logs` view, indexed on `body` and `service_name`. Rebuilt by the maintenance framework (not by triggers) to keep the write path free of FTS overhead.

### Indexes

- Primary keys on all tables
- Foreign keys for referential integrity
- Indexes on: `timestamp_ns`, `severity_number`, `trace_id`, `resource_id`, `body`, `event_name`, attribute keys
- Composite indexes: `(resource_id, timestamp_ns)`, `(trace_id, timestamp_ns)`
- Partial indexes on attribute values for filtered queries

### Migrations

Database migrations are stored in the `migrations/` directory:

| Migration | Description |
|-----------|-------------|
| `001_initial_schema.sql` | Initial tables (log_resource, log_event, log_attr), indexes, and migration tracking |
| `002_add_search_indexes.sql` | FTS5 index with triggers, additional attribute/event indexes |
| `003_logs_view_and_fts.sql` | Retires trigger-based FTS, creates `logs` view and contentless `logs_fts` index |

The collector applies the initial schema on startup via `CREATE TABLE IF NOT EXISTS` statements embedded in the SQLite Writer. Migration files are provided for schema documentation and manual upgrades.

## Performance Tuning

### Queue Sizes

- **Ingress Queue**: Controls memory usage for incoming logs
  - Larger values: better burst absorption, higher memory usage
  - Smaller values: lower memory usage, earlier backpressure to clients

- **Command Queue**: Controls memory usage for batched commands
  - Larger values: better burst absorption, higher memory usage
  - Smaller values: lower memory usage, earlier backpressure to batcher

### Batch Sizes

- **Batch Size**: Number of records per batch
  - Larger values: better write throughput, higher memory usage per batch
  - Smaller values: lower memory usage, more frequent writes

- **Flush Interval**: Maximum time between batch flushes
  - Shorter intervals: lower latency, more frequent writes
  - Longer intervals: better throughput, higher latency

### Transaction Caps

- **Max Transaction Records**: The writer splits command batches exceeding 5000 records into multiple transactions to keep commit latency bounded

### SQLite Configuration

- **WAL Mode**: Enabled by default for concurrent access
- **Synchronous**: Set to NORMAL for better performance
- **Autocheckpoint**: Configured at 1000 pages

### Measured Throughput

See [docs/loadtest-baseline.md](docs/loadtest-baseline.md) for detailed benchmarks. Key results:

| Scenario | Ingest (acknowledged) | Process (written to SQLite) |
|----------|----------------------:|---------------------------:|
| Burst (32 clients × 1000 rec, 30s) | ~278k rec/s | Queued (draining) |
| Sustained (32 clients × 150 rec, 60s) | ~4.7k rec/s | ~4.7k rec/s (flat queue) |

## Troubleshooting

### Common Issues

1. **gRPC connection refused**:
   - Check that the collector is running
   - Verify the listen address configuration
   - Check firewall settings

2. **Database permission errors**:
   - Ensure the database directory is writable
   - Check file permissions
   - Verify disk space

3. **High memory usage**:
   - Reduce queue capacities
   - Reduce batch sizes
   - Check for memory leaks

4. **Slow write performance**:
   - Increase batch sizes
   - Increase flush intervals
   - Check disk I/O performance
   - Consider reducing indexes during ingest

### Debug Mode

Enable debug logging by setting the `LOG_LEVEL` environment variable:

```bash
LOG_LEVEL=debug ./bin/otel-collector
```

## Development

### Building

```bash
# Build the application
make build

# Run the application
make run

# Clean build artifacts
make clean
```

### Testing

```bash
# Run all tests
make test

# Run tests with race detector
go test -race ./...

# Run specific package tests
go test ./internal/otlp/...
```

### Code Generation

```bash
# Generate protobuf and gRPC code
make generate
```

### Linting and Formatting

```bash
# Run linter
make lint

# Format code
make fmt
```

### Adding a New Maintenance Command

1. Implement the `storage.Command` interface in `internal/storage/sqlite/`
2. Create a task in `internal/maintenance/tasks/` implementing `maintenance.MaintenanceTask`
3. Register the task in `cmd/collector/main.go`
4. Add configuration fields to `internal/maintenance/config.go` and `config.example.yaml`

No changes to the SQLite Writer, Command Queue, or maintenance worker are needed.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

### Code Style

- Use `gofmt` for formatting
- Follow Go conventions
- Add appropriate comments
- Include tests for new functionality
- Keep commits atomic and well-described

### Pull Request Checklist

- [ ] Code compiles successfully
- [ ] All tests pass
- [ ] Linting passes
- [ ] Code is properly formatted
- [ ] Documentation is updated
- [ ] Migration files are included (if schema changes)

## License

Copyright 2024 nnadolski

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Acknowledgments

- [OpenTelemetry](https://opentelemetry.io/) for the OTLP protocol
- [gRPC](https://grpc.io/) for the RPC framework
- [SQLite](https://sqlite.org/) for the embedded database
- [Prometheus](https://prometheus.io/) for metrics collection
- [ModernC SQLite](https://modernc.org/sqlite) for the pure-Go SQLite driver

## Related Projects

- [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector)
- [SQLite](https://www.sqlite.org/index.html)
- [gRPC-Go](https://github.com/grpc/grpc-go)
- [Prometheus Client Golang](https://github.com/prometheus/client_golang)

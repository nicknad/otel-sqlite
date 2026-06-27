# OTLP SQLite Collector

A high-performance OpenTelemetry log collector that receives OTLP logs over gRPC and stores them in SQLite for efficient querying.

## Features

- **OTLP gRPC Support**: Receives OpenTelemetry logs via gRPC protocol
- **Bounded Ingestion Pipeline**: Prevents memory exhaustion with bounded queues
- **Single SQLite Writer**: Dedicated goroutine for all database writes
- **Batched Transactions**: Efficient bulk inserts with configurable batch sizes
- **WAL Mode**: Concurrent read/write access to SQLite database
- **Prometheus Metrics**: Comprehensive observability with built-in metrics
- **Backpressure**: Graceful handling of load spikes without data loss
- **Clean Architecture**: OTLP types isolated to transport layer

## Architecture

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation.

### Pipeline Flow

```
gRPC OTLP Receiver
    ↓
OTLP Mapper (converts protobuf → internal models)
    ↓
Ingress Queue (bounded, backpressure)
    ↓
Batch Builder (groups records into batches)
    ↓
Batch Queue (bounded, backpressure)
    ↓
SQLite Writer (single goroutine, batched transactions)
    ↓
SQLite Database (WAL mode, indexed)
```

## Quick Start

### Prerequisites

- Go 1.24+
- protoc (Protocol Buffers compiler)
- protoc-gen-go
- protoc-gen-go-grpc

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/nnadolski/otel-sqlite.git
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

3. **Using docker-compose**:
   ```bash
   docker-compose up -d
   ```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDRESS` | `:4317` | gRPC server listen address |
| `SQLITE_PATH` | `otel-logs.db` | Path to SQLite database file |
| `INGRESS_QUEUE_CAPACITY` | `10000` | Maximum ingress queue size |
| `BATCH_QUEUE_CAPACITY` | `1000` | Maximum batch queue size |
| `BATCH_SIZE` | `100` | Number of records per batch |
| `FLUSH_INTERVAL` | `5s` | Maximum time between batch flushes |
| `METRICS_ADDRESS` | `:9090` | Prometheus metrics server address |
| `SHUTDOWN_TIMEOUT` | `30s` | Graceful shutdown timeout |

### Reference Configuration

A [`config.example.yaml`](config.example.yaml) file is provided as a reference for customizing the collector via environment variables. The collector itself is configured entirely through environment variables (see table above); the YAML file is for documentation only.

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

# Query recent logs
SELECT * FROM log_event ORDER BY timestamp_ns DESC LIMIT 10;

# Query logs by service
SELECT * FROM log_event 
WHERE resource_id IN (SELECT id FROM log_resource WHERE service_name = 'my-service')
ORDER BY timestamp_ns DESC;

# Query logs by severity
SELECT * FROM log_event 
WHERE severity_number >= 17  -- ERROR and above
ORDER BY timestamp_ns DESC;

# Query logs by trace ID
SELECT * FROM log_event 
WHERE trace_id = x'0102030405060708090a0b0c0d0e0f10';
```

## Observability

### Metrics

The collector exposes Prometheus metrics on the configured metrics address (default `:9090`):

- `otel_collector_ingest_logs_received_total` - Total logs received
- `otel_collector_ingest_ingress_queue_depth` - Current ingress queue depth
- `otel_collector_ingest_batch_queue_depth` - Current batch queue depth
- `otel_collector_batcher_batches_created_total` - Total batches created
- `otel_collector_batcher_batch_size` - Batch size histogram
- `otel_collector_storage_logs_written_total` - Total logs written
- `otel_collector_storage_batches_written_total` - Total batches written
- `otel_collector_storage_write_latency_seconds` - Write latency histogram
- `otel_collector_storage_write_errors_total` - Total write errors

### Health Checks

- **gRPC Health Check**: Available at the gRPC endpoint
- **HTTP Health Check**: `GET /health` on the metrics port

### Logging

The collector uses structured logging for operational events. Logs are written to stdout.

## Project Structure

```
.
├── cmd/
│   └── collector/           # Main application entry point
├── internal/
│   ├── config/             # Configuration management
│   ├── model/              # Internal domain models
│   ├── otlp/               # OTLP transport layer and mapper
│   ├── ingest/             # Bounded queue abstractions
│   ├── batcher/            # Batch building logic
│   ├── storage/            # Storage abstractions
│   │   └── sqlite/          # SQLite storage implementation
│   ├── metrics/            # Prometheus metrics
│   └── generated/          # Generated protobuf code
├── api/
│   └── otlp/               # OTLP protobuf definitions
├── migrations/             # Database migration files
├── scripts/                # Utility scripts
├── docs/                   # Documentation
├── Makefile                # Build targets
├── go.mod                  # Go module definition
├── Dockerfile              # Docker build configuration
└── docker-compose.yml      # Docker Compose configuration
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

# Or manually
protoc --go_out=internal/generated --go_opt=paths=source_relative \
    --go-grpc_out=internal/generated --go-grpc_opt=paths=source_relative \
    -I=api/otlp api/otlp/opentelemetry/proto/collector/logs/v1/logs_service.proto
```

### Linting and Formatting

```bash
# Run linter
make lint

# Format code
make fmt
```

## Database Schema

### Tables

- **log_resource**: Resource metadata (service.name, host.name, schema_url, etc.)
- **log_event**: Log record metadata (timestamp, severity, trace/span IDs, body, etc.)
- **log_attr**: Log record attributes (key-value pairs)

### Indexes

- Primary keys on all tables
- Foreign keys for referential integrity
- Indexes on: timestamp, severity, trace_id, resource_id, attribute keys
- Composite indexes for common query patterns

### Migrations

Database migrations are stored in the `migrations/` directory. The collector automatically applies migrations on startup.

## Performance Tuning

### Queue Sizes

- **Ingress Queue**: Controls memory usage for incoming logs
  - Larger values: Better throughput, higher memory usage
  - Smaller values: Lower memory usage, potential backpressure

- **Batch Queue**: Controls memory usage for batched logs
  - Larger values: Better throughput, higher memory usage
  - Smaller values: Lower memory usage, potential backpressure

### Batch Sizes

- **Batch Size**: Number of records per batch
  - Larger values: Better write throughput, higher memory usage per batch
  - Smaller values: Lower memory usage, more frequent writes

- **Flush Interval**: Maximum time between batch flushes
  - Shorter intervals: Lower latency, more frequent writes
  - Longer intervals: Better throughput, higher latency

### SQLite Configuration

- **WAL Mode**: Enabled by default for concurrent access
- **Synchronous**: Set to NORMAL for better performance
- **Autocheckpoint**: Configured for optimal performance

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
   - Consider using a faster storage device

### Debug Mode

Enable debug logging by setting the `LOG_LEVEL` environment variable:

```bash
LOG_LEVEL=debug ./bin/otel-collector
```

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

## Related Projects

- [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector)
- [SQLite](https://www.sqlite.org/index.html)
- [gRPC-Go](https://github.com/grpc/grpc-go)
- [Prometheus Client Golang](https://github.com/prometheus/client_golang)

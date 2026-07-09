# OTLP SQLite Collector

A high-performance OpenTelemetry log collector that receives OTLP logs over gRPC and stores them in SQLite using a command-based architecture for efficient, extensible persistence.

## Features

- **OTLP gRPC Support**: Receives OpenTelemetry logs via gRPC protocol
- **Bounded Ingestion Pipeline**: Prevents memory exhaustion with bounded queues at every stage
- **Single SQLite Writer**: Dedicated goroutine for all database writes, no concurrent write conflicts
- **Prometheus Metrics**: Comprehensive observability with built-in metrics for ingestion, batching, commands, storage, and maintenance
- **Backpressure**: Graceful handling of load spikes without data loss at every pipeline stage
- **Batch-Level Ingress**: Batches sent through the ingress queue instead of individual records — 250× fewer channel operations
- **Production-Tuned SQLite**: WAL mode, 64MB page cache, 256MB mmap I/O, temp_store=MEMORY, journal size limit
- **OTLP Type Isolation**: Protobuf types confined to the transport layer; internal packages depend only on domain models
- **Notification Pipeline**: Rule-based alerting with sliding-window aggregation, threshold counting, alert state machine (pending→firing→resolved), and dead-letter queue
- **Pluggable Notifiers**: HTTP webhook and log-based notifiers; add your own via the `Notifier` interface
- **YAML Configuration**: Optional config file support via `CONFIG_FILE` environment variable

## Architecture

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation.

### Pipeline Flow

```
gRPC OTLP Receiver (otlp.Server)
    │
    ▼
OTLP Mapper (converts protobuf → internal domain models, uses sync.Pool)
    │
    ▼
Ingress Queue (bounded channel, batch-based, backpressure)
    │
    ▼
Batcher (collects batches, groups into larger batches, wraps in WriteBatchCommand)
    │
    ▼
Command Queue (bounded, FIFO, channel-based Command objects)
    │
    ▼
SQLite Writer (single goroutine, executes Command.Execute within transactions,
    │          returns records to pool after write)
    ▼
SQLite Database (WAL mode, 64MB cache, 256MB mmap, prepared statements, minimized indexes)
    │
    ▼ (optional, if notification.enabled)
Notification Worker
    │
    ├── Rule Engine (severity threshold, regex filters)
    ├── Alert Pipeline (ERROR → time window → counter → state → alert)
    ├── Alert Store (bbolt: pending/firing/resolved per rule×resource)
    ├── Notification State Store (bbolt: cooldown, retry tracking, DLQ)
    ├── HTTP Webhook Notifier (POSTs Alert JSON)
    ├── Log Notifier (log.Printf)
    └── Dead-Letter Queue (bbolt, persistent)
```

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

> **Legacy support:** The older `BATCH_SIZE` and `FLUSH_INTERVAL` env vars are
> still accepted. When set, they apply to **both** the batcher and writer
> unless the specific `BATCHER_*` / `WRITER_*` variables are provided.

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDRESS` | `:4317` | gRPC server listen address |
| `SQLITE_PATH` | `otel-logs.db` | Path to SQLite database file |
| `INGRESS_QUEUE_CAPACITY` | `10000` | Maximum ingress queue size |
| `BATCH_QUEUE_CAPACITY` | `1000` | Maximum command queue size |
| `BATCHER_BATCH_SIZE` | `250` | Number of log records per batch (batcher) |
| `BATCHER_FLUSH_INTERVAL` | `5s` | Maximum time between batch flushes |
| `BATCHER_ERROR_SEVERITY_THRESHOLD` | `ERROR` | Minimum severity to forward to notification worker |
| `WRITER_BATCH_SIZE` | `100` | Number of commands per transaction (writer) |
| `WRITER_FLUSH_INTERVAL` | `5s` | Maximum time between transaction flushes |
| `WRITER_MAX_TRANSACTION_RECORDS` | `5000` | Maximum records per SQLite transaction |
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
| `NOTIFICATION_ENABLED` | `false` | Enable notification pipeline |
| `NOTIFICATION_EVENT_QUEUE_DEPTH` | `1000` | Notification event queue capacity |
| `NOTIFICATION_STORE_PATH` | `notify-state.db` | Path to notification delivery state store (bbolt) |
| `NOTIFICATION_ALERT_STORE_PATH` | `alert-state.db` | Path to alert state store (bbolt) |
| `NOTIFICATION_RETRY_INTERVAL` | `30s` | Retry scan interval |
| `NOTIFICATION_GC_INTERVAL` | `5m` | Resolved alert garbage-collection interval |
| `NOTIFICATION_ALERT_IDLE_TTL` | `24h` | Max idle time for Pending/Firing alerts before GC eviction |
| `NOTIFICATION_RESOLVED_ALERT_RETENTION` | `24h` | How long Resolved alerts are retained before GC |
| `NOTIFICATION_DLQ_RETENTION` | `720h` (30d) | Max age of DLQ entries before purge |
| `NOTIFICATION_BBOLT_COMPACTION_ENABLED` | `false` | Enable periodic bbolt compaction |
| `NOTIFICATION_BBOLT_COMPACTION_INTERVAL` | `24h` | Interval between bbolt compactions |

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


### Notification Pipeline

The collector includes an optional notification pipeline that sends alerts when error-level log records are ingested.

**Alert pipeline flow:**
```
ERROR event
  → Time Window (sliding window per rule: count events within alert_window)
    → Counter (threshold check: count >= alert_threshold?)
      → State Machine (pending → firing → resolved)
        → Alert (persisted per rule×resource pair)
          → Notifier (HTTP webhook or log)
            → Retry Loop (exponential backoff) → Dead-Letter Queue
```

**Alert state machine:**
- **Pending**: events are being counted but haven't reached the threshold yet
- **Firing**: threshold exceeded — notifications are delivered
- **Resolved**: silence for `alert_resolve_window` — a resolution notification is sent

Each rule×resource pair creates one Alert. Multiple error messages from the same resource under the same rule count toward a single alert.

**Features:**
- **Rule-based matching**: filter by severity, resource ID (regex), body (regex), attributes
- **Sliding-window aggregation**: count events within a configurable time window per rule
- **Threshold-based alerting**: fire when count reaches `alert_threshold`; resolve after `alert_resolve_window` of silence
- **Cooldown**: minimum interval between notifications for the same alert
- **Retry with backoff**: exponential backoff (base × 2^attempt), capped at 2^10
- **Dead-letter queue**: persistent storage for deliveries that exhausted retries or received 4xx responses
- **Garbage collection**: resolved alerts older than `resolved_alert_retention` (default 24h) are automatically cleaned up; idle Pending/Firing alerts evicted after `alert_idle_ttl`
- **Graceful shutdown**: drains buffered events before stopping
- **Metrics**: events received, matched, delivered, failed, dead-lettered; queue depth

**Configuration example** (`config.yaml`):
```yaml
notification:
  enabled: true
  event_queue_depth: 1000
  store_path: "notify-state.db"     # delivery state (cooldown, retries, DLQ)
  alert_store_path: "alert-state.db" # alert state (pending/firing/resolved)
  retry_interval: "30s"
  gc_interval: "5m"
  alert_idle_ttl: "24h"           # evict idle Pending/Firing alerts after this
  resolved_alert_retention: "24h"  # remove Resolved alerts after this
  dlq_retention: "720h"            # purge dead-letter entries after this
  notifiers:
    slack:
      type: "http"
      url: "https://hooks.slack.com/services/..."
      timeout: "10s"
    dev:
      type: "log"
  rules:
    - name: "production-errors"
      match_severity: "ERROR"
      resource_filter: ".*production.*"
      cooldown: "5m"
      max_retries: 3
      retry_backoff: "30s"
      alert_window: "5m"           # count events within 5-minute windows
      alert_threshold: 3           # fire after 3 events in one window
      alert_resolve_window: "15m"  # auto-resolve after 15m of silence
      destination: "slack"
    - name: "all-fatals"
      match_severity: "FATAL"
      alert_threshold: 1          # fire immediately on first Fatal
      destination: "dev"
```

**HTTP notifier**: sends JSON POST requests with the alert payload (id, rule_id, resource_id, status, severity, opened_at, updated_at, last_matched, count). Non-retryable errors (4xx) go straight to DLQ; retryable errors (5xx, network) follow the retry backoff.

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

#### Notification Metrics

- `otel_collector_notify_events_received_total` — Total events received by the notification worker
- `otel_collector_notify_events_matched_total` — Events that matched a rule (labeled by rule)
- `otel_collector_notify_events_delivered_total` — Alert notifications successfully delivered (labeled by destination)
- `otel_collector_notify_events_failed_total` — Alert notifications that failed delivery (labeled by destination)
- `otel_collector_notify_events_dead_lettered_total` — Alerts moved to dead-letter queue
- `otel_collector_notify_queue_depth` — Current notification event queue depth
- `otel_collector_notify_retry_queue_depth` — Current retry queue depth

### Health Checks

- **gRPC Health Check**: Available at the gRPC endpoint
- **HTTP Health Check**: `GET /health` on the metrics port


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
- Indexes on: `timestamp_ns` (time-range queries), `resource_id` (resource filtering), `event_id` on attributes
- Unused indexes removed (migration 004): `severity_number`, `trace_id`, `severity_text`, `body`, `event_name`, composite `(resource_id, timestamp_ns)`, composite `(trace_id, timestamp_ns)`, and all attribute value indexes — write performance is prioritized over read performance on non-critical query paths

### SQLite Performance Tuning

The writer applies these pragmas at startup for production-grade durability and performance:

| Pragma | Value | Purpose |
|--------|-------|---------|
| `journal_mode` | WAL | Crash-safe writes without blocking readers |
| `synchronous` | NORMAL | Safe in WAL mode; avoids per-transaction fsync |
| `cache_size` | -65536 (64 MB) | Large page cache for B-tree efficiency on established databases |
| `mmap_size` | 268435456 (256 MB) | Zero-copy page access via memory-mapped I/O |
| `temp_store` | MEMORY | Force temp tables/indexes into RAM |
| `journal_size_limit` | 67108864 (64 MB) | Cap WAL file growth; force checkpoint if exceeded |
| `wal_autocheckpoint` | 1000 pages | Auto-checkpoint after ~4-8 MB written |
| `busy_timeout` | 5000 ms | Retry on lock contention instead of immediate failure |
| `foreign_keys` | ON | Enforce referential integrity at insert time |

These settings are most impactful on large, established databases where B-tree depth is significant. On fresh databases with fast NVMe storage, the WAL absorbs write latency — the B-tree optimizations primarily benefit read queries, maintenance operations, and sustained write throughput over time.

### Migrations

Database migrations are stored in the `migrations/` directory:

The collector applies the initial schema on startup via `CREATE TABLE IF NOT EXISTS` statements embedded in the SQLite Writer. Migration files are provided for schema documentation and manual upgrades.

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

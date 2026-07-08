# Project Structure

```
otel-sqlite/
├── cmd/
│   ├── collector/          # Main OTLP log collector binary
│   │   └── main.go         # Entry point, wiring, lifecycle management
│   ├── loadtest/           # Built-in gRPC load generator for benchmarking
│   └── webhook-receiver/   # Dev tool: HTTP server for testing notification webhooks
│
├── internal/
│   ├── batcher/            # Batch builder: collects records from ingress, wraps in commands
│   │   ├── batcher.go      # Batcher struct: grouping, flushing, error notifier forwarding
│   │   └── batcher_test.go
│   │
│   ├── config/             # Configuration: YAML file + env var overrides
│   │   ├── config.go       # Config struct, defaults, env parsing, validation
│   │   ├── config_test.go
│   │   └── file.go         # YAML file config parsing with duration extensions
│   │
│   ├── generated/          # Generated protobuf code (OTLP)
│   │   └── opentelemetry/proto/...
│   │
│   ├── ingest/             # Ingress queue abstraction
│   │   ├── queue.go        # Bounded channel queue for LogBatch ingress
│   │   └── queue_test.go
│   │
│   ├── maintenance/        # Database maintenance framework
│   │   ├── worker.go       # Generic task scheduler
│   │   ├── task.go         # MaintenanceTask interface
│   │   ├── config.go       # Maintenance config, env vars, validation
│   │   ├── metrics.go      # Maintenance-specific Prometheus metrics
│   │   └── tasks/          # Pluggable maintenance tasks
│   │       ├── retention.go        # Purge old log records
│   │       ├── checkpoint.go       # WAL checkpoint
│   │       ├── optimize.go         # PRAGMA optimize
│   │       ├── vacuum.go           # Database vacuum (disabled by default)
│   │       └── fts_rebuild.go      # Rebuild contentless FTS5 index
│   │
│   ├── metrics/            # Prometheus metrics (ingestion, storage, commands, notify)
│   │   ├── metrics.go      # All metric definitions with nil-safe accessors
│   │   └── metrics_test.go
│   │
│   ├── model/              # Internal domain model (no protobuf dependency)
│   │   ├── logrecord.go    # LogRecord, LogBatch, Severity, Attribute, ValueType
│   │   ├── logrecord_pool.go   # sync.Pool for LogRecord (production build)
│   │   ├── logrecord_nopool.go # Heap alloc for LogRecord (-tags nopool)
│   │   ├── resource.go     # Resource struct with deterministic ID hashing
│   │   └── value.go        # AttributeValue (union type for resource attributes)
│   │
│   ├── events/             # Event model for the notification pipeline
│   │   ├── event.go        # Event struct, Fingerprint, EventFromLogRecord
│   │   └── event_test.go
│   │
│   ├── rules/              # Notification rule matching engine
│   │   ├── rule.go         # Rule (with alert_window/threshold/resolve_window), RuleEngine
│   │   └── rule_test.go
│   │
│   ├── alerts/             # Alert state machine (window → counter → state → alert)
│   │   ├── alert.go        # Alert struct, AlertStatus enum, AlertID, NewAlert
│   │   ├── window.go       # Sliding time window with LoadFrom/Snapshot
│   │   ├── counter.go      # Counter wraps Window + threshold check
│   │   ├── state.go        # EvaluateAlert state machine, ApplyTransition
│   │   ├── store.go        # AlertStore interface (Get, Put, Delete, ListByStatus, ListAll)
│   │   ├── store_bbolt.go  # bbolt-backed AlertStore
│   │   └── *_test.go       # Unit tests for all components
│   │
│   ├── notifications/      # Notification delivery + retry + DLQ
│   │   ├── types.go        # IsRetryable, NewNotRetryableError, sentinel errors
│   │   ├── store.go        # Store interface, NotificationState, DLQEntry
│   │   ├── store_bbolt.go  # bbolt implementation with retry bucket index + GC
│   │   ├── notifier.go     # Notifier interface (receives *alerts.Alert) + NewNotifier factory
│   │   ├── notifier_http.go    # HTTP webhook notifier (4xx→non-retryable, 5xx→retryable)
│   │   ├── notifier_log.go     # Log notifier (log.Printf, dev/testing)
│   │   ├── worker.go       # Worker: goroutines (process, retry, GC), alert pipeline integration
│   │   └── e2e_test.go     # End-to-end: Event → Rule → Alert → Notifier
│   │
│   ├── otlp/               # OTLP gRPC transport layer (protobuf boundary)
│   │   ├── server.go       # gRPC service implementation
│   │   ├── mapper.go       # OTLP protobuf → internal model conversion (uses sync.Pool)
│   │   └── *_bench_test.go # Allocation and throughput benchmarks
│   │
│   └── storage/            # Storage abstractions
│       ├── command.go      # Command, CommandExecutor, NonTransactionalCommand interfaces
│       ├── queue.go        # CommandQueue (bounded FIFO channel)
│       ├── storage.go      # Legacy LogStorage/LogWriter/LogReader interfaces
│       └── sqlite/         # SQLite writer implementation
│           ├── writer.go           # Single-goroutine writer, transaction lifecycle, pragmas
│           ├── write_batch_command.go  # WriteBatchCommand: hot-path log insertion
│           ├── migrator.go         # Schema migrations (001-004) with tracking table
│           ├── checkpoint_command.go   # WAL checkpoint (non-transactional)
│           ├── optimize_command.go     # PRAGMA optimize (non-transactional)
│           ├── vacuum_command.go       # Database vacuum (non-transactional)
│           ├── purge_logs_command.go   # Retention: delete old records
│           ├── rebuild_fts_command.go  # FTS5 index rebuild
│           ├── writer_test.go          # CRUD + transaction tests
│           ├── writer_bench_test.go    # Insert benchmarks
│           ├── writer_perf_bench_test.go   # Pragma comparison + E2E pipeline benchmarks
│           ├── writer_pool_bench_test.go   # sync.Pool allocation benchmarks
│           └── e2e_test.go            # Full pipeline integration tests
│
├── docs/
│   ├── architecture.md     # Architecture overview, component ownership, design decisions
│   └── project_structure.md # This file
│
├── config.example.yaml     # Annotated YAML config with all options documented
├── goals.md               # Original notification pipeline design spec (now implemented)
│
├── buf.gen.yaml           # Buf code generation config (protobuf)
├── buf.yaml               # Buf registry config
├── lefthook.yml           # Git hooks: lint, vet, test, security on pre-commit/push
│
├── docker-compose.yml          # Collector + Prometheus + Grafana
├── docker-compose.loadtest.yml # Collector-only for load testing
├── prometheus.yml              # Prometheus scrape config
│
└── README.md              # User-facing documentation
```

## Package Dependency Rules

```
cmd/collector → internal/config
              → internal/batcher
              → internal/events
              → internal/rules
              → internal/alerts
              → internal/notifications
              → internal/otlp
              → internal/storage
              → internal/storage/sqlite
              → internal/maintenance
              → internal/metrics

internal/otlp → internal/ingest
              → internal/model           (OTLP boundary: no protobuf leaks past here)
              → internal/metrics

internal/batcher → internal/ingest
                 → internal/storage
                 → internal/model
                 → internal/metrics

internal/notifications → internal/events
                       → internal/rules
                       → internal/alerts
                       → internal/model   (no protobuf, no SQLite)
                       → internal/metrics

internal/rules → internal/events
               → internal/model

internal/alerts → internal/model

internal/events → internal/model

internal/storage/sqlite → internal/storage
                        → internal/model
                        → internal/metrics

internal/maintenance → internal/storage  (CommandSubmitter interface only)
                     → internal/storage/sqlite (CheckpointMode type only)
```

## Key Design Decisions

1. **OTLP Protobuf isolation**: Protobuf types never leave `internal/otlp/`. The mapper converts to `model.LogRecord` at the boundary. All downstream packages work with domain types only.

2. **Single SQLite writer**: Exactly one goroutine writes to SQLite. This eliminates lock contention, simplifies transaction management, and makes WAL mode effective (one writer + many readers).

3. **Command pattern**: All database mutations are `Command` objects submitted to a bounded queue. The writer owns transaction lifecycle. Commands are self-contained, immutable, and testable in isolation.

4. **Non-blocking notification**: The notification pipeline is a sidecar — the batcher's hot path does a non-blocking channel send. If the queue is full, events are dropped with a counter increment. The notification path never blocks ingestion.

5. **Alert state machine**: Each rule×resource pair creates one Alert with a lifecycle (pending → firing → resolved). Events are counted within sliding time windows per rule. Thresholds determine when alerts fire; silence windows determine when they resolve. Alert and delivery state are persisted in separate bbolt stores.

6. **bbolt for state**: Pure-Go embedded KV store with no CGo dependency. Retry index bucket enables O(retryable) scanning. Resolved alerts and stale delivery state are GC'd asynchronously after 24h.

6. **Prepared statements via tx.Stmt()**: Insert SQL is compiled once at startup, then bound to each transaction. Multi-row batch INSERT was benchmarked but proved slower — per-row with prepared statements is optimal for this workload.

7. **Production pragmas at startup**: cache_size, mmap_size, temp_store, journal_size_limit, busy_timeout are set before migrations. These are impactful on large/established databases; on fresh NVMe-backed DBs the WAL absorbs write latency naturally.

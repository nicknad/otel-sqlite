# Notification Queue & Rule Engine — `internal/notify`

## Context

You are extending the OTLP-SQLite collector (Go). The existing pipeline is:

```
gRPC Server (otlp.Server)
  → Mapper (otlp.Mapper) → model.LogBatch(es)
  → IngressQueue (chan *model.LogBatch)
  → Batcher (batcher.Batcher) → WriteBatchCommand
  → CommandQueue (chan Command)
  → SQLite Writer (sqlite.Writer)
```

The batcher (`internal/batcher/batcher.go`) has already been modified to call a `notify.Worker` when a record's `SeverityNumber >= model.SeverityError`. That worker is injected via `WithErrorNotifier(worker)`. The signatures already exist in the batcher's constructor and `run()` loop — you only need to design the `internal/notify` package that the batcher calls into.

Read these key files first to understand conventions:
- `internal/batcher/batcher.go` — the injection point
- `internal/model/logrecord.go` — `Severity`, `LogRecord` types
- `internal/storage/command.go` — `Command`/`CommandExecutor` pattern (model for your interfaces)
- `internal/config/config.go` — config patterns (struct, env vars, validation, `LoadFromEnv`)
- `internal/maintenance/worker.go` — worker lifecycle pattern (Start, Stop, Wait)

## What to build

Create the directory `internal/notify/` containing a stateful notification pipeline with four layers:

```
LogRecord (from batcher)
  → Filter / Rule Engine  (evaluates thresholds, dedup, rate limits)
    → State Store          (embedded KV — tracks retries, cooldowns, counters)
      → Notifier           (pluggable sender: Slack, HTTP, etc.)
        → Retry Queue      (buffered channel + goroutine)
          → Dead-Letter Queue (persistent after N failures)
```

---

## 1. Core Types (`internal/notify/types.go`)

```go
package notify

// Event is the notification payload passed from the batcher.
type Event struct {
    Severity         model.Severity
    SeverityText     string
    Body             string
    ResourceID       string
    Resource         *model.Resource
    Timestamp        int64
    TraceID          [16]byte
    SpanID           [8]byte
    Attributes       []model.Attribute
    ScopeName        string
    ScopeVersion     string
}
```

---

## 2. State Store Interface (`internal/notify/store.go`)

Design a generic store interface for tracking notification state. The user mentioned RocksDB, but keep the interface agnostic so alternative backends (bbolt, Badger, SQLite) can be swapped.

Minimum state tracked per "notification key" (e.g., a composite of resource + rule + event fingerprint):

| Field | Purpose |
|---|---|
| `RetryCount` | How many times delivery has been attempted |
| `LastAttempt` | Unix timestamp of last delivery attempt |
| `LastSuccess` | Unix timestamp of last successful delivery |
| `NextRetry` | Unix timestamp when next retry is allowed |
| `CooldownUntil` | Unix timestamp — suppress notifications until this time |
| `ErrorRateWindow` | Sliding window counters for rate limiting (N events in last M minutes) |
| `EventDigest` | Hash of body+attributes for deduplication within a window |
| `DeadLettered` | Bool — moved to DLQ |

```go
// Store is the persistence layer for notification state.
// Implementations can use RocksDB, bbolt, Badger, or even SQLite.
type Store interface {
    // GetState retrieves state for a notification key.
    // Returns ErrNotFound if absent.
    GetState(ctx context.Context, key string) (*NotificationState, error)

    // PutState upserts state for a notification key.
    PutState(ctx context.Context, key string, state *NotificationState) error

    // DeleteState removes state (after successful delivery or manual ack).
    DeleteState(ctx context.Context, key string) error

    // EnqueueDLQ moves an event to the dead-letter queue.
    EnqueueDLQ(ctx context.Context, event *Event, reason string) error

    // ListDLQ returns all events currently in the dead-letter queue.
    ListDLQ(ctx context.Context) ([]*DLQEntry, error)

    // AckDLQ removes a DLQ entry after manual intervention or replay.
    AckDLQ(ctx context.Context, id string) error

    // Close cleans up store resources.
    Close() error
}

type NotificationState struct {
    RetryCount      int
    LastAttempt     int64   // unix nanos
    LastSuccess     int64   // unix nanos
    NextRetry       int64   // unix nanos
    CooldownUntil   int64   // unix nanos
    ErrorRateBucket []int64 // timestamps for sliding-window rate limiting
    EventDigest     string
    DeadLettered    bool
    UpdatedAt       int64   // unix nanos
}

type DLQEntry struct {
    ID          string
    Event       *Event
    FailReason  string
    RetryCount  int
    FailedAt    int64 // unix nanos
    OriginalKey string
}
```

Provide one concrete implementation using **bbolt** (pure Go, embedded, file-backed KV) as the default. Structure it as `internal/notify/store_bbolt.go`. The user mentioned RocksDB — add a build-tag `//go:build rocksdb` variant or keep it as a future option; default should be bbolt for zero CGo dependency.

---

## 3. Rule Engine (`internal/notify/rule.go`)

Rules control **whether** a notification fires and **how** it is grouped/rate-limited. They should be composable and loaded from config.

```go
// Rule defines a single notification rule.
type Rule struct {
    // Name is a human-readable label (used in logging and metrics).
    Name string

    // MatchSeverity is the minimum severity to trigger (e.g., SeverityError).
    MatchSeverity model.Severity

    // ResourceFilter is an optional glob/regex on resource ID.
    // Empty means match all.
    ResourceFilter string

    // BodyFilter is an optional substring/regex filter on the event body.
    BodyFilter string

    // AttributeFilters is an optional map of attribute key→value/regex filters.
    AttributeFilters map[string]string

    // Cooldown is the minimum duration between notifications for the same key.
    // During cooldown, events are silently dropped (no state update).
    Cooldown time.Duration

    // RateLimit is the maximum number of notifications allowed per RateWindow.
    RateLimit int
    RateWindow time.Duration

    // DedupWindow suppresses duplicate events (same digest) within this window.
    DedupWindow time.Duration

    // MaxRetries before the event moves to the dead-letter queue.
    MaxRetries int

    // RetryBackoff is the base delay between retries (exponential: base * 2^attempt).
    RetryBackoff time.Duration

    // Destination is the name of the Notifier implementation to use.
    // Must match a registered notifier.
    Destination string
}

// RuleEngine evaluates events against a set of rules.
type RuleEngine interface {
    // Evaluate checks all rules against the event.
    // Returns the first matching Rule (most specific wins) along with a
    // composite key string for state tracking, or nil if no rule matches.
    Evaluate(ctx context.Context, event *Event) (rule *Rule, key string, matched bool)

    // Rules returns the list of configured rules.
    Rules() []Rule
}
```

---

## 4. Notifier Interface (`internal/notify/notifier.go`)

Pluggable destination implementations.

```go
// Notifier sends a notification to an external system.
// Implementations must be safe for concurrent use.
type Notifier interface {
    // Send delivers the event. Returns nil on success, error on failure.
    // The caller decides retry logic.
    Send(ctx context.Context, event *Event) error

    // Name returns a unique name for this notifier (matches Rule.Destination).
    Name() string

    // Close cleans up notifier resources.
    Close() error
}
```

Provide at least two implementations:
- **`internal/notify/notifier_log.go`** — writes to `log.Printf` (for development/testing)
- **`internal/notify/notifier_http.go`** — POSTs JSON to a configurable URL with optional auth

---

## 5. Worker (`internal/notify/worker.go`)

The async worker that ties everything together. This is what the batcher calls into.

```go
// Worker receives events from the batcher, runs them through the rule engine,
// manages state/retries in the store, and calls the appropriate Notifier.
type Worker struct {
    // unexported fields
}

// NewWorker creates a worker.
// eventQueueDepth sets the buffered channel capacity for incoming events.
func NewWorker(store Store, engine RuleEngine, notifiers map[string]Notifier, eventQueueDepth int) *Worker

// Send enqueues an event for processing (non-blocking if queue not full).
// Returns ErrQueueFull if the event queue is at capacity.
func (w *Worker) Send(ctx context.Context, event *Event) error

// Start launches the worker goroutines (main processor + retry loop).
func (w *Worker) Start(ctx context.Context)

// Stop signals shutdown. Flushes in-flight events before returning.
func (w *Worker) Stop()
```

The worker must run **two** goroutines:

1. **Main processor** — reads from the event queue, calls `RuleEngine.Evaluate`, checks state store for cooldown/dedup/rate-limit, calls `Notifier.Send`, updates state (increment retry count on failure, clear on success).

2. **Retry loop** — periodically scans the store for entries where `NextRetry` is in the past and `RetryCount < MaxRetries`, re-attempts delivery. When `RetryCount >= MaxRetries`, moves the event to the dead-letter queue via `Store.EnqueueDLQ`.

---

## 6. Configuration (`internal/config/config.go`)

Add a `Notification` field to `Config`:

```go
type NotificationConfig struct {
    // Enabled is the master switch.
    Enabled bool `mapstructure:"enabled"`

    // EventQueueDepth is the buffered channel capacity between batcher and worker.
    EventQueueDepth int `mapstructure:"event_queue_depth"`

    // StorePath is the file path for the embedded KV store (bbolt).
    StorePath string `mapstructure:"store_path"`

    // RetryInterval is how often the retry goroutine scans for retryable events.
    RetryInterval time.Duration `mapstructure:"retry_interval"`

    // Rules defines the notification rules.
    Rules []RuleConfig `mapstructure:"rules"`
}

type RuleConfig struct {
    Name             string            `mapstructure:"name"`
    MatchSeverity    string            `mapstructure:"match_severity"` // "ERROR", "FATAL", etc.
    ResourceFilter   string            `mapstructure:"resource_filter"`
    BodyFilter       string            `mapstructure:"body_filter"`
    AttributeFilters map[string]string `mapstructure:"attribute_filters"`
    Cooldown         string            `mapstructure:"cooldown"`      // duration string
    RateLimit        int               `mapstructure:"rate_limit"`
    RateWindow       string            `mapstructure:"rate_window"`  // duration string
    DedupWindow      string            `mapstructure:"dedup_window"` // duration string
    MaxRetries       int               `mapstructure:"max_retries"`
    RetryBackoff     string            `mapstructure:"retry_backoff"` // duration string
    Destination      string            `mapstructure:"destination"`
}
```

Add notification config to the env-var mapping, validation, and YAML example.

Wire it into `cmd/collector/main.go`:
- In `initialize()`: if enabled, create store, rule engine, notifiers, and worker; call `batcher.WithErrorNotifier(worker)`.
- In `start()`: call `worker.Start()`.
- In `cleanup()`: call `worker.Stop()` then `store.Close()` and each notifier's `Close()`.

---

## 7. Metrics (`internal/metrics/metrics.go`)

Add notification-related metrics:
- `notify_events_received_total` (counter)
- `notify_events_matched_total` (counter, with rule label)
- `notify_events_delivered_total` (counter, with destination label)
- `notify_events_failed_total` (counter, with destination label)
- `notify_events_dead_lettered_total` (counter)
- `notify_queue_depth` (gauge)
- `notify_retry_queue_depth` (gauge)
- `notify_store_size_bytes` (gauge, if available)

---

## 8. Testing

- `internal/notify/worker_test.go` — mock Store, RuleEngine, and Notifier; test the full lifecycle
- `internal/notify/store_bbolt_test.go` — test CRUD, DLQ, edge cases with a temp file
- `internal/notify/rule_test.go` — test matching, ordering, filter combinations
- `internal/notify/notifier_http_test.go` — test with `httptest.Server`

---

## Design constraints

- **No CGo in the default build** — use bbolt (pure Go) for the default store; RocksDB can be added behind a build tag later.
- **All interfaces must be mockable** — no concrete dependencies leaking across package boundaries.
- **Non-blocking ingestion** — `Send()` to the worker must never block the batcher's hot path beyond a bounded channel send.
- **Graceful shutdown** — `Stop()` must drain in-flight events. Use `context.CancelCauseFunc` like the existing components.
- **Backpressure** — if the event queue is full, `Send()` returns `ErrQueueFull`; the batcher logs and drops the event (increments a counter).
- **Follow existing conventions** — look at how `internal/maintenance/worker.go` handles lifecycle, how `internal/config/config.go` does env-override + validation, and how `internal/metrics/metrics.go` structures its nil-safe methods.

---

## Files to create/modify

| File | Action |
|---|---|
| `internal/notify/types.go` | Create — core types |
| `internal/notify/store.go` | Create — `Store` interface |
| `internal/notify/store_bbolt.go` | Create — bbolt implementation |
| `internal/notify/rule.go` | Create — `Rule`, `RuleEngine` |
| `internal/notify/notifier.go` | Create — `Notifier` interface |
| `internal/notify/notifier_log.go` | Create — log notifier |
| `internal/notify/notifier_http.go` | Create — HTTP notifier |
| `internal/notify/worker.go` | Create — `Worker` struct + lifecycle |
| `internal/notify/worker_test.go` | Create — tests with mocks |
| `internal/config/config.go` | Modify — add `NotificationConfig`, env vars, validation |
| `internal/metrics/metrics.go` | Modify — add notification metrics |
| `cmd/collector/main.go` | Modify — wire notification components |
| `config.example.yaml` | Modify — add notification config example |

---

## Implementation order

1. Types + Store interface + bbolt implementation
2. Rule + RuleEngine
3. Notifier interface + LogNotifier + HTTPNotifier
4. Worker (main loop + retry loop)
5. Config + wiring + metrics
6. YAML example
7. Tests

# Metrics ingestion & storage — extension proposal

Branch: `metric-extension`
Status: **Phase 1 implemented** (Gauge + Sum end-to-end: proto generation, domain
model, migration 006, mapper, `WriteMetricsCommand`, metric ingress queue +
metric batcher, dual gRPC services, config wiring, tests). Phases 2–3
(Histogram/ExpHistogram/Summary queries, bucket normalization, exemplar
queries) and the later-phase items below remain open.

This document proposes extending the collector to ingest **OTLP Metrics**
(application telemetry) and persist them in SQLite, reusing the existing
queue → batcher → command → single-writer pipeline.

## 0. Terminology: two different kinds of "metrics"

Before anything else, the two "metrics" in this project must be kept distinct.
They share a name, but they are different systems with different lifecycles:

| | Collector self-instrumentation | Ingested OTLP metrics |
|---|---|---|
| Direction | Out: the collector reports on itself | In: applications report their telemetry |
| Protocol | Prometheus text, pulled by a scraper | OTLP/gRPC, pushed by SDKs/agents |
| Endpoint | `GET /metrics` (`internal/otlp/server.go` → `startMetricsServer`, `promhttp`) | `Export(Metrics)` gRPC method |
| Model | `internal/metrics` package (prometheus counters/histograms/gauges) | New domain types in `internal/model` (`Metric`, `MetricSeries`, `DataPoint`) |
| Storage | None (in-memory counters) | SQLite tables (migration 006) |
| Consumers | Prometheus scraping the collector | Future SQL queries / dashboards |

Rules:

1. The `internal/metrics` package stays exactly what it is: instrumentation
   *about* the collector. It gains new counters *about ingestion of OTLP
   metrics* (e.g. `otlp_metrics_received_total`), but it never stores or
   represents the ingested metric data.
2. Ingested metrics are **never** exposed on `/metrics`. The Prometheus
   endpoint remains collector telemetry only.
3. The domain model for ingested metrics lives in `internal/model/metric.go`
   (a new file), not in `internal/metrics`.

## 1. Goal and non-goals

Goal: accept OTLP `ExportMetricsServiceRequest`, map it to an internal domain
model, and persist it losslessly enough that a future query layer can answer
"give me series `http.server.request.duration` for `service.name=X` between
t1 and t2".

Non-goals for this extension (deliberately deferred):

- PromQL / a query API / serving stored metrics
- Downsampling, aggregation, rate calculation
- Retention policies for metrics (the existing retention task targets
  `log_event` only; a metric retention task is a later addition)
- Cardinality management / series limits
- Alerting on ingested metric values

The pipeline must be extended, not rewritten. The existing architecture for
logs already handles backpressure, batching, transactions, and single-writer
ordering; metrics should plug into it.

## 2. Pipeline shape: what stays, what is added

Current log path (from `docs/architecture.md`):

```text
OTLP Logs (gRPC)
  -> internal/otlp/mapper.go        (protobuf -> domain)
  -> ingest.IngressQueue            (bounded, *model.LogBatch)
  -> batcher.Batcher                (merge + wrap as Command)
  -> storage.CommandQueue           (bounded, storage.Command)
  -> sqlite.Writer                  (single goroutine, transactions)
  -> SQLite
```

Proposed metrics path — the bottom half is identical:

```text
OTLP Metrics (gRPC)
  -> internal/otlp/metrics.go       (protobuf -> domain)   [new]
  -> ingest queue (metrics)         (bounded, *model.MetricBatch)  [new]
  -> metric batcher                 (merge + wrap as Command)       [new]
  -> storage.CommandQueue           (SAME)
  -> sqlite.Writer                  (SAME, small generalization)
  -> SQLite

                 ┌── WriteLogsCommand ──────┐
                 │                           │
Command Queue ───┼── WriteMetricsCommand ────┼──> SQLite
                 │                           │
                 └── WriteTracesCommand ─────┘   (future)
```

What stays unchanged:

- `storage.Command` interface (`internal/storage/command.go`) — `WriteMetricsCommand`
  implements exactly this.
- `storage.CommandQueue` (`internal/storage/queue.go`) — commands from both
  signals share one bounded queue; the writer never needs to know which signal
  a command carries.
- `sqlite.Writer` run loop, transaction lifecycle, WAL pragmas, prepared
  statement binding via `tx.Stmt()`, `WRITER_BATCH_SIZE` command counting.
- `sqlite.MaxTransactionRecords` splitting — with one small generalization
  (see §7).

What is added:

| File | Purpose |
|---|---|
| `api/opentelemetry/proto/metrics/v1/metrics.proto` | already present in `master` (untracked); must be committed + generated |
| `api/opentelemetry/proto/collector/metrics/v1/metrics_service.proto` | to be added |
| `internal/generated/.../metrics/v1/*` | generated protobuf (gitignored, produced by `make generate`) |
| `internal/model/metric.go` | domain types (§3) |
| `internal/otlp/metrics.go` | mapper (§5) |
| `internal/ingest/` (metric queue) | §6 |
| `internal/batcher/metric_batcher.go` | §6 |
| `internal/storage/sqlite/metrics.go` | `WriteMetricsCommand` + SQL (§7) |
| `migrations/006_metrics.sql` + `migration006SQL` const | §4 |

## 3. Domain model (`internal/model/metric.go`)

Metrics are **not** "logs with a different payload". A log event is an
occurrence; a metric data point is a sample of a named, typed series. The
domain model must make that explicit.

```go
// MetricType mirrors OTLP Metric.Data oneof.
type MetricType uint8

const (
    MetricTypeGauge MetricType = iota
    MetricTypeSum
    MetricTypeHistogram
    MetricTypeExponentialHistogram
    MetricTypeSummary
)

// AggregationTemporality matters for Sum/Histogram/ExponentialHistogram:
// downstream queries must know whether a value is delta or cumulative.
type AggregationTemporality uint8

const (
    TemporalityUnspecified AggregationTemporality = iota
    TemporalityDelta
    TemporalityCumulative
)

// Metric is the definition: name + metadata + type. It does NOT carry values.
// Multiple series share one Metric row (see series identity below).
type Metric struct {
    ID           string // hash: resource_id + scope + name + unit + type (see §4)
    ResourceID   string
    ScopeName    string
    ScopeVersion string
    SchemaURL    string
    Name         string
    Description  string
    Unit         string
    Type         MetricType
    IsMonotonic  bool                    // Sum only
    Temporality  AggregationTemporality  // Sum/Histogram/ExpHistogram only
}

// MetricSeries is the time-series identity: (metric, attribute set).
// The attribute set is what makes series distinct, e.g.
//   http.server.request.duration {method=GET,  status=200}
//   http.server.request.duration {method=GET,  status=500}
//   http.server.request.duration {method=POST, status=200}
type MetricSeries struct {
    ID         string              // hash of (Metric.ID + canonical attributes)
    MetricID   string
    Attributes []model.Attribute   // reuse the existing inline Attribute type
}

// DataPoint is one sample. Exactly one value shape is populated.
type DataPoint struct {
    Timestamp      int64 // unix ns
    StartTimestamp int64 // unix ns; 0 when absent (gauge)
    Flags          uint32 // OTLP DataPointFlags (NoRecordedValue, ...)

    // Scalar (Gauge / Sum)
    DoubleValue *float64
    IntValue    *int64

    // Aggregations (Phase 2+; columns reserved in the schema)
    Count    *uint64
    Sum      *float64
    Min      *float64
    Max      *float64

    // Rich payloads (Phase 2/3): serialized, not yet normalized.
    HistogramJSON          []byte // explicit-bound buckets
    ExponentialHistogramJSON []byte
    SummaryJSON            []byte // quantile values

    Exemplars []Exemplar // preserved from day one (Phase 1 keeps the column)
}

// Exemplar links a measurement to a trace.
type Exemplar struct {
    Timestamp  int64
    Value      float64
    TraceID    [16]byte
    SpanID     [8]byte
    HasTrace   bool
    Attributes []model.Attribute
}

// MetricBatch is the unit that flows through the ingress queue —
// the analogue of model.LogBatch.
type MetricBatch struct {
    Resource     *model.Resource // reuse existing type + SHA-256 ID scheme
    SchemaURL    string
    ScopeName    string
    ScopeVersion string
    Metrics      []*Metric   // each metric carries its Series and DataPoints
}
```

Design notes:

- **Series identity is the core abstraction.** We never flatten data points
  into a metric with ad-hoc attribute JSON. The series key is derived from the
  canonical (sorted) attribute set, so identical attribute sets deduplicate
  (`INSERT OR IGNORE`, same pattern as resources today).
- **Temporality and monotonicity are first-class.** `Value float64` alone is
  not enough; `Sum{value=100, cumulative}` means something different from
  `Sum{value=100, delta}`.
- **Value shapes mirror OTLP's oneof**, so the mapper is a mechanical
  projection and nothing is reinterpreted.
- Reuse `model.Attribute` (inline struct, no heap indirection) and
  `model.Resource` (including `computeResourceID`) as-is.

## 4. Storage schema (`migrations/006_metrics.sql`)

Resources already have a perfect home: `log_resource`. It is signal-agnostic
(`id, service_name, host_name, schema_url, attributes_json`) and the writer
already dedups it. Reuse it; a later cosmetic rename to `resource` is possible
but not required. The resource ID hashing in the mapper is shared.

Scope: logs currently denormalize `scope_name`/`scope_version` onto
`log_event`. For metrics we introduce a real `scope` table — this is the right
moment to fix the abstraction, and metric cardinality is low enough that the
extra lookup is cheap. (Alternative: denormalize scope onto `metric` like logs
do; see open questions §11.)

```sql
-- Migration 006: Metrics
-- Phased: this file contains the Phase 1 (Gauge + Sum) tables plus
-- columns reserved for Phase 2/3 payloads.

CREATE TABLE IF NOT EXISTS scope (
    id TEXT PRIMARY KEY,             -- hash: resource_id + name + version + schema_url
    resource_id TEXT NOT NULL,
    name TEXT,
    version TEXT,
    schema_url TEXT,
    FOREIGN KEY (resource_id) REFERENCES log_resource(id)
);

-- Metric definitions (low cardinality, deduped).
CREATE TABLE IF NOT EXISTS metric (
    id TEXT PRIMARY KEY,             -- hash: resource_id + scope_id + name + unit + type
    scope_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    unit TEXT,
    type INTEGER NOT NULL,           -- 0 gauge, 1 sum, 2 histogram, ...
    is_monotonic INTEGER NOT NULL DEFAULT 0,
    aggregation_temporality INTEGER NOT NULL DEFAULT 0, -- 0 unspec, 1 delta, 2 cumulative
    FOREIGN KEY (scope_id) REFERENCES scope(id)
);

-- Time-series identity (moderate cardinality, deduped).
CREATE TABLE IF NOT EXISTS metric_series (
    id TEXT PRIMARY KEY,             -- hash: metric_id + canonical attribute set
    metric_id TEXT NOT NULL,
    attributes_json TEXT NOT NULL,   -- canonical (sorted) JSON; identity, not arbitrary attrs
    FOREIGN KEY (metric_id) REFERENCES metric(id)
);

-- Data points (high cardinality, append-only, like log_event).
CREATE TABLE IF NOT EXISTS metric_data_point (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id TEXT NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    start_timestamp_ns INTEGER,
    flags INTEGER NOT NULL DEFAULT 0,
    double_value REAL,               -- Gauge/Sum double
    int_value INTEGER,               -- Gauge/Sum int
    count INTEGER,                   -- Histogram/Summary (Phase 2+)
    sum REAL,                        -- Histogram/Summary (Phase 2+)
    min REAL,                        -- Histogram (Phase 2+)
    max REAL,                        -- Histogram (Phase 2+)
    histogram_json TEXT,             -- Phase 2: explicit bounds + bucket counts
    exponential_histogram_json TEXT, -- Phase 3
    summary_json TEXT,               -- Phase 3: quantiles
    exemplars_json TEXT,             -- preserved, not yet queryable
    FOREIGN KEY (series_id) REFERENCES metric_series(id)
);

CREATE INDEX IF NOT EXISTS idx_metric_dp_series_time
    ON metric_data_point(series_id, timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_metric_series_metric
    ON metric_series(metric_id);
CREATE INDEX IF NOT EXISTS idx_metric_scope
    ON metric(scope_id);
```

Rationale, following the design guidance:

- **Normalize series identity, not every attribute.** `metric_series` with a
  canonical attribute hash gives us time-series identity for `WHERE`
  filtering without a per-attribute table (which was already removed for logs
  in migration 005).
- **Histograms start as a JSON payload** (`histogram_json`) on the data point
  row, with `count/sum/min/max` promoted to real columns because they are
  needed for every histogram query. Buckets are not normalized yet — that is
  the Phase 2 optimization once real query requirements exist.
- **Exemplars are stored, not dropped** (`exemplars_json`), even in Phase 1.
  They are the trace↔metric link and cheap to preserve.
- The migration lives in `migrations/006_metrics.sql` and is embedded as a
  `migration006SQL` const registered in `allMigrations()` in
  `internal/storage/sqlite/migrator.go` (same mechanism as 001–005).

## 5. Mapper (`internal/otlp/metrics.go`)

New file in `internal/otlp`, mirroring `mapper.go`:

```go
// MapMetricsData converts OTLP ResourceMetrics to []*model.MetricBatch.
func (m *Mapper) MapMetricsData(resourceMetrics []*metricsV1.ResourceMetrics) []*model.MetricBatch
```

- Reuses `mapResource` / `computeResourceID` unchanged — resources are the
  same concept across signals.
- Maps `ResourceMetrics → ScopeMetrics → Metric → data points`:
  - `Metric.Type` from the oneof (`Gauge`, `Sum`, `Histogram`, ...).
  - `IsMonotonic`, `AggregationTemporality` from `Sum`/`Histogram`.
  - `NumberDataPoint`: `AsDouble`/`AsInt`, timestamps, flags, attributes
    (converted with the existing `mapAttribute`).
  - Exemplars mapped into `DataPoint.Exemplars` (trace/span IDs copied into
    fixed-size arrays, same pattern as `LogRecord.TraceID`).
- Compute `Metric.ID` and `MetricSeries.ID` with the same SHA-256 approach as
  `computeResourceID`, so identical definitions/attribute sets deduplicate in
  the writer.
- Compute scope ID the same way.

Protobuf plumbing (prerequisite):

- Commit `api/opentelemetry/proto/metrics/v1/metrics.proto` (already in
  `master`, currently untracked) and add
  `api/opentelemetry/proto/collector/metrics/v1/metrics_service.proto`.
- Extend `Makefile` `PROTO_FILES` + the `--go_opt`/`--go-grpc_opt` module
  mappings; run `make generate` to produce `internal/generated/.../metrics/v1`.
- `internal/otlp/server.go`: register `metricsV1.RegisterMetricsServiceServer`
  alongside the logs service; add `ExportMetrics` handler mirroring `Export`
  (map → backpressure check → ingress queue send), instrumented with
  `aMetrics.IncrementMetricsReceived(...)`.

## 6. Ingress queue and batcher

The ingress queue interface (`ingest.IngressQueue`) is typed to
`*model.LogBatch`. Two options:

- **A. Generic core (recommended):** extract the channel-based bounded queue
  into a small generic `boundedQueue[T]` and keep two thin typed wrappers
  (`IngressQueue` for logs, `MetricIngressQueue` for metrics). The `Server`
  and `Batcher` signatures keep their concrete types; only the queue
  internals are shared. Mechanical, low risk.
- **B. Parallel implementation:** duplicate the ~40 lines of channel queue
  for `*model.MetricBatch`. Slightly more code, zero shared-surface risk.

Either way: two separate queues, each with its own capacity, feeding one
shared `storage.CommandQueue`. Backpressure behavior is identical.

Batcher: add `internal/batcher/metric_batcher.go`, mirroring `Batcher` but
for `*model.MetricBatch`. Same shape — `MetricBatchSize`, flush interval,
command factory injection:

```go
type NewWriteMetricsCommand func(batch *model.MetricBatch) storage.Command
```

The error-notifier hook stays log-only (severity is a log concept).

## 7. Write command and writer integration

`internal/storage/sqlite/metrics.go` defines `WriteMetricsCommand`, following
the `WriteBatchCommand` pattern exactly: immutable after construction, never
begins/commits/rolls back, uses injected prepared statements, carries a
process-local "already inserted" cache to skip `INSERT OR IGNORE` probes.

```go
type WriteMetricsCommand struct {
    batch    *model.MetricBatch
    points   int // data points, cached at construction

    preparedStmts *PreparedStatements
    seenScopes    map[string]struct{}
    seenMetrics   map[string]struct{}
    seenSeries    map[string]struct{}
}

func (c *WriteMetricsCommand) Execute(ctx context.Context, tx *sql.Tx) error
func (c *WriteMetricsCommand) Size() int // returns c.points
```

Execute order inside one transaction (parent rows before children, same
invariant the writer already relies on with `foreign_keys=OFF`):

1. `INSERT OR IGNORE` scope (if seen, skip).
2. `INSERT OR IGNORE` metric per definition.
3. `INSERT OR IGNORE` metric_series per series.
4. `INSERT INTO metric_data_point` per data point (append-only, like
   `log_event`).

Prepared statements (add three to `PreparedStatements` +
`preparedStatementsSQL` in `writer.go`): `InsertScope`, `InsertMetric`,
`InsertSeries`, `InsertDataPoint`. No changes to the writer's run loop.

The one small generalization in `writer.go`:

```go
// commandRecordCount today special-cases *WriteBatchCommand.
// Introduce a shared interface so the writer counts actual data points
// for metrics, not metric objects.
type RecordCounter interface{ Size() int }

func commandRecordCount(cmd storage.Command) int {
    if rc, ok := cmd.(RecordCounter); ok {
        return rc.Size()
    }
    return 1
}
```

This preserves `MaxTransactionRecords` semantics for both signals — critical
because a single OTLP metrics request can carry tens of thousands of data
points (`10 resources × 100 metrics × 50 series` = 50k points).

## 8. Config and `cmd/collector/main.go` wiring

- Config: `metrics_enabled` (default true once implemented), reuse
  `IngressQueueCapacity` or add `MetricsIngressQueueCapacity`,
  `BatcherMetricsBatchSize`, `WriterMaxTransactionRecords` (shared — it now
  counts data points of either signal).
- `initialize()`: create metric ingress queue + `MetricBatcher` with the
  `sqlite.NewWriteMetricsCommand` factory; register metrics service on the
  gRPC server.
- `start()`/`cleanup()`: start/stop the metric batcher alongside the log
  batcher; stop order unchanged (batchers → writer).

## 9. Collector instrumentation (`internal/metrics`)

Add Prometheus counters *about OTLP metric ingestion* (self-telemetry —
distinct from the stored data, per §0):

- `otlp_metrics_received_total` — data points received by the gRPC handler
- `otlp_metric_data_points_written_total` — data points committed
- `otlp_metrics_rejected_total` — backpressure rejections on the metrics path
- optionally: series created, per-type histograms of data points

The writer already reports `IncrementLogsWritten`; mirror with
`IncrementMetricDataPointsWritten`. None of these touch the stored metric
rows.

## 10. Phased delivery

**Phase 1 — Gauge + Sum (the deliverable of this branch)**

- Proto files committed + generated; metrics gRPC service registered.
- `internal/model/metric.go` (Metric, MetricSeries, DataPoint, MetricBatch).
- `migrations/006_metrics.sql` + `migration006SQL` + `allMigrations()` entry.
- `internal/otlp/metrics.go` mapper.
- `WriteMetricsCommand` + writer generalization (`RecordCounter`).
- Metric ingress queue + metric batcher + config + main wiring.
- Tests: mapper unit tests, `sql_syntax_test` for migration 006, e2e test
  mirroring the logs e2e (export metrics → query back rows).

**Phase 2 — Histogram**

- `HistogramJSON` payload, `count/sum/min/max` columns populated;
  mapper for `Histogram` data points; decide bucket normalization vs JSON.

**Phase 3 — ExponentialHistogram, Summary, Exemplars**

- Remaining payload columns; exemplar query support; first read-side queries
  (`WHERE metric.name = ... AND series attributes ... ORDER BY timestamp`).

**Later (explicitly out of scope for now)**

- Metric retention task in `internal/maintenance/tasks`; unified `scope` for
  logs (migrate `log_event.scope_name/version` to FK); PromQL/query API;
  downsampling; cardinality limits.

## 11. Open questions

1. **Resource table:** reuse `log_resource` for metrics (proposed) vs. new
   `metric_resource` table vs. renaming to `resource` in a follow-up
   migration. Reuse avoids duplicating dedup logic and mapper code.
2. **Scope:** first-class `scope` table (proposed) vs. denormalized
   `scope_name`/`scope_version` columns on `metric`. The table is the better
   abstraction; denormalization is the cheaper write path. Decide with the
   Phase 1 benchmark.
3. **Queue/batcher unification:** generic queue core + thin wrappers (option
   A) vs. parallel copies (option B). A is cleaner; B is safer for the hot
   log path.
4. **Series identity attribute set:** full attribute set (proposed) vs. a
   configured subset. Full set is lossless and matches OTLP semantics; subset
   matters only if cardinality becomes a problem.
5. **Scalar storage:** `double_value`/`int_value` columns (proposed) vs. JSON.
   Columns keep the hot path allocation-free and match future `WHERE value > x`
   queries.
6. **Series ID stability:** the proposed hash includes `unit` and `type` in
   the metric key; a unit change creates a new metric (and thus series). This
   matches OTLP semantics (unit is part of identity) but should be stated
   explicitly.

## 12. What this proposal deliberately does not change

- `internal/storage/command.go` — the `Command` interface is already signal-
  agnostic; that was the right abstraction and it stays.
- `sqlite.Writer` run loop, transaction lifecycle, WAL pragmas, prepared
  statement binding — untouched except for the `RecordCounter`
  generalization and the extended prepared statement list.
- `internal/metrics` — gains counters, loses nothing.
- The Prometheus `/metrics` endpoint — never serves ingested metrics.

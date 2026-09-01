-- Migration 006: Metrics schema
-- Created: 2025-08-15
-- Description: Creates the storage model for ingested OTLP metrics.
--
-- Resource -> scope -> metric -> series -> data point, following the OTLP
-- Resource/Scope/Signal semantics. Resources are shared with logs: the
-- log_resource table is the single resource store for both signals.
--
-- Phase 1 (Gauge + Sum) populates double_value/int_value. The remaining
-- columns (count/sum/min/max, JSON payloads, exemplars) are reserved so
-- Histogram, ExponentialHistogram, Summary and Exemplars are preserved
-- losslessly from day one.
--
-- nan_mask records which float columns hold NaN: SQLite stores NaN as NULL,
-- so a NaN column is bound as NULL and its bit is set here, distinguishing
-- "NaN" from "absent". Bit 0 = double_value, 1 = sum, 2 = min, 3 = max.
-- ±Inf survives in REAL columns and needs no bit.

-- Instrumentation scope (first-class, unlike the denormalized scope columns
-- on log_event).
CREATE TABLE IF NOT EXISTS scope (
    id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL,
    name TEXT,
    version TEXT,
    schema_url TEXT,
    FOREIGN KEY (resource_id) REFERENCES log_resource(id)
);

-- Metric definitions (low cardinality, deduplicated via INSERT OR IGNORE).
CREATE TABLE IF NOT EXISTS metric (
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    unit TEXT,
    type INTEGER NOT NULL,
    is_monotonic INTEGER NOT NULL DEFAULT 0,
    aggregation_temporality INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (scope_id) REFERENCES scope(id)
);

-- Time-series identity (moderate cardinality, deduplicated).
-- attributes_json is the canonical (sorted) attribute key; it defines the
-- series, so identical attribute sets share one row.
CREATE TABLE IF NOT EXISTS metric_series (
    id TEXT PRIMARY KEY,
    metric_id TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    FOREIGN KEY (metric_id) REFERENCES metric(id)
);

-- Data points (high cardinality, append-only, like log_event).
CREATE TABLE IF NOT EXISTS metric_data_point (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id TEXT NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    start_timestamp_ns INTEGER,
    flags INTEGER NOT NULL DEFAULT 0,
    double_value REAL,
    int_value INTEGER,
    count INTEGER,
    sum REAL,
    min REAL,
    max REAL,
    nan_mask INTEGER NOT NULL DEFAULT 0, -- bits: 1=double_value, 2=sum, 4=min, 8=max are NaN
    histogram_json TEXT,
    exponential_histogram_json TEXT,
    summary_json TEXT,
    exemplars_json TEXT,
    FOREIGN KEY (series_id) REFERENCES metric_series(id)
);

-- Indexes for query performance.
CREATE INDEX IF NOT EXISTS idx_metric_dp_series_time
    ON metric_data_point(series_id, timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_metric_series_metric
    ON metric_series(metric_id);
CREATE INDEX IF NOT EXISTS idx_metric_scope
    ON metric(scope_id);

-- Record this migration
INSERT OR IGNORE INTO schema_migrations (version, description) VALUES ('006', 'Metrics schema');

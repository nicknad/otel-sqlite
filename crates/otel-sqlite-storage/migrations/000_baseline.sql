-- ============================================================================
-- OTEL SQLite Storage
-- Squashed canonical schema
--
-- Design principles:
--   * SQLite WAL + single writer
--   * canonical data tables contain no FTS maintenance overhead
--   * FTS5 is derived/read-side state, rebuilt by maintenance
--   * log attributes are stored inline as JSON
--   * resources are shared between logs and metrics
--   * metrics follow Resource -> Scope -> Metric -> Series -> DataPoint
--   * read-side views provide stable query-layer interfaces
--   * indexes are limited to known query/retention paths
-- ============================================================================

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

-- ============================================================================
-- 1. Shared resources
--
-- Resource identity is application-defined.
-- `id` should be a deterministic fingerprint of the resource attributes.
-- ============================================================================

CREATE TABLE IF NOT EXISTS log_resource (
    id              TEXT PRIMARY KEY,
    service_name    TEXT NOT NULL,
    host_name       TEXT,
    schema_url      TEXT,
    attributes_json TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_log_resource_service
    ON log_resource(service_name);

-- ============================================================================
-- 2. Log events
--
-- INTEGER PRIMARY KEY is intentional.
-- It is already SQLite's rowid and avoids AUTOINCREMENT overhead.
-- ============================================================================

CREATE TABLE IF NOT EXISTS log_event (
    id                       INTEGER PRIMARY KEY,

    resource_id              TEXT NOT NULL,

    timestamp_ns             INTEGER NOT NULL,
    observed_timestamp_ns    INTEGER NOT NULL,

    severity_number          INTEGER NOT NULL,
    severity_text            TEXT,

    trace_id                 BLOB,
    span_id                  BLOB,

    body                     TEXT,
    event_name               TEXT,

    flags                    INTEGER NOT NULL DEFAULT 0,
    dropped_attributes_count INTEGER NOT NULL DEFAULT 0,

    scope_name               TEXT,
    scope_version            TEXT,

    attributes_json          TEXT NOT NULL DEFAULT '{}',

    FOREIGN KEY (resource_id)
        REFERENCES log_resource(id)
        ON DELETE CASCADE
);

-- Primary retention/time-range access path.
CREATE INDEX IF NOT EXISTS idx_log_event_timestamp
    ON log_event(timestamp_ns);

-- Common resource-scoped time-range queries.
CREATE INDEX IF NOT EXISTS idx_log_event_resource_timestamp
    ON log_event(resource_id, timestamp_ns);

-- Trace lookup followed by chronological ordering.
CREATE INDEX IF NOT EXISTS idx_log_event_trace_timestamp
    ON log_event(trace_id, timestamp_ns)
    WHERE trace_id IS NOT NULL;

-- ============================================================================
-- 3. Log read-side view
-- ============================================================================

CREATE VIEW IF NOT EXISTS logs AS
SELECT
    le.id                       AS id,
    le.resource_id              AS resource_id,
    le.timestamp_ns             AS timestamp_ns,
    le.observed_timestamp_ns    AS observed_timestamp_ns,
    le.severity_number          AS severity_number,
    le.severity_text            AS severity_text,
    le.trace_id                 AS trace_id,
    le.span_id                  AS span_id,
    le.body                     AS body,
    le.event_name               AS event_name,
    le.flags                    AS flags,
    le.dropped_attributes_count AS dropped_attributes_count,
    le.scope_name               AS scope_name,
    le.scope_version            AS scope_version,
    le.attributes_json          AS attributes_json,
    lr.service_name             AS service_name,
    lr.host_name                AS host_name,
    lr.schema_url               AS resource_schema_url
FROM log_event AS le
JOIN log_resource AS lr
    ON lr.id = le.resource_id;

-- ============================================================================
-- 4. Log FTS5 index (DERIVED DATA)
--
-- Contentless FTS: the source text is not duplicated and the ingestion path
-- never writes here. The table is created empty; the maintenance subsystem
-- owns dropping/recreating/repopulating it from the canonical tables.
-- `id` is represented by the FTS rowid.
-- ============================================================================

CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts USING fts5(
    body,
    service_name,
    host_name,
    severity_text,
    event_name,
    scope_name,

    tokenize = 'unicode61 remove_diacritics 2',

    content = ''
);

-- ============================================================================
-- 5. Metric instrumentation scopes
-- ============================================================================

CREATE TABLE IF NOT EXISTS scope (
    id          TEXT PRIMARY KEY,

    resource_id TEXT NOT NULL,

    name        TEXT,
    version     TEXT,
    schema_url  TEXT,

    FOREIGN KEY (resource_id)
        REFERENCES log_resource(id)
        ON DELETE CASCADE,

    UNIQUE (
        resource_id,
        name,
        version,
        schema_url
    )
);

CREATE INDEX IF NOT EXISTS idx_scope_resource
    ON scope(resource_id);

-- ============================================================================
-- 6. Metric definitions
-- ============================================================================

CREATE TABLE IF NOT EXISTS metric (
    id                         TEXT PRIMARY KEY,

    scope_id                   TEXT NOT NULL,

    name                       TEXT NOT NULL,
    description                TEXT,
    unit                       TEXT,

    type                       INTEGER NOT NULL,

    is_monotonic               INTEGER NOT NULL DEFAULT 0
        CHECK (is_monotonic IN (0, 1)),

    aggregation_temporality    INTEGER NOT NULL DEFAULT 0,

    FOREIGN KEY (scope_id)
        REFERENCES scope(id)
        ON DELETE CASCADE,

    UNIQUE (
        scope_id,
        name,
        unit,
        type
    )
);

CREATE INDEX IF NOT EXISTS idx_metric_scope
    ON metric(scope_id);

-- ============================================================================
-- 7. Metric time-series identity
--
-- attributes_json must be canonicalized by the application:
-- keys sorted, deterministic serialization, no insignificant whitespace.
-- Identical attribute sets therefore produce identical series identity.
-- ============================================================================

CREATE TABLE IF NOT EXISTS metric_series (
    id              TEXT PRIMARY KEY,

    metric_id       TEXT NOT NULL,

    attributes_json TEXT NOT NULL DEFAULT '{}',

    FOREIGN KEY (metric_id)
        REFERENCES metric(id)
        ON DELETE CASCADE,

    UNIQUE (
        metric_id,
        attributes_json
    )
);

CREATE INDEX IF NOT EXISTS idx_metric_series_metric
    ON metric_series(metric_id);

-- ============================================================================
-- 8. Metric data points (append-only)
--
-- double_value / int_value are mutually exclusive.
-- Histogram / exponential histogram / summary remain lossless JSON payloads
-- until concrete query requirements justify physical normalization.
-- ============================================================================

CREATE TABLE IF NOT EXISTS metric_data_point (
    id                          INTEGER PRIMARY KEY,

    series_id                   TEXT NOT NULL,

    timestamp_ns                INTEGER NOT NULL,
    start_timestamp_ns          INTEGER,

    flags                       INTEGER NOT NULL DEFAULT 0,

    double_value                REAL,
    int_value                   INTEGER,

    count                       INTEGER,
    sum                         REAL,
    min                         REAL,
    max                         REAL,

    nan_mask                    INTEGER NOT NULL DEFAULT 0
        CHECK ((nan_mask & ~15) = 0),

    histogram_json              TEXT,
    exponential_histogram_json  TEXT,
    summary_json                TEXT,
    exemplars_json              TEXT,

    FOREIGN KEY (series_id)
        REFERENCES metric_series(id)
        ON DELETE CASCADE,

    CHECK (
        NOT (
            double_value IS NOT NULL
            AND int_value IS NOT NULL
        )
    ),

    CHECK (
        start_timestamp_ns IS NULL
        OR start_timestamp_ns <= timestamp_ns
    )
);

-- Main metric query path.
CREATE INDEX IF NOT EXISTS idx_metric_dp_series_time
    ON metric_data_point(series_id, timestamp_ns);

-- ============================================================================
-- 9. Metrics read-side view
-- ============================================================================

CREATE VIEW IF NOT EXISTS metrics AS
SELECT
    dp.id                         AS id,
    dp.series_id                  AS series_id,
    dp.timestamp_ns               AS timestamp_ns,
    dp.start_timestamp_ns         AS start_timestamp_ns,
    dp.flags                      AS flags,
    dp.double_value               AS double_value,
    dp.int_value                  AS int_value,
    dp.count                      AS count,
    dp.sum                        AS sum,
    dp.min                        AS min,
    dp.max                        AS max,
    dp.nan_mask                   AS nan_mask,
    dp.histogram_json             AS histogram_json,
    dp.exponential_histogram_json AS exponential_histogram_json,
    dp.summary_json               AS summary_json,
    dp.exemplars_json             AS exemplars_json,
    ms.attributes_json            AS series_attributes,
    m.id                          AS metric_id,
    m.name                        AS metric_name,
    m.description                 AS metric_description,
    m.unit                        AS unit,
    m.type                        AS metric_type,
    m.is_monotonic                AS is_monotonic,
    m.aggregation_temporality     AS aggregation_temporality,
    s.id                          AS scope_id,
    s.name                        AS scope_name,
    s.version                     AS scope_version,
    s.schema_url                  AS scope_schema_url,
    r.id                          AS resource_id,
    r.service_name                AS service_name,
    r.host_name                   AS host_name,
    r.schema_url                  AS resource_schema_url
FROM metric_data_point AS dp
JOIN metric_series AS ms
    ON ms.id = dp.series_id
JOIN metric AS m
    ON m.id = ms.metric_id
JOIN scope AS s
    ON s.id = m.scope_id
JOIN log_resource AS r
    ON r.id = s.resource_id;

-- ============================================================================
-- 10. Histogram bucket read-side view
--
-- histogram_json:
-- {
--   "bounds": [...],
--   "counts": [...]
-- }
-- The final count entry represents the +Inf bucket.
-- ============================================================================

CREATE VIEW IF NOT EXISTS metric_buckets AS

SELECT
    dp.id                    AS data_point_id,
    dp.series_id             AS series_id,
    dp.timestamp_ns          AS timestamp_ns,
    CAST(b.key AS INTEGER)   AS bucket_index,
    b.value                  AS bound_json,
    CASE
        WHEN json_type(b.value) IN ('integer', 'real')
        THEN CAST(b.value AS REAL)
    END                      AS bound,
    CAST(c.value AS INTEGER) AS bucket_count
FROM metric_data_point AS dp
JOIN json_each(
    dp.histogram_json,
    '$.bounds'
) AS b
JOIN json_each(
    dp.histogram_json,
    '$.counts'
) AS c
    ON c.key = b.key
WHERE dp.histogram_json IS NOT NULL

UNION ALL

SELECT
    dp.id,
    dp.series_id,
    dp.timestamp_ns,
    CAST(c.key AS INTEGER),
    '"+Inf"',
    NULL,
    CAST(c.value AS INTEGER)
FROM metric_data_point AS dp
JOIN json_each(
    dp.histogram_json,
    '$.counts'
) AS c
WHERE dp.histogram_json IS NOT NULL
AND CAST(c.key AS INTEGER) = (
    SELECT COUNT(*)
    FROM json_each(
        dp.histogram_json,
        '$.bounds'
    )
);

-- ============================================================================
-- 004 · Metric identity includes temporality and monotonicity
--
-- A delta Sum and a cumulative Sum (or a monotonic and non-monotonic Sum)
-- sharing a descriptor are distinct metrics. Their fingerprints now include
-- `is_monotonic` and `aggregation_temporality`, but the table's UNIQUE
-- constraint did not, so the second variant collided and was quarantined.
-- SQLite cannot alter a constraint in place, so rebuild the dimension table.
--
-- The rebuild runs with foreign keys disabled (the migration runner toggles
-- them off) so dropping the old table does not cascade into `metric_series`.
-- The `metrics` view references `metric` and must be dropped for the rename
-- step (SQLite re-parses view definitions when renaming) and recreated
-- afterwards.
-- ============================================================================

DROP VIEW IF EXISTS metrics;

CREATE TABLE metric_rebuild (
    id                         TEXT PRIMARY KEY,

    scope_id                   TEXT NOT NULL,

    name                       TEXT NOT NULL,
    description                TEXT,
    unit                       TEXT,

    type                       INTEGER NOT NULL,

    is_monotonic               INTEGER NOT NULL DEFAULT 0
        CHECK (is_monotonic IN (0, 1)),

    aggregation_temporality    INTEGER NOT NULL DEFAULT 0,

    metadata_json              TEXT NOT NULL DEFAULT '{}',

    FOREIGN KEY (scope_id)
        REFERENCES scope(id)
        ON DELETE CASCADE,

    UNIQUE (
        scope_id,
        name,
        unit,
        type,
        is_monotonic,
        aggregation_temporality
    )
);

INSERT INTO metric_rebuild (
    id,
    scope_id,
    name,
    description,
    unit,
    type,
    is_monotonic,
    aggregation_temporality,
    metadata_json
)
SELECT
    id,
    scope_id,
    name,
    description,
    unit,
    type,
    is_monotonic,
    aggregation_temporality,
    metadata_json
FROM metric;

DROP TABLE metric;

ALTER TABLE metric_rebuild RENAME TO metric;

CREATE INDEX IF NOT EXISTS idx_metric_scope
    ON metric(scope_id);

DROP VIEW IF EXISTS metrics;

CREATE VIEW metrics AS
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
    m.metadata_json               AS metric_metadata,
    s.id                          AS scope_id,
    s.name                        AS scope_name,
    s.version                     AS scope_version,
    s.schema_url                  AS scope_schema_url,
    s.attributes_json             AS scope_attributes,
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

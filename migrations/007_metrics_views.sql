-- Migration 007: Metrics read-side views
-- Created: 2025-08-15 (Phase 2)
-- Description: Adds the read-side `metrics` view (metric_data_point joined
--   through series -> metric -> scope -> log_resource, mirroring the `logs`
--   view from migration 003) and the `metric_buckets` view that normalizes
--   histogram buckets at query time via json_each.
--
-- Histogram bucket normalization decision (proposal §10, phase 2): buckets
-- stay in metric_data_point.histogram_json (lossless, one row per data
-- point, no write-path fan-out). Query-time normalization is done through
-- json_each in the metric_buckets view below. A dedicated
-- metric_histogram_bucket table is deferred until real query requirements
-- (e.g. aggregation across bucket counts) exist — the view makes the JSON
-- payload queryable today.
--
-- JSON float encoding note for readers: histogram bounds, exp-histogram
-- zero_threshold and summary quantiles may be JSON numbers OR string
-- markers ("NaN", "+Inf", "-Inf") — encoding/json cannot serialize
-- non-finite floats, so the mapper writes string markers. metric_buckets
-- exposes both the raw JSON (bound_json) and the numeric value when finite
-- (bound, NULL for the string markers).

-- ---------------------------------------------------------------------------
-- 1. The `metrics` view: one row per metric data point with full context.
--    This is the single logical row the query layer sees; readers select
--    from `metrics` rather than re-joining the base tables.
-- ---------------------------------------------------------------------------
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
FROM metric_data_point dp
JOIN metric_series ms ON ms.id = dp.series_id
JOIN metric m       ON m.id = ms.metric_id
JOIN scope s        ON s.id = m.scope_id
JOIN log_resource r ON r.id = s.resource_id;

-- ---------------------------------------------------------------------------
-- 2. The `metric_buckets` view: query-time normalization of histogram_json.
--    histogram_json is {"bounds":[...],"counts":[...]}; bounds and counts
--    are parallel arrays, so bounds[k] is the upper bound of bucket k with
--    count counts[k]. OTLP bucket counts carry one MORE entry than bounds
--    (the final "overflow" bucket above the last bound), so the view also
--    emits that row with an implicit +Inf bound. json_each.key is the
--    array index.
-- ---------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS metric_buckets AS
SELECT
    dp.id                         AS data_point_id,
    dp.series_id                  AS series_id,
    dp.timestamp_ns               AS timestamp_ns,
    CAST(b.key AS INTEGER)        AS bucket_index,
    b.value                       AS bound_json,
    CASE WHEN json_type(b.value) IN ('integer', 'real')
         THEN CAST(b.value AS REAL) END AS bound,
    CAST(c.value AS INTEGER)      AS bucket_count
FROM metric_data_point dp
JOIN json_each(dp.histogram_json, '$.bounds') b
JOIN json_each(dp.histogram_json, '$.counts') c ON c.key = b.key
WHERE dp.histogram_json IS NOT NULL
UNION ALL
-- The overflow bucket: counts[len(bounds)] has no explicit bound.
SELECT
    dp.id,
    dp.series_id,
    dp.timestamp_ns,
    CAST(c.key AS INTEGER),
    '"+Inf"',
    NULL,
    CAST(c.value AS INTEGER)
FROM metric_data_point dp
JOIN json_each(dp.histogram_json, '$.counts') c
WHERE dp.histogram_json IS NOT NULL
  AND CAST(c.key AS INTEGER) = (
      SELECT COUNT(*) FROM json_each(dp.histogram_json, '$.bounds')
  );

-- Record this migration
INSERT OR IGNORE INTO schema_migrations (version, description)
VALUES ('007', 'Metrics read views (metrics, metric_buckets via json_each)');

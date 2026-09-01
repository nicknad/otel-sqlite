-- ============================================================================
-- 003 · Mapping fidelity (P0-3)
--
-- Stop discarding supported OTLP fields on their way into storage:
--   * log_event gains the scope's attributes and schema URL (scope is
--     denormalized onto the event for logs, matching scope_name/version),
--   * the shared scope dimension table gains an attributes column so metric
--     scopes keep their attributes,
--   * the metric dimension table gains a metadata column,
--   * exemplars already had metric_data_point.exemplars_json (unused until
--     now); the read-side views are recreated to expose the new columns.
-- ============================================================================

ALTER TABLE log_event
    ADD COLUMN scope_attributes_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE log_event
    ADD COLUMN scope_schema_url TEXT;

ALTER TABLE scope
    ADD COLUMN attributes_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE metric
    ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}';

-- ============================================================================
-- Recreate the read-side views to expose the new columns.
-- ============================================================================

DROP VIEW IF EXISTS logs;

CREATE VIEW logs AS
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
    le.scope_attributes_json    AS scope_attributes_json,
    le.scope_schema_url         AS scope_schema_url,
    le.attributes_json          AS attributes_json,
    lr.service_name             AS service_name,
    lr.host_name                AS host_name,
    lr.schema_url               AS resource_schema_url
FROM log_event AS le
JOIN log_resource AS lr
    ON lr.id = le.resource_id;

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
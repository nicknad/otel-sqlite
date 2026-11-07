-- ============================================================================
-- 002: Metric retention access path
--
-- Retention prunes delete metric data points by age
-- (`DELETE FROM metric_data_point WHERE timestamp_ns < ?`), mirroring the
-- log-event retention index. Without this index every prune degrades to a
-- full table scan of the append-only point table.
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_metric_dp_timestamp
    ON metric_data_point(timestamp_ns);

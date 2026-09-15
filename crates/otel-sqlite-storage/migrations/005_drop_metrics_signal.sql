-- ============================================================================
-- 005 · Drop the OTLP metrics signal
--
-- The sink is logs-only: the metric tables, their read-side views and indexes
-- have no writers or readers left. Views are dropped first, then tables in
-- child-to-parent order so the implicit DELETE that `DROP TABLE` performs
-- under `foreign_keys = ON` never sees rows referencing a live parent.
--
-- This is irreversible: metric rows stored by earlier versions are deleted
-- (operators take a backup before upgrading; upgrades are forward-only).
-- Log tables, the `logs` view and the `logs_fts` index are untouched.
-- ============================================================================

DROP VIEW IF EXISTS metrics;
DROP VIEW IF EXISTS metric_buckets;

DROP TABLE IF EXISTS metric_data_point;
DROP TABLE IF EXISTS metric_series;
DROP TABLE IF EXISTS metric;
DROP TABLE IF EXISTS scope;

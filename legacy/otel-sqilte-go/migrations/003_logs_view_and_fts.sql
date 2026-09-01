-- Migration 003: Logs View and Contentless FTS5 Index
-- Created: 2026-07-01
-- Description: Introduces the read-side `logs` view (log_event + log_resource
--   join) and the contentless `logs_fts` full-text index built on top of it.
--
-- This migration supersedes the trigger-based full-text index created in
-- migration 002. Per the project constraints, NO triggers are created: the
-- `logs_fts` index is kept in sync by an explicit rebuild performed by the
-- maintenance tool (package migrate), never by SQLite triggers or by the
-- ingestion pipeline. This keeps the write path free of FTS overhead and the
-- read-only sidecar free of write access.
--
-- Design notes:
--   * A contentless FTS5 table (content='') is used so the index can be built
--     from a VIEW rather than a real base table carrying its own rowid. The
--     index stores only tokens; rowids are supplied explicitly as logs.id
--     during rebuild.
--   * Contentless FTS5 does not support DELETE, so the only canonical way to
--     clear it is DROP + CREATE, which the maintenance tool performs inside a
--     single transaction. This migration therefore only creates the empty
--     index; population is the maintenance tool's responsibility.

-- ---------------------------------------------------------------------------
-- 1. Retire the legacy trigger-based full-text index from migration 002.
--    Triggers depend on `log_event`; drop them before dropping the FTS table.
-- ---------------------------------------------------------------------------
DROP TRIGGER IF EXISTS log_event_fts_delete;
DROP TRIGGER IF EXISTS log_event_fts_update;
DROP TRIGGER IF EXISTS log_event_fts_insert;
DROP TABLE IF EXISTS log_event_fts;

-- ---------------------------------------------------------------------------
-- 2. Create the read-side `logs` view joining log_event + log_resource.
--
--    This is the single logical row the query sidecar and the FTS index see:
--      id           -- log_event.id (the FTS rowid during rebuild)
--      body         -- log_event.body (indexed token stream)
--      service_name -- log_resource.service_name (indexed token stream)
--    Additional columns used by the query side are also exposed here so the
--    view remains the one place that owns the join shape; readers select from
--    `logs` rather than re-joining the base tables.
-- ---------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS logs AS
SELECT
    le.id                AS id,
    le.resource_id      AS resource_id,
    le.timestamp_ns     AS timestamp_ns,
    le.observed_timestamp_ns AS observed_timestamp_ns,
    le.severity_number   AS severity_number,
    le.severity_text     AS severity_text,
    le.trace_id          AS trace_id,
    le.span_id           AS span_id,
    le.body              AS body,
    le.event_name        AS event_name,
    le.flags             AS flags,
    le.dropped_attributes_count AS dropped_attributes_count,
    le.scope_name        AS scope_name,
    le.scope_version     AS scope_version,
    lr.service_name      AS service_name,
    lr.host_name         AS host_name,
    lr.schema_url        AS schema_url
FROM log_event le
JOIN log_resource lr ON le.resource_id = lr.id;

-- ---------------------------------------------------------------------------
-- 3. Create the contentless FTS5 index over the `logs` view.
--
--    content='' makes this a contentless table: SQLite stores only the token
--    inverted index, not the source text. Population happens via
--      INSERT INTO logs_fts(rowid, body, service_name)
--      SELECT id, body, service_name FROM logs;
--    issued by the maintenance tool (migrate.RebuildFTS). Because the table is
--    contentless there is no rowid column to maintain incrementally and no
--    DELETE support; the tool drops + recreates it within one transaction to
--    rebuild idempotently.
-- ---------------------------------------------------------------------------
CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts USING fts5(
    body,
    service_name,
    content=''
);

-- Optional helper index so common equality lookups on service_name against
-- log_resource stay cheap even outside the FTS path.
CREATE INDEX IF NOT EXISTS idx_log_resource_service_name
    ON log_resource(service_name);

-- Record this migration
INSERT OR IGNORE INTO schema_migrations (version, description)
VALUES ('003', 'Logs view (log_event+log_resource join) and contentless logs_fts index; retire trigger-based FTS from 002');
-- ============================================================================
-- OTEL SQLite Storage
-- Squashed logs-only baseline
--
-- This file is the canonical schema for a brand-new database. It folds the
-- historical 000-005 upgrade chain into the schema those migrations actually
-- produced, without ever creating the metrics signal that 005 used to drop.
--
-- The frozen upgrade history lives in `migrations/upgrades/`. Existing
-- databases replay it; fresh databases only execute this file and are then
-- stamped as already satisfying every version through `005`.
--
-- Design principles:
--   * SQLite WAL + single writer
--   * canonical data tables contain no FTS maintenance overhead
--   * FTS5 is contentless and maintained incrementally by triggers
--   * log attributes are stored inline as JSON
--   * resources are shared by every log event
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
--
-- Scope attributes and schema URL are denormalized onto the event, matching
-- scope_name/scope_version. Their position after `attributes_json` matches
-- the effective column order produced by historical migration 003.
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

    scope_attributes_json    TEXT NOT NULL DEFAULT '{}',
    scope_schema_url         TEXT,

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
    le.scope_attributes_json    AS scope_attributes_json,
    le.scope_schema_url         AS scope_schema_url,
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
-- Contentless FTS with contentless_delete so individual rows can be removed
-- by rowid. The ingestion path never writes here directly: triggers keep the
-- index consistent with insert and delete (including retention prunes).
--
-- The trigger bodies mirror `FTS_SCHEMA` in maintenance.rs on purpose:
-- migrations are frozen snapshots of the schema they produce, while that
-- constant must recreate the identical shape for full rebuilds. Change both
-- together or not at all.
-- ============================================================================

CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts USING fts5(
    body,
    service_name,
    host_name,
    severity_text,
    event_name,
    scope_name,

    tokenize = 'unicode61 remove_diacritics 2',

    content = '',
    contentless_delete = 1
);

-- logs_fts_ai = "after insert": index every new log event immediately so it is
-- searchable without waiting for a rebuild.
CREATE TRIGGER logs_fts_ai AFTER INSERT ON log_event BEGIN
    INSERT INTO logs_fts (rowid, body, service_name, host_name, severity_text, event_name, scope_name)
    SELECT
        new.id,
        COALESCE(new.body, ''),
        COALESCE(lr.service_name, ''),
        COALESCE(lr.host_name, ''),
        COALESCE(new.severity_text, ''),
        COALESCE(new.event_name, ''),
        COALESCE(new.scope_name, '')
    FROM log_resource AS lr
    WHERE lr.id = new.resource_id;
END;

-- logs_fts_ad = "after delete": remove a deleted event's index row so retention
-- prunes can never leave stale search hits behind.
CREATE TRIGGER logs_fts_ad AFTER DELETE ON log_event BEGIN
    DELETE FROM logs_fts WHERE rowid = old.id;
END;

-- No-op on a truly fresh database. Kept in the baseline so this file is a
-- faithful replacement for historical migration 001, including its backfill.
INSERT INTO logs_fts (rowid, body, service_name, host_name, severity_text, event_name, scope_name)
SELECT
    le.id,
    COALESCE(le.body, ''),
    COALESCE(lr.service_name, ''),
    COALESCE(lr.host_name, ''),
    COALESCE(le.severity_text, ''),
    COALESCE(le.event_name, ''),
    COALESCE(le.scope_name, '')
FROM log_event AS le
JOIN log_resource AS lr
    ON lr.id = le.resource_id;

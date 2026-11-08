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
-- by rowid. The index is optional: the configured mode is reconciled at
-- startup, and the state row below records the installed shape and rebuild
-- progress. The default indexes the body only; the insert trigger needs no
-- log_resource join.
--
-- The trigger bodies mirror `crate::search` on purpose: that module recreates
-- the identical shape for mode changes and full rebuilds. Change both
-- together or not at all.
-- ============================================================================

CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts USING fts5(
    body,
    tokenize = 'unicode61 remove_diacritics 2',
    content = '',
    contentless_delete = 1
);

-- logs_fts_ai = "after insert": index every new log event immediately so it is
-- searchable without waiting for a rebuild.
CREATE TRIGGER logs_fts_ai AFTER INSERT ON log_event BEGIN
    INSERT INTO logs_fts (rowid, body)
    VALUES (new.id, COALESCE(new.body, ''));
END;

-- logs_fts_ad = "after delete": remove a deleted event's index row so retention
-- prunes can never leave stale search hits behind.
CREATE TRIGGER logs_fts_ad AFTER DELETE ON log_event BEGIN
    DELETE FROM logs_fts WHERE rowid = old.id;
END;

-- Installed search-index state. A fresh database has no rows to rebuild, so it
-- starts ready; an upgraded database starts rebuilding in `upgrades/006`.
CREATE TABLE IF NOT EXISTS search_index_state (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    mode          TEXT    NOT NULL CHECK (mode IN ('off', 'body', 'full')),
    status        TEXT    NOT NULL CHECK (status IN ('ready', 'rebuilding')),
    last_rowid    INTEGER NOT NULL DEFAULT 0,
    rebuild_until INTEGER,
    updated_at    TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (mode = 'off'
            AND status = 'ready'
            AND last_rowid = 0
            AND rebuild_until IS NULL)
        OR
        (mode <> 'off'
            AND (
                (status = 'ready' AND rebuild_until IS NULL)
                OR
                (status = 'rebuilding'
                    AND rebuild_until IS NOT NULL
                    AND rebuild_until >= last_rowid)
            ))
    )
);

INSERT INTO search_index_state (id, mode, status, last_rowid, rebuild_until)
VALUES (1, 'body', 'ready', 0, NULL);

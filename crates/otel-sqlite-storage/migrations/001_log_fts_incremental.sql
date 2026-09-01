-- ============================================================================
-- Migration 001: keep the log search index consistent with the canonical
-- table.
--
-- Problem with the 000 baseline: `logs_fts` was contentless FTS5 that only
-- maintenance rebuilt. Inserts never touched it (new logs were invisible to
-- searches until an explicit rebuild) and retention prunes left ghost rows
-- in it pointing at deleted `log_event` entries.
--
-- Fix, without duplicating body text into the index:
--   * recreate `logs_fts` as contentless FTS5 with `contentless_delete = 1`
--     so individual rows can be removed by rowid;
--   * AFTER INSERT / AFTER DELETE triggers on `log_event` maintain the
--     index incrementally — inserts become searchable immediately, and a
--     prune can no longer leave stale entries behind;
--   * backfill once from the canonical tables so existing rows are covered.
--
-- The trigger bodies mirror `FTS_SCHEMA` in crates/otel-sqlite-storage's
-- maintenance.rs on purpose: migrations are frozen snapshots of the schema
-- they produce, while that constant must recreate the identical shape for
-- full rebuilds. Change both together or not at all.
-- ============================================================================

DROP TABLE IF EXISTS logs_fts;

CREATE VIRTUAL TABLE logs_fts USING fts5(
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

-- logs_fts_ai = "after insert": whenever a log_event row is inserted, index it
-- immediately so new logs are searchable without waiting for a rebuild.
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

-- logs_fts_ad = "after delete": whenever a log_event row is deleted (e.g.
-- retention prune), remove its index entry so no ghost rows are left behind.
CREATE TRIGGER logs_fts_ad AFTER DELETE ON log_event BEGIN
    DELETE FROM logs_fts WHERE rowid = old.id;
END;

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

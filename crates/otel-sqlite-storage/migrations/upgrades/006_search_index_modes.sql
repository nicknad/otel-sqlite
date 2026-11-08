-- ============================================================================
-- 006 · Search index becomes an optional, reconciled derived index
--
-- The index is derived data, so its shape is reconciled from configuration at
-- startup instead of being fixed by a migration. This upgrade installs the
-- reduced default shape (body only) and marks existing rows for the bounded,
-- resumable background rebuild. It deliberately does not scan log_event: a
-- full backfill inside one migration transaction is unbounded and would
-- crash-loop against the startup budget.
--
-- `search_index_state` is the installed-state record. `status = 'rebuilding'`
-- means the background rebuild has not covered every pre-existing row yet.
-- ============================================================================

DROP TRIGGER IF EXISTS logs_fts_ai;
DROP TRIGGER IF EXISTS logs_fts_ad;
DROP TABLE IF EXISTS logs_fts;

CREATE VIRTUAL TABLE logs_fts USING fts5(
    body,
    tokenize = 'unicode61 remove_diacritics 2',
    content = '',
    contentless_delete = 1
);

CREATE TRIGGER logs_fts_ai AFTER INSERT ON log_event BEGIN
    INSERT INTO logs_fts (rowid, body)
    VALUES (new.id, COALESCE(new.body, ''));
END;

CREATE TRIGGER logs_fts_ad AFTER DELETE ON log_event BEGIN
    DELETE FROM logs_fts WHERE rowid = old.id;
END;

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
SELECT
    1,
    'body',
    CASE WHEN COALESCE(MAX(id), 0) > 0 THEN 'rebuilding' ELSE 'ready' END,
    0,
    CASE WHEN COALESCE(MAX(id), 0) > 0 THEN MAX(id) ELSE NULL END
FROM log_event;

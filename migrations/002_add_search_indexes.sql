-- Migration 002: Add Search Indexes
-- Created: 2024-01-02
-- Description: Adds additional indexes for search functionality

-- Full-text search index on log body (SQLite FTS5)
-- Note: This creates a virtual table for full-text search
CREATE VIRTUAL TABLE IF NOT EXISTS log_event_fts USING fts5(
    id UNINDEXED,
    resource_id UNINDEXED,
    body,
    severity_text UNINDEXED,
    event_name UNINDEXED,
    tokenize="unicode61 remove_diacritics 2"
);

-- Trigger to keep FTS table in sync with main table
CREATE TRIGGER IF NOT EXISTS log_event_fts_insert AFTER INSERT ON log_event
BEGIN
    INSERT INTO log_event_fts(rowid, resource_id, body, severity_text, event_name) 
    VALUES (new.id, new.resource_id, new.body, new.severity_text, new.event_name);
END;

CREATE TRIGGER IF NOT EXISTS log_event_fts_update AFTER UPDATE ON log_event
BEGIN
    UPDATE log_event_fts SET 
        resource_id = new.resource_id,
        body = new.body,
        severity_text = new.severity_text,
        event_name = new.event_name
    WHERE rowid = old.id;
END;

CREATE TRIGGER IF NOT EXISTS log_event_fts_delete AFTER DELETE ON log_event
BEGIN
    DELETE FROM log_event_fts WHERE rowid = old.id;
END;

-- Additional indexes for attribute search
CREATE INDEX IF NOT EXISTS idx_log_attr_value_string ON log_attr(string_value) WHERE string_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_log_attr_value_int ON log_attr(int_value) WHERE int_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_log_attr_value_double ON log_attr(double_value) WHERE double_value IS NOT NULL;

-- Index for event name
CREATE INDEX IF NOT EXISTS idx_log_event_event_name ON log_event(event_name);

-- Record this migration
INSERT OR IGNORE INTO schema_migrations (version, description) VALUES ('002', 'Add search indexes and full-text search');

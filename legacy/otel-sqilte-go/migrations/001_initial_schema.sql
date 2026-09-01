-- Migration 001: Initial Schema
-- Created: 2024-01-01
-- Description: Creates the initial database schema for OTLP log storage

-- Log resources table
CREATE TABLE IF NOT EXISTS log_resource (
    id TEXT PRIMARY KEY,
    service_name TEXT NOT NULL,
    host_name TEXT,
    schema_url TEXT,
    attributes_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Log events table
CREATE TABLE IF NOT EXISTS log_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_id TEXT NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    observed_timestamp_ns INTEGER NOT NULL,
    severity_number INTEGER NOT NULL,
    severity_text TEXT,
    trace_id BLOB,
    span_id BLOB,
    body TEXT,
    event_name TEXT,
    flags INTEGER NOT NULL,
    dropped_attributes_count INTEGER NOT NULL,
    scope_name TEXT,
    scope_version TEXT,
    FOREIGN KEY (resource_id) REFERENCES log_resource(id)
);

-- Log attributes table
CREATE TABLE IF NOT EXISTS log_attr (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value_type TEXT NOT NULL,
    string_value TEXT,
    int_value INTEGER,
    double_value REAL,
    bool_value INTEGER,
    bytes_value BLOB,
    FOREIGN KEY (event_id) REFERENCES log_event(id) ON DELETE CASCADE
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_log_event_timestamp ON log_event(timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_log_event_severity ON log_event(severity_number);
CREATE INDEX IF NOT EXISTS idx_log_event_trace_id ON log_event(trace_id);
CREATE INDEX IF NOT EXISTS idx_log_event_resource_id ON log_event(resource_id);
CREATE INDEX IF NOT EXISTS idx_log_attr_event_id ON log_attr(event_id);
CREATE INDEX IF NOT EXISTS idx_log_attr_key ON log_attr(key);
CREATE INDEX IF NOT EXISTS idx_log_event_severity_text ON log_event(severity_text);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_log_event_resource_timestamp ON log_event(resource_id, timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_log_event_trace_timestamp ON log_event(trace_id, timestamp_ns);

-- Create migration tracking table
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

-- Record this migration
INSERT OR IGNORE INTO schema_migrations (version, description) VALUES ('001', 'Initial schema for OTLP log storage');

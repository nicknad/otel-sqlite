// Package sqlite provides SQLite storage for log records.
package sqlite

import (
	"database/sql"
	"fmt"
	"log"
)

// migration represents a single database migration.
type migration struct {
	version     string
	description string
	sql         string
	apply       func(*sql.Tx) error
}

// allMigrations returns all known migrations in application order.
func allMigrations() []migration {
	return []migration{
		{
			version:     "001",
			description: "Initial schema for OTLP log storage",
			sql:         migration001SQL,
		},
		{
			version:     "002",
			description: "Add search indexes and full-text search",
			sql:         migration002SQL,
		},
		{
			version: "003",
			description: "Logs view (log_event+log_resource join) and contentless logs_fts index; " +
				"retire trigger-based FTS from 002",
			sql: migration003SQL,
		},
		{
			version:     "004",
			description: "Remove unused indexes to improve write performance",
			sql:         migration004SQL,
		},
		{
			version:     "005",
			description: "Inline event attributes as compact JSON and remove log_attr",
			sql:         migration005SQL,
			apply:       applyMigration005,
		},
		{
			version:     "006",
			description: "Metrics schema (scope, metric, metric_series, metric_data_point)",
			sql:         migration006SQL,
		},
	}
}

// RunMigrations applies all pending migrations to the database.
// It creates the schema_migrations tracking table if it does not exist,
// checks which migrations have already been applied, and runs any
// outstanding ones in order.
func RunMigrations(db *sql.DB) error {
	// Ensure the migration tracking table exists first so we can record
	// what has been applied.  This is the same table that migration 001
	// also creates (CREATE TABLE IF NOT EXISTS, so it is idempotent).
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		description TEXT
	)`); err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	// Query already-applied migrations.
	applied := make(map[string]bool)
	rows, err := db.Query(`SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("query applied migrations: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return fmt.Errorf("scan migration version: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate applied migrations: %w", err)
	}

	// Apply pending migrations in order.
	for _, m := range allMigrations() {
		if applied[m.version] {
			continue
		}

		log.Printf("Applying migration %s: %s", m.version, m.description)

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", m.version, err)
		}

		apply := m.apply
		if apply == nil {
			apply = func(tx *sql.Tx) error {
				_, err := tx.Exec(m.sql)
				return err
			}
		}
		if err := apply(tx); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %s failed: %w", m.version, err)
		}

		// Record the migration in the same transaction as its schema/data
		// changes so a failed migration can be retried safely.
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO schema_migrations (version, description) VALUES (?, ?)`,
			m.version, m.description,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s: %w", m.version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", m.version, err)
		}

		log.Printf("Migration %s applied successfully", m.version)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Migration SQL
// ---------------------------------------------------------------------------

// migration001SQL creates the initial database schema for OTLP log storage.
// This replaces the former inline initializeSchema function and is identical
// to migrations/001_initial_schema.sql minus the schema_migrations
// DDL/DML, which is handled by RunMigrations.
const migration001SQL = `
CREATE TABLE IF NOT EXISTS log_resource (
    id TEXT PRIMARY KEY,
    service_name TEXT NOT NULL,
    host_name TEXT,
    schema_url TEXT,
    attributes_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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

CREATE INDEX IF NOT EXISTS idx_log_event_timestamp ON log_event(timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_log_event_severity ON log_event(severity_number);
CREATE INDEX IF NOT EXISTS idx_log_event_trace_id ON log_event(trace_id);
CREATE INDEX IF NOT EXISTS idx_log_event_resource_id ON log_event(resource_id);
CREATE INDEX IF NOT EXISTS idx_log_attr_event_id ON log_attr(event_id);
CREATE INDEX IF NOT EXISTS idx_log_attr_key ON log_attr(key);
CREATE INDEX IF NOT EXISTS idx_log_event_severity_text ON log_event(severity_text);

CREATE INDEX IF NOT EXISTS idx_log_event_resource_timestamp ON log_event(resource_id, timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_log_event_trace_timestamp ON log_event(trace_id, timestamp_ns);
`

// migration002SQL adds full-text search via FTS5 and additional indexes
// for search functionality.  Identical to migrations/002_add_search_indexes.sql.
const migration002SQL = `
CREATE VIRTUAL TABLE IF NOT EXISTS log_event_fts USING fts5(
    id UNINDEXED,
    resource_id UNINDEXED,
    body,
    severity_text UNINDEXED,
    event_name UNINDEXED,
    tokenize="unicode61 remove_diacritics 2"
);

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

CREATE INDEX IF NOT EXISTS idx_log_attr_value_string ON log_attr(string_value) WHERE string_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_log_attr_value_int ON log_attr(int_value) WHERE int_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_log_attr_value_double ON log_attr(double_value) WHERE double_value IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_log_event_event_name ON log_event(event_name);
`

// migration003SQL retires the trigger-based full-text index from 002 and
// introduces the read-side logs view + contentless FTS5 index.
// Identical to migrations/003_logs_view_and_fts.sql.
const migration003SQL = `
DROP TRIGGER IF EXISTS log_event_fts_delete;
DROP TRIGGER IF EXISTS log_event_fts_update;
DROP TRIGGER IF EXISTS log_event_fts_insert;
DROP TABLE IF EXISTS log_event_fts;

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

CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts USING fts5(
    body,
    service_name,
    content=''
);

CREATE INDEX IF NOT EXISTS idx_log_resource_service_name
    ON log_resource(service_name);
`

// migration004SQL removes indexes that are not used by any queries,
// improving write performance.  Identical to migrations/004_remove_unused_indexes.sql.
const migration004SQL = `
DROP INDEX IF EXISTS idx_log_event_severity;
DROP INDEX IF EXISTS idx_log_event_trace_id;
DROP INDEX IF EXISTS idx_log_event_severity_text;
DROP INDEX IF EXISTS idx_log_event_resource_timestamp;
DROP INDEX IF EXISTS idx_log_event_trace_timestamp;
DROP INDEX IF EXISTS idx_log_event_event_name;

DROP INDEX IF EXISTS idx_log_attr_key;
DROP INDEX IF EXISTS idx_log_attr_value_string;
DROP INDEX IF EXISTS idx_log_attr_value_int;
DROP INDEX IF EXISTS idx_log_attr_value_double;
`

// migration005SQL is the DDL contract for the inline event attribute
// migration. The Go hook executes it only when the column is absent, then
// performs the typed backfill and removes the legacy EAV table in the same
// transaction.
const migration005SQL = `
ALTER TABLE log_event ADD COLUMN attributes_json TEXT NOT NULL DEFAULT '{}';
`

// migration006SQL creates the metrics storage model (scope, metric,
// metric_series, metric_data_point). Identical to
// migrations/006_metrics.sql minus the schema_migrations DML, which is
// handled by RunMigrations. All DDL is idempotent (IF NOT EXISTS).
const migration006SQL = `
CREATE TABLE IF NOT EXISTS scope (
    id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL,
    name TEXT,
    version TEXT,
    schema_url TEXT,
    FOREIGN KEY (resource_id) REFERENCES log_resource(id)
);

CREATE TABLE IF NOT EXISTS metric (
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    unit TEXT,
    type INTEGER NOT NULL,
    is_monotonic INTEGER NOT NULL DEFAULT 0,
    aggregation_temporality INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (scope_id) REFERENCES scope(id)
);

CREATE TABLE IF NOT EXISTS metric_series (
    id TEXT PRIMARY KEY,
    metric_id TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    FOREIGN KEY (metric_id) REFERENCES metric(id)
);

CREATE TABLE IF NOT EXISTS metric_data_point (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id TEXT NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    start_timestamp_ns INTEGER,
    flags INTEGER NOT NULL DEFAULT 0,
    double_value REAL,
    int_value INTEGER,
    count INTEGER,
    sum REAL,
    min REAL,
    max REAL,
    nan_mask INTEGER NOT NULL DEFAULT 0, -- bits: 1=double_value, 2=sum, 4=min, 8=max are NaN
    histogram_json TEXT,
    exponential_histogram_json TEXT,
    summary_json TEXT,
    exemplars_json TEXT,
    FOREIGN KEY (series_id) REFERENCES metric_series(id)
);

CREATE INDEX IF NOT EXISTS idx_metric_dp_series_time
    ON metric_data_point(series_id, timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_metric_series_metric
    ON metric_series(metric_id);
CREATE INDEX IF NOT EXISTS idx_metric_scope
    ON metric(scope_id);
`

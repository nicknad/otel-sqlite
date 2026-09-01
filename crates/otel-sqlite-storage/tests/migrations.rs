// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

//! Upgrade tests: every supported schema version must
//! migrate forward to the current schema through the production `Storage::open`
//! path — with data preserved, the search index backfilled, integrity and
//! foreign keys clean, and the read-side views exposing the current columns.
//!
//! Historical databases are built with the same migration machinery the
//! production writer uses (`migrate_up_to`), then seeded with era-appropriate
//! data, then reopened by `Storage::open` exactly as a real upgrade would.

use std::time::Duration;

use crossbeam_channel::unbounded;
use otel_sqlite_storage::{Storage, StorageConfig, migrate_up_to};
use rusqlite::Connection;

/// A database built and stamped at an old schema version, seeded with data
/// valid for that era, ready for `Storage::open` to upgrade.
fn historic_db(path: &std::path::Path, version: &str) {
    let mut conn = Connection::open(path).expect("open db");
    migrate_up_to(&mut conn, version).expect("build historic schema");
    seed(&conn);
    drop(conn);
}

/// Inserts representative logs and metrics using only columns that exist in
/// every schema version (000-era columns; 003 columns are added later).
fn seed(conn: &Connection) {
    conn.execute_batch(
        "INSERT INTO log_resource (id, service_name, host_name, schema_url, attributes_json)
             VALUES ('res-1', 'checkout', 'host-a', NULL, '{}');
         INSERT INTO log_event
             (resource_id, timestamp_ns, observed_timestamp_ns, severity_number,
              severity_text, body, event_name, scope_name, scope_version, attributes_json)
             VALUES ('res-1', 1000, 1500, 17, 'ERROR', 'payment failed', 'order.failed',
                     'scope-a', '1.0.0', '{\"attempt\":2}');
         INSERT INTO log_event
             (resource_id, timestamp_ns, observed_timestamp_ns, severity_number,
              severity_text, body, scope_name, attributes_json)
             VALUES ('res-1', 2000, 2000, 9, 'INFO', 'all good', 'scope-a', '{}');
         INSERT INTO scope (id, resource_id, name, version, schema_url)
             VALUES ('scope-1', 'res-1', 'scope-m', '0.9.9', NULL);
         INSERT INTO metric (id, scope_id, name, description, unit, type, is_monotonic,
                             aggregation_temporality)
             VALUES ('metric-1', 'scope-1', 'requests.total', 'count', '1', 3, 1, 1);
         INSERT INTO metric_series (id, metric_id, attributes_json)
             VALUES ('series-1', 'metric-1', '{}');
         INSERT INTO metric_data_point (series_id, timestamp_ns, start_timestamp_ns,
                                        double_value, int_value)
             VALUES ('series-1', 300, 100, NULL, 42);",
    )
    .expect("seed historic data");
}

/// Reopens the database through the production writer so the full migration
/// path to the current schema runs, then joins cleanly.
fn upgrade_via_storage_open(path: &std::path::Path) {
    let (sender, receiver) = unbounded();
    let mut storage = Storage::open(
        receiver,
        StorageConfig {
            sqlite_path: path.to_path_buf(),
            insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
                1_000,
                Duration::from_millis(20),
            ),
            ..StorageConfig::default()
        },
    )
    .expect("storage opens over the historic database");
    drop(sender);
    storage.join().expect("storage joins cleanly");
}

fn scalar(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

/// Asserts the post-upgrade database: current schema stamp, preserved data,
/// backfilled search index, clean integrity/foreign keys, and the 003 columns
/// visible on the read-side views.
fn assert_upgraded(path: &std::path::Path) {
    let conn = Connection::open(path).expect("open upgraded db");

    let version: String = conn
        .query_row(
            "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "003", "upgrade must land on the current schema");

    let applied: Vec<String> = conn
        .prepare("SELECT version FROM schema_migrations ORDER BY version")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(
        applied.windows(2).all(|pair| pair[0] < pair[1]),
        "migration stamps must be applied in order: {applied:?}"
    );

    // Data preserved.
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM log_event"), 2);
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM log_resource"), 1);
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM scope"), 1);
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM metric"), 1);
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM metric_series"), 1);
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM metric_data_point"), 1);
    let body: String = conn
        .query_row("SELECT body FROM log_event WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(body, "payment failed");
    let point_value: i64 = conn
        .query_row("SELECT int_value FROM metric_data_point", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(point_value, 42);

    // The search index is backfilled and searchable without a rebuild.
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM logs_fts"), 2);
    let matches: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM logs_fts WHERE logs_fts MATCH 'payment'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(matches, 1);

    // Integrity and foreign keys.
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    let fk_violations: i64 = conn
        .prepare("PRAGMA foreign_key_check")
        .unwrap()
        .query_map([], |_| Ok(()))
        .unwrap()
        .count() as i64;
    assert_eq!(fk_violations, 0);

    // 003 columns exist and the read-side views expose them with defaults.
    let scope_attributes: String = conn
        .query_row(
            "SELECT scope_attributes_json FROM logs WHERE body = 'payment failed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(scope_attributes, "{}");
    let metric_metadata: String = conn
        .query_row("SELECT metric_metadata FROM metrics", [], |row| row.get(0))
        .unwrap();
    assert_eq!(metric_metadata, "{}");
}

/// A database stamped at version 000 upgrades to the current schema with its
/// (untouched, pre-trigger) search index fully backfilled.
#[test]
fn database_stamped_000_upgrades_through_current_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("otel-logs.db");
    historic_db(&db, "000");

    // The 000 schema has no incremental FTS triggers yet.
    let conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM logs_fts"), 0);
    drop(conn);

    upgrade_via_storage_open(&db);
    assert_upgraded(&db);
}

/// A database stamped at version 001 (incremental FTS already installed)
/// upgrades to the current schema.
#[test]
fn database_stamped_001_upgrades_through_current_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("otel-logs.db");
    historic_db(&db, "001");

    // 001 installed the insert triggers, so seeding already indexed the rows.
    let conn = Connection::open(&db).unwrap();
    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM logs_fts"), 2);
    drop(conn);

    upgrade_via_storage_open(&db);
    assert_upgraded(&db);
}

/// A database stamped at version 002 upgrades to the current schema.
#[test]
fn database_stamped_002_upgrades_through_current_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("otel-logs.db");
    historic_db(&db, "002");
    upgrade_via_storage_open(&db);
    assert_upgraded(&db);
}

/// Reopening a current-schema database is a no-op: the stamp stays 003 and the
/// data is untouched — upgrades are idempotent for already-current databases.
#[test]
fn current_schema_reopen_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("otel-logs.db");
    historic_db(&db, "003");
    upgrade_via_storage_open(&db);
    upgrade_via_storage_open(&db);
    assert_upgraded(&db);
}

/// A capped migration run refuses to go beyond its target version: building a
/// version-000 database must not apply 001-003 even though the SQL is present.
#[test]
fn migrate_up_to_stops_at_the_target_version() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("otel-logs.db");
    historic_db(&db, "000");

    let conn = Connection::open(&db).unwrap();
    let applied: Vec<String> = conn
        .prepare("SELECT version FROM schema_migrations ORDER BY version")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        applied,
        vec!["000"],
        "only the target version may be stamped"
    );
    drop(conn);
}

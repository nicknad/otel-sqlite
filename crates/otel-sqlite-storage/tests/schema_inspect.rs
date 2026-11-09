//! Read-only schema inspection: `inspect_schema` must report migration state
//! without creating tables, applying SQL, or touching the file.

use otel_sqlite_storage::backup::{open_immutable, open_readonly};
use otel_sqlite_storage::{inspect_schema, latest_schema_version, migrate_up_to};
use rusqlite::Connection;

fn read_only_inspect(path: &std::path::Path) -> otel_sqlite_storage::SchemaStatus {
    let conn = open_readonly(path).expect("open read-only");
    inspect_schema(&conn).expect("inspect schema")
}

#[test]
fn empty_database_reports_every_migration_pending_and_is_not_otel() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.db");
    Connection::open(&path).expect("create empty database");

    let status = read_only_inspect(&path);

    assert_eq!(status.latest_known, latest_schema_version());
    assert_eq!(status.applied.len(), 0);
    assert_ne!(status.pending.len(), 0);
    assert_eq!(
        status.pending.last().copied(),
        Some(latest_schema_version())
    );
    assert!(!status.newer_than_binary);
    assert!(
        !status.is_otel_schema,
        "an empty file has no log_event table"
    );
}

#[test]
fn inspection_does_not_create_the_migration_table_or_change_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("untouched.db");
    Connection::open(&path).expect("create empty database");

    let before = std::fs::read(&path).expect("read before");
    let status = read_only_inspect(&path);
    let after = std::fs::read(&path).expect("read after");

    assert_eq!(before, after, "inspection must not modify the file");
    assert!(
        status.applied.is_empty(),
        "no migrations can be stamped by inspection"
    );

    let conn = Connection::open(&path).expect("reopen");
    let tables: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'",
            [],
            |row| row.get(0),
        )
        .expect("count tables");
    assert_eq!(tables, 0, "inspection must not create schema_migrations");
}

#[test]
fn historic_schema_reports_the_next_migration_as_pending() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("historic.db");
    let mut conn = Connection::open(&path).expect("open");
    migrate_up_to(&mut conn, "004").expect("build 004 schema");
    drop(conn);

    let status = read_only_inspect(&path);

    assert_eq!(status.applied.last().map(String::as_str), Some("004"));
    assert_eq!(status.pending, vec!["005", "006"]);
    assert!(!status.newer_than_binary);
    assert!(status.is_otel_schema);
}

#[test]
fn current_schema_has_nothing_pending() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("current.db");
    let mut conn = Connection::open(&path).expect("open");
    migrate_up_to(&mut conn, latest_schema_version()).expect("build current schema");
    drop(conn);

    let status = read_only_inspect(&path);

    assert_eq!(status.pending.len(), 0);
    assert!(!status.newer_than_binary);
    assert!(status.is_otel_schema);
    assert_eq!(
        status.applied.last().map(String::as_str),
        Some(latest_schema_version())
    );
}

#[test]
fn newer_schema_is_flagged_as_newer_than_the_binary() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("newer.db");
    let mut conn = Connection::open(&path).expect("open");
    migrate_up_to(&mut conn, latest_schema_version()).expect("build current schema");
    conn.execute(
        "INSERT INTO schema_migrations (version, description) VALUES ('999', 'future')",
        [],
    )
    .expect("stamp future migration");
    drop(conn);

    let status = read_only_inspect(&path);

    assert!(status.newer_than_binary);
    assert_eq!(status.applied.last().map(String::as_str), Some("999"));
    assert_eq!(status.pending.len(), 0);
    assert!(status.is_otel_schema);
}

#[test]
fn immutable_open_leaves_no_wal_or_shm_sidecars() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("wal.db");
    {
        let mut conn = Connection::open(&path).expect("open");
        conn.pragma_update(None, "journal_mode", "WAL")
            .expect("enable WAL");
        migrate_up_to(&mut conn, latest_schema_version()).expect("build current schema");
    }
    // Closing the last connection checkpoints and removes the sidecars.
    assert!(!dir.path().join("wal.db-wal").exists());
    assert!(!dir.path().join("wal.db-shm").exists());

    let before = std::fs::read(&path).expect("read before");
    let conn = open_immutable(&path).expect("open immutable");
    let status = inspect_schema(&conn).expect("inspect");
    drop(conn);
    let after = std::fs::read(&path).expect("read after");

    assert_eq!(status.pending.len(), 0);
    assert_eq!(before, after, "inspection must not modify the file");
    assert!(
        !dir.path().join("wal.db-wal").exists(),
        "immutable inspection must not create a WAL"
    );
    assert!(
        !dir.path().join("wal.db-shm").exists(),
        "immutable inspection must not create a shared-memory file"
    );
}

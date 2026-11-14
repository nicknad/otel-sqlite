use rusqlite::Connection;

use crate::error::StorageError;

/// Frozen upgrade history, in ascending version order.
///
/// New databases do not execute these files: [`BASELINE_SQL`] already
/// produces the schema this ladder ends at. They remain the forward-upgrade
/// path for databases stamped by an older binary, and the source for
/// [`migrate_up_to`]'s historical snapshots.
const MIGRATIONS: &[Migration] = &[
    Migration {
        version: "000",
        description: "Canonical OTLP SQLite schema: logs, resources, contentless FTS5, metrics and read-side views",
        sql: include_str!("../migrations/upgrades/000_baseline.sql"),
        disable_foreign_keys: false,
    },
    Migration {
        version: "001",
        description: "Incrementally maintained log search index: triggers keep FTS5 consistent with inserts and retention prunes",
        sql: include_str!("../migrations/upgrades/001_log_fts_incremental.sql"),
        disable_foreign_keys: false,
    },
    Migration {
        version: "002",
        description: "Metric retention access path: timestamp index for age-based data-point prunes",
        sql: include_str!("../migrations/upgrades/002_metric_retention_index.sql"),
        disable_foreign_keys: false,
    },
    Migration {
        version: "003",
        description: "Mapping fidelity: scope attributes/schema URL on log events and scopes, metric metadata, exemplar persistence columns exposed on the read-side views",
        sql: include_str!("../migrations/upgrades/003_mapping_fidelity.sql"),
        disable_foreign_keys: false,
    },
    Migration {
        version: "004",
        description: "Metric identity includes is_monotonic and aggregation_temporality in the dimension UNIQUE constraint (table rebuild)",
        sql: include_str!("../migrations/upgrades/004_metric_identity_temporality.sql"),
        disable_foreign_keys: true,
    },
    Migration {
        version: "005",
        description: "Logs-only sink: drop metric tables, views and indexes",
        sql: include_str!("../migrations/upgrades/005_drop_metrics_signal.sql"),
        disable_foreign_keys: false,
    },
    Migration {
        version: "006",
        description: "Search index becomes an optional, reconciled derived index with a reduced default column set",
        sql: include_str!("../migrations/upgrades/006_search_index_modes.sql"),
        disable_foreign_keys: false,
    },
];

/// Newest schema version this baseline is equivalent to.
///
/// A fresh database executes [`BASELINE_SQL`] once and is stamped as satisfying
/// every migration up to and including this version. Keep this equal to the
/// last legacy migration that the baseline folds in; a unit test enforces it.
const BASELINE_VERSION: &str = "006";

/// Complete logs-only schema for a brand-new database.
///
/// It is the frozen result of the historical ladder at [`BASELINE_VERSION`],
/// with the metric signal removed rather than created and then dropped, plus
/// the reduced, trigger-maintained search index.
const BASELINE_SQL: &str = include_str!("../migrations/baseline_006.sql");

/// Stamp text for versions satisfied by [`BASELINE_SQL`]. Their historical
/// DDL never ran, so this deliberately does not reuse the per-version
/// descriptions that mention the metric schema.
const BASELINE_DESCRIPTION: &str = "Satisfied by squashed logs-only baseline";

struct Migration {
    version: &'static str,
    description: &'static str,
    sql: &'static str,
    /// Run with `PRAGMA foreign_keys = OFF`. Required for table rebuilds that
    /// drop a parent table: with FKs on, `DROP TABLE` would cascade into the
    /// child rows before the rebuilt table is renamed into place.
    disable_foreign_keys: bool,
}

/// Read-only view of an existing database's migration state.
///
/// Produced by [`inspect_schema`] so a preflight (or any operator tool) can
/// answer "will this binary open this database, and what would it change?"
/// without creating tables or applying migrations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaStatus {
    /// Newest schema version this binary knows about.
    pub latest_known: &'static str,
    /// Applied migration versions, ascending.
    pub applied: Vec<String>,
    /// Versions this binary would apply on the next open, in order.
    pub pending: Vec<&'static str>,
    /// The database carries a stamp newer than [`Self::latest_known`], so this
    /// binary must refuse to open it.
    pub newer_than_binary: bool,
    /// The canonical `log_event` table exists; distinguishes otel-sqlite
    /// storage from an arbitrary SQLite file.
    pub is_otel_schema: bool,
}

/// Inspects migration state **without mutating the database**: it never
/// creates `schema_migrations` and never applies SQL.
///
/// A database without a `schema_migrations` table reports no applied versions
/// and every known migration as pending; [`SchemaStatus::is_otel_schema`]
/// tells whether the file looks like otel-sqlite storage at all.
pub fn inspect_schema(conn: &Connection) -> Result<SchemaStatus, StorageError> {
    let mut applied = read_applied_versions(conn)?;
    applied.sort();

    let latest_known = latest_schema_version();
    let newer_than_binary = applied
        .last()
        .is_some_and(|version| version.as_str() > latest_known);
    let pending = MIGRATIONS
        .iter()
        .filter(|migration| !applied.iter().any(|version| version == migration.version))
        .map(|migration| migration.version)
        .collect();

    Ok(SchemaStatus {
        latest_known,
        applied,
        pending,
        newer_than_binary,
        is_otel_schema: table_exists(conn, "log_event")?,
    })
}

/// Newest schema version compiled into this binary.
pub fn latest_schema_version() -> &'static str {
    MIGRATIONS
        .last()
        .map_or_default(|migration| migration.version)
}

/// Message for a database stamped by a binary newer than this one.
pub fn version_too_new_message(applied: &str, latest_known: &str) -> String {
    format!(
        "database schema version {applied} is newer than the newest supported \
         version {latest_known}; upgrade this binary before opening it"
    )
}

/// Whether `name` is a table in the opened database. Uses `sqlite_master`
/// directly so a missing table is data, not an error.
pub(crate) fn table_exists(conn: &Connection, name: &str) -> rusqlite::Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [name],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

/// Reads the applied migration stamps without assuming the table exists.
fn read_applied_versions(conn: &Connection) -> Result<Vec<String>, StorageError> {
    if !table_exists(conn, "schema_migrations")? {
        return Ok(Vec::new());
    }
    let mut statement = conn.prepare("SELECT version FROM schema_migrations")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

pub(crate) fn migrate(conn: &mut Connection) -> Result<(), StorageError> {
    migrate_inner(conn, None)
}

/// Applies the migrations up to and including `target_version`; versions after
/// it are skipped even when unapplied. Used by the upgrade tests to build
/// databases stamped at historical schema versions.
#[doc(hidden)]
pub fn migrate_up_to(conn: &mut Connection, target_version: &str) -> Result<(), StorageError> {
    migrate_inner(conn, Some(target_version))
}

fn migrate_inner(conn: &mut Connection, target_version: Option<&str>) -> Result<(), StorageError> {
    // Probe before creating anything: a database is fresh only when it has
    // neither migration stamps nor the canonical log table. An existing
    // otel-sqlite database with missing stamps is legacy, not fresh.
    let mut applied: Vec<String> = read_applied_versions(conn)?;
    let fresh = applied.is_empty() && !table_exists(conn, "log_event")?;

    // Downgrade guard: an older binary must refuse to run against a database
    // stamped by a newer one. Proceeding would silently write a schema that
    // the newer binary cannot reason about (or worse, misread).
    let newest_known = latest_schema_version();
    if let Some(newest_applied) = applied.iter().max()
        && newest_applied.as_str() > newest_known
    {
        return Err(StorageError::StartupFailed {
            message: version_too_new_message(newest_applied, newest_known),
        });
    }

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version     TEXT PRIMARY KEY,
            applied_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        );",
    )?;

    // A fresh database goes straight to the squashed baseline, unless the
    // caller is explicitly building a historical snapshot below it.
    let baseline_eligible = target_version.is_none_or(|target| target >= BASELINE_VERSION);
    if fresh && baseline_eligible {
        apply_baseline(conn)?;
        applied = read_applied_versions(conn)?;
    }

    for migration in MIGRATIONS {
        if applied.iter().any(|version| version == migration.version) {
            continue;
        }
        // A capped run stops before any migration beyond its target: an
        // upgrade test that builds version `002` must not silently apply
        // `003`.
        if target_version.is_some_and(|target| migration.version > target) {
            continue;
        }

        if migration.disable_foreign_keys {
            conn.pragma_update(None, "foreign_keys", "OFF")?;
        }
        let tx = conn.transaction()?;
        tx.execute_batch(migration.sql)?;
        tx.execute(
            "INSERT INTO schema_migrations (version, description) VALUES (?1, ?2)",
            rusqlite::params![migration.version, migration.description],
        )?;
        tx.commit()?;
        if migration.disable_foreign_keys {
            conn.pragma_update(None, "foreign_keys", "ON")?;
        }

        tracing::info!(version = migration.version, "applied schema migration");
    }

    Ok(())
}

/// Creates a brand-new database from [`BASELINE_SQL`] and records every
/// migration version the baseline satisfies.
///
/// One transaction: a crash leaves the database empty rather than half
/// created, so the next open retries the same baseline path.
fn apply_baseline(conn: &mut Connection) -> Result<(), StorageError> {
    let tx = conn.transaction()?;
    tx.execute_batch(BASELINE_SQL)?;
    for migration in MIGRATIONS {
        if migration.version > BASELINE_VERSION {
            break;
        }
        tx.execute(
            "INSERT INTO schema_migrations (version, description) VALUES (?1, ?2)",
            rusqlite::params![migration.version, BASELINE_DESCRIPTION],
        )?;
    }
    tx.commit()?;

    tracing::info!(
        version = BASELINE_VERSION,
        "applied squashed logs-only schema baseline"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_versions_are_strictly_ascending_and_unique() {
        for pair in MIGRATIONS.windows(2) {
            assert!(
                pair[0].version < pair[1].version,
                "migration versions must ascend: {} then {}",
                pair[0].version,
                pair[1].version
            );
        }
    }

    #[test]
    fn baseline_version_is_part_of_the_migration_ladder() {
        assert_eq!(
            MIGRATIONS.last().map(|migration| migration.version),
            Some(BASELINE_VERSION),
            "baseline version {BASELINE_VERSION} must name the newest migration"
        );
    }

    #[test]
    fn baseline_sql_is_logs_only_and_folds_in_fts_and_scope_columns() {
        for metric_object in [
            "metric_data_point",
            "metric_series",
            "CREATE TABLE metric",
            "CREATE VIEW metrics",
            "CREATE TABLE scope",
        ] {
            assert!(
                !BASELINE_SQL.contains(metric_object),
                "baseline must not contain metric object `{metric_object}`"
            );
        }
        assert!(
            BASELINE_SQL.contains("contentless_delete = 1"),
            "baseline must create the incremental FTS index"
        );
        assert!(
            BASELINE_SQL.contains("logs_fts_ai") && BASELINE_SQL.contains("logs_fts_ad"),
            "baseline must install both FTS maintenance triggers"
        );
        assert!(
            BASELINE_SQL.contains("search_index_state"),
            "baseline must install the search-index state row"
        );
        assert!(
            BASELINE_SQL.contains("scope_attributes_json")
                && BASELINE_SQL.contains("scope_schema_url"),
            "baseline must fold in the log scope columns"
        );
    }

    #[test]
    fn upgrade_006_defers_the_backfill_and_marks_the_index_rebuilding() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("upgrade.db");
        let mut conn = Connection::open(&path).expect("open database");
        migrate_up_to(&mut conn, "005").expect("build a 005 database");

        conn.execute(
            "INSERT INTO log_resource (id, service_name) VALUES ('r', 'checkout')",
            [],
        )
        .expect("seed resource");
        conn.execute(
            "INSERT INTO log_event
                 (resource_id, timestamp_ns, observed_timestamp_ns, severity_number, body)
             VALUES ('r', 1, 1, 9, 'payment failed')",
            [],
        )
        .expect("seed log");

        let indexed: i64 = conn
            .query_row("SELECT COUNT(*) FROM logs_fts", [], |row| row.get(0))
            .expect("count legacy index");
        assert_eq!(indexed, 1, "the 005 index tracks the seeded row");

        migrate(&mut conn).expect("apply 006");

        let indexed: i64 = conn
            .query_row("SELECT COUNT(*) FROM logs_fts", [], |row| row.get(0))
            .expect("count reduced index");
        assert_eq!(indexed, 0, "006 must not backfill inside the migration");

        let (mode, status): (String, String) = conn
            .query_row(
                "SELECT mode, status FROM search_index_state WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("read state");
        assert_eq!(mode, "body");
        assert_eq!(status, "rebuilding");

        let canonical: i64 = conn
            .query_row("SELECT COUNT(*) FROM log_event", [], |row| row.get(0))
            .expect("count logs");
        assert_eq!(canonical, 1, "canonical rows survive the upgrade");
    }
}

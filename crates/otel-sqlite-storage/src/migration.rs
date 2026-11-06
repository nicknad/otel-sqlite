use rusqlite::Connection;

use crate::error::StorageError;

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: "000",
        description: "Canonical OTLP SQLite schema: logs, resources, contentless FTS5, metrics and read-side views",
        sql: include_str!("../migrations/000_baseline.sql"),
        disable_foreign_keys: false,
    },
    Migration {
        version: "001",
        description: "Incrementally maintained log search index: triggers keep FTS5 consistent with inserts and retention prunes",
        sql: include_str!("../migrations/001_log_fts_incremental.sql"),
        disable_foreign_keys: false,
    },
    Migration {
        version: "002",
        description: "Metric retention access path: timestamp index for age-based data-point prunes",
        sql: include_str!("../migrations/002_metric_retention_index.sql"),
        disable_foreign_keys: false,
    },
    Migration {
        version: "003",
        description: "Mapping fidelity: scope attributes/schema URL on log events and scopes, metric metadata, exemplar persistence columns exposed on the read-side views",
        sql: include_str!("../migrations/003_mapping_fidelity.sql"),
        disable_foreign_keys: false,
    },
    Migration {
        version: "004",
        description: "Metric identity includes is_monotonic and aggregation_temporality in the dimension UNIQUE constraint (table rebuild)",
        sql: include_str!("../migrations/004_metric_identity_temporality.sql"),
        disable_foreign_keys: true,
    },
    Migration {
        version: "005",
        description: "Logs-only sink: drop metric tables, views and indexes",
        sql: include_str!("../migrations/005_drop_metrics_signal.sql"),
        disable_foreign_keys: false,
    },
];

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

    let latest_known = newest_known();
    let newest_applied = applied.last().cloned().unwrap_or_default();
    let newer_than_binary = newest_applied.as_str() > latest_known;
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

fn newest_known() -> &'static str {
    MIGRATIONS
        .last()
        .map(|migration| migration.version)
        .unwrap_or_default()
}

/// Newest schema version compiled into this binary.
pub fn latest_schema_version() -> &'static str {
    newest_known()
}

/// Whether `name` is a table in the opened database. Uses `sqlite_master`
/// directly so a missing table is data, not an error.
fn table_exists(conn: &Connection, name: &str) -> Result<bool, StorageError> {
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
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version     TEXT PRIMARY KEY,
            applied_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        );",
    )?;

    let applied: Vec<String> = read_applied_versions(conn)?;

    // Downgrade guard: an older binary must refuse to run against a database
    // stamped by a newer one. Proceeding would silently write a schema that
    // the newer binary cannot reason about (or worse, misread).
    let newest_known = newest_known();
    if let Some(newest_applied) = applied.iter().max()
        && newest_applied.as_str() > newest_known
    {
        return Err(StorageError::StartupFailed {
            message: format!(
                "database schema version {newest_applied} is newer than the newest supported \
                 version {newest_known}; upgrade this binary before opening it"
            ),
        });
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

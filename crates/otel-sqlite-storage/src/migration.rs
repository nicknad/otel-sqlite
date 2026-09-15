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

    let applied: Vec<String> = {
        let mut statement = conn.prepare("SELECT version FROM schema_migrations")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    // Downgrade guard: an older binary must refuse to run against a database
    // stamped by a newer one. Proceeding would silently write a schema that
    // the newer binary cannot reason about (or worse, misread).
    let newest_known = MIGRATIONS
        .last()
        .map(|migration| migration.version)
        .unwrap_or_default();
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

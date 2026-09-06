use std::path::Path;

use otel_sqlite_core::storage::SyncMode;
use rusqlite::Connection;

use crate::error::StorageError;

pub(crate) fn open(path: &Path, synchronous: SyncMode) -> Result<Connection, StorageError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }

    let existed = path.exists();
    let conn = Connection::open(path)?;

    // Telemetry often contains PII/secrets: the live database must be
    // owner-only from creation, mirroring backup artifacts.
    if let Err(error) = crate::permissions::restrict_permissions(path) {
        tracing::warn!(
            path = %path.display(),
            %error,
            "could not restrict database file permissions"
        );
    }
    if existed {
        crate::permissions::warn_if_world_readable(path, "sqlite database file");
    }

    let journal_mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    tracing::debug!(journal_mode = journal_mode.as_str(), "sqlite journal mode");

    conn.pragma_update(None, "synchronous", synchronous.as_str())?;
    tracing::debug!(
        synchronous = synchronous.as_str(),
        "sqlite synchronous mode"
    );
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;

    // Throughput-oriented defaults for the 500-row JSON+FTS transaction
    // profile: 64 MiB page cache (default 2 MiB spills), in-memory temp
    // sorts for ANALYZE/prune, 256 MiB mmap for reads, 64 MiB WAL cap so
    // checkpoints stay bounded. All best-effort: failures fall back to
    // SQLite defaults rather than failing startup.
    for (pragma, value) in [
        ("cache_size", "-64000"),
        ("temp_store", "MEMORY"),
        ("mmap_size", "268435456"),
        ("journal_size_limit", "67108864"),
    ] {
        if let Err(error) = conn.pragma_update(None, pragma, value) {
            tracing::debug!(pragma, %error, "sqlite pragma left at default");
        }
    }
    // Prefer incremental vacuum for future maintenance: full VACUUM rewrites
    // the entire file under an exclusive lock (seconds on GB DBs). This only
    // takes effect for new databases (or after one converting VACUUM); see
    // `maintenance::vacuum` for the migration path.
    if let Err(error) = conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;") {
        tracing::debug!(%error, "auto_vacuum left at default");
    }

    Ok(conn)
}

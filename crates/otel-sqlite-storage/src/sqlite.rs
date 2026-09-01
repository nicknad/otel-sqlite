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

    let conn = Connection::open(path)?;

    let journal_mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    tracing::debug!(journal_mode = journal_mode.as_str(), "sqlite journal mode");

    conn.pragma_update(None, "synchronous", synchronous.as_str())?;
    tracing::debug!(
        synchronous = synchronous.as_str(),
        "sqlite synchronous mode"
    );
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;

    Ok(conn)
}

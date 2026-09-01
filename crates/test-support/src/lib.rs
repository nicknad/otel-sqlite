//! Shared helpers for integration and unit tests across the workspace.
//!
//! Keeping these in one place avoids copy-pasted polling and database-open
//! logic drifting apart between crates. Not for production use.

use std::path::Path;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OpenFlags};

/// Polls `predicate` every few milliseconds until it returns `true` or
/// `timeout` elapses. Returns whether the predicate succeeded in time.
pub fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let started = Instant::now();
    while !predicate() {
        if started.elapsed() >= timeout {
            return false;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    true
}

/// Opens a SQLite database in read-only mode.
///
/// # Panics
/// Panics when the database cannot be opened (tests assert on persisted
/// state, so a broken open is fatal anyway).
pub fn open_readonly(db_path: &Path) -> Connection {
    Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .expect("database readable")
}

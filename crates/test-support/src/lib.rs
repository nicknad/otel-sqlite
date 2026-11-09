//! Shared helpers for integration and unit tests across the workspace.
//!
//! Keeping these in one place avoids copy-pasted polling and database-open
//! logic drifting apart between crates. Not for production use.

use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
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

/// Polls a TCP endpoint until a connection succeeds or `timeout` elapses.
///
/// `host_port` is a `host:port` pair (no URL scheme). Name resolution is done
/// once; returns `false` on an invalid address or when nothing accepts within
/// the budget.
pub fn wait_for_tcp(host_port: &str, timeout: Duration) -> bool {
    let Some(addr) = host_port
        .to_socket_addrs()
        .ok()
        .and_then(|mut addresses| addresses.next())
    else {
        return false;
    };
    wait_until(timeout, || {
        TcpStream::connect_timeout(&addr, Duration::from_millis(250)).is_ok()
    })
}

/// Opens a SQLite database in read-only mode.
///
/// # Panics
/// Panics when the database cannot be opened (tests assert on persisted
/// state, so a broken open is fatal anyway).
pub fn open_readonly(db_path: &Path) -> Connection {
    try_open_readonly(db_path).expect("database readable")
}

/// Fallible variant of [`open_readonly`] for callers that attach their own
/// context to the failure.
pub fn try_open_readonly(db_path: &Path) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
}

/// SQLite write-ahead-log sidecar path (`<db>-wal`).
///
/// Appending the suffix matches SQLite's naming for any database path,
/// including extensionless ones, unlike `Path::with_extension`.
pub fn wal_path(db_path: &Path) -> PathBuf {
    let mut os = db_path.as_os_str().to_owned();
    os.push("-wal");
    PathBuf::from(os)
}

/// File length in bytes, or zero when the file does not exist.
pub fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |meta| meta.len())
}

/// Whether the log search index finished its background rebuild.
pub fn search_index_ready(db_path: &Path) -> bool {
    Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .ok()
        .and_then(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM search_index_state
                  WHERE id = 1 AND status = 'ready'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .ok()
        })
        .is_some_and(|ready| ready == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_path_appends_sidecar_suffix_without_replacing_extensions() {
        assert_eq!(wal_path(Path::new("logs.db")), PathBuf::from("logs.db-wal"));
        assert_eq!(wal_path(Path::new("logs")), PathBuf::from("logs-wal"));
        assert_eq!(
            wal_path(Path::new("/tmp/a.b/logs")),
            PathBuf::from("/tmp/a.b/logs-wal")
        );
    }

    #[test]
    fn file_len_is_zero_for_missing_files_and_reports_real_bytes() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("payload");
        assert_eq!(file_len(&path), 0);
        std::fs::write(&path, b"hello").expect("write payload");
        assert_eq!(file_len(&path), 5);
    }

    #[test]
    fn wait_for_tcp_detects_open_and_closed_ports() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        assert!(wait_for_tcp(&addr, Duration::from_secs(2)));

        let closed = {
            let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
            probe.local_addr().expect("addr")
        };
        assert!(!wait_for_tcp(
            &closed.to_string(),
            Duration::from_millis(100)
        ));
    }
}

//! Read-only SQLite inspection of a finished benchmark database.
//!
//! Nothing here touches the hot path: callers open the database only after
//! producers stopped and the pipeline drained. Captured pragmas, physical
//! sizes and row counts feed the benchmark reports and the drain poll.

use std::path::Path;

use anyhow::Context;
use rusqlite::{Connection, OpenFlags};

use crate::generator::RUN_ID_KEY;

#[derive(Debug, Clone, serde::Serialize)]
pub struct StorageInfo {
    pub journal_mode: String,
    pub synchronous: i64,
    pub busy_timeout_ms: i64,
    pub cache_size: i64,
    pub temp_store: i64,
    pub foreign_keys: i64,
    pub page_size: i64,
    pub sqlite_version: String,
    pub database_bytes: u64,
    pub wal_bytes: u64,
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |meta| meta.len())
}

pub fn open_readonly(db_path: &Path) -> anyhow::Result<Connection> {
    Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open {}", db_path.display()))
}

/// Capture the exact SQLite configuration and physical sizes of the database
/// produced by a benchmark run.
pub fn inspect(db_path: &Path) -> anyhow::Result<StorageInfo> {
    let conn = open_readonly(db_path)?;
    let pragma_i64 = |name: &str| -> anyhow::Result<i64> {
        Ok(conn.query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))?)
    };

    Ok(StorageInfo {
        journal_mode: conn.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?,
        synchronous: pragma_i64("synchronous")?,
        busy_timeout_ms: pragma_i64("busy_timeout")?,
        cache_size: pragma_i64("cache_size")?,
        temp_store: pragma_i64("temp_store")?,
        foreign_keys: pragma_i64("foreign_keys")?,
        page_size: pragma_i64("page_size")?,
        sqlite_version: rusqlite::version().to_owned(),
        database_bytes: file_len(db_path),
        wal_bytes: file_len(&db_path.with_extension("db-wal")),
    })
}

/// Total log rows currently persisted (any run).
pub fn count_all_log_events(db_path: &Path) -> anyhow::Result<u64> {
    let conn = open_readonly(db_path)?;
    let count: u64 = conn.query_row("SELECT COUNT(*) FROM log_event", [], |row| {
        row.get::<_, i64>(0).map(|value| value.max(0) as u64)
    })?;
    Ok(count)
}

/// Poll until every row scoped to `run_id` is persisted, proving the writer
/// has drained everything accepted during the run.
///
/// Success conditions:
///
/// * `target_rows` given: the count reaches it (the precise, embedded-mode
///   signal — callers pass the exact number of accepted records).
/// * No target given (external mode): the count is stable for longer than the
///   writer's batch age, which means commits stopped arriving because there
///   is nothing left to commit.
///
/// Expiry of `timeout` is always an **error**: a drain that neither reached
/// its target nor stabilized means the pipeline failed to persist accepted
/// records (dead worker, wedged writer, undersized budget). Silently
/// returning a partial count here would degrade a pipeline failure into
/// confusing "missing sequences" validation noise downstream.
pub fn wait_until_drained(
    db_path: &Path,
    run_id: &str,
    target_rows: Option<u64>,
    timeout: std::time::Duration,
) -> anyhow::Result<u64> {
    let started = std::time::Instant::now();
    let mut last_count = count_for_run(db_path, run_id)?;
    let mut last_change = std::time::Instant::now();

    loop {
        if let Some(target) = target_rows {
            if last_count >= target {
                return Ok(last_count);
            }
        } else if last_change.elapsed() >= std::time::Duration::from_millis(1_500) && last_count > 0
        {
            // Stable for longer than the writer's 1s flush interval.
            return Ok(last_count);
        }
        if started.elapsed() > timeout {
            anyhow::bail!(
                "drain budget expired after {:.1}s: {} rows persisted for run '{run_id}'{} \
                 (last change {:.1}s ago); the pipeline did not drain — inspect storage \
                 health/counters before trusting this database",
                started.elapsed().as_secs_f64(),
                last_count,
                target_rows.map_or_else(String::new, |target| format!(" of {target} expected")),
                last_change.elapsed().as_secs_f64(),
            );
        }

        std::thread::sleep(std::time::Duration::from_millis(200));
        let current = count_for_run(db_path, run_id)?;
        if current != last_count {
            last_count = current;
            last_change = std::time::Instant::now();
        }
    }
}

fn count_for_run(db_path: &Path, run_id: &str) -> anyhow::Result<u64> {
    let conn = open_readonly(db_path)?;
    let count: u64 = conn.query_row(
        "SELECT COUNT(*) FROM log_event \
         WHERE json_extract(attributes_json, ?2) = ?1",
        rusqlite::params![run_id, format!("$.\"{RUN_ID_KEY}\"")],
        |row| row.get::<_, i64>(0).map(|value| value.max(0) as u64),
    )?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    /// Minimal stand-in for the migrated schema: `wait_until_drained` only
    /// reads `log_event.attributes_json` through `count_for_run`.
    fn fixture_db(rows: &[(&str, u64)]) -> anyhow::Result<PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "otel-sqlite-e2e-dbtest-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir)?;
        let db_path = dir.join("otel-logs.db");
        let conn = Connection::open(&db_path)?;
        conn.execute_batch("CREATE TABLE log_event (attributes_json TEXT NOT NULL);")?;
        let insert = |run_id: &str| -> anyhow::Result<()> {
            conn.execute(
                "INSERT INTO log_event (attributes_json) VALUES (json_object(?1, ?2))",
                rusqlite::params![RUN_ID_KEY, run_id],
            )?;
            Ok(())
        };
        for (run_id, rows_for_run) in rows {
            for _ in 0..*rows_for_run {
                insert(run_id)?;
            }
        }
        drop(conn);
        Ok(db_path)
    }

    #[test]
    fn reaches_target_and_returns_count() {
        let db_path = fixture_db(&[("run-a", 40)]).expect("fixture db");
        let drained = wait_until_drained(&db_path, "run-a", Some(40), Duration::from_secs(5))
            .expect("target reachable");
        assert_eq!(drained, 40);
    }

    #[test]
    fn unreachable_target_is_an_error_not_a_partial_ok() {
        let db_path = fixture_db(&[("run-b", 20)]).expect("fixture db");
        let outcome = wait_until_drained(&db_path, "run-b", Some(50), Duration::from_millis(700));
        let error = outcome.expect_err("short drain must not bless missing rows");
        assert!(
            error.to_string().contains("drain budget expired"),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string().contains("of 50 expected"),
            "error must name the target: {error}"
        );
    }

    #[test]
    fn stability_without_target_returns_early() {
        let db_path = fixture_db(&[("run-c", 30)]).expect("fixture db");
        let started = std::time::Instant::now();
        let drained = wait_until_drained(&db_path, "run-c", None, Duration::from_secs(30))
            .expect("stable counts must succeed");
        assert_eq!(drained, 30);
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "stability path must return long before the budget expires"
        );
    }
}

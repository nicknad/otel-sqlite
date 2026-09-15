use std::path::Path;

use otel_sqlite_core::storage::{CheckpointMode, RetentionPolicy};
use rusqlite::Connection;

use crate::error::StorageError;

/// Schema of the log search index, including the sync triggers that keep it
/// consistent with `log_event`.
///
/// This mirrors migration `001_log_fts_incremental.sql` on purpose:
/// `rebuild_fts` drops and recreates the table, so it must recreate the
/// identical trigger shape the migration installed. Change both together.
const FTS_SCHEMA: &str = "
CREATE VIRTUAL TABLE logs_fts USING fts5(
    body,
    service_name,
    host_name,
    severity_text,
    event_name,
    scope_name,

    tokenize = 'unicode61 remove_diacritics 2',

    content = '',
    contentless_delete = 1
);

CREATE TRIGGER logs_fts_ai AFTER INSERT ON log_event BEGIN
    INSERT INTO logs_fts (rowid, body, service_name, host_name, severity_text, event_name, scope_name)
    SELECT
        new.id,
        COALESCE(new.body, ''),
        COALESCE(lr.service_name, ''),
        COALESCE(lr.host_name, ''),
        COALESCE(new.severity_text, ''),
        COALESCE(new.event_name, ''),
        COALESCE(new.scope_name, '')
    FROM log_resource AS lr
    WHERE lr.id = new.resource_id;
END;

CREATE TRIGGER logs_fts_ad AFTER DELETE ON log_event BEGIN
    DELETE FROM logs_fts WHERE rowid = old.id;
END;";

pub(crate) fn checkpoint(conn: &Connection, mode: CheckpointMode) -> Result<(), StorageError> {
    let sql = match mode {
        CheckpointMode::Passive => "PRAGMA wal_checkpoint(PASSIVE);",
        CheckpointMode::Full => "PRAGMA wal_checkpoint(FULL);",
        CheckpointMode::Restart => "PRAGMA wal_checkpoint(RESTART);",
        CheckpointMode::Truncate => "PRAGMA wal_checkpoint(TRUNCATE);",
    };
    conn.execute_batch(sql)?;
    Ok(())
}

/// Rows deleted per prune transaction. Bounds WAL growth and lock hold time
/// for large retention backlogs; a crash between batches resumes cleanly.
const PRUNE_BATCH_ROWS: usize = 5_000;

pub(crate) fn analyze(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch("ANALYZE;")?;
    Ok(())
}

pub(crate) fn vacuum(conn: &Connection) -> Result<(), StorageError> {
    // Prefer incremental vacuum: frees freelist pages without rewriting the
    // entire file, so ingestion stalls for milliseconds, not seconds. Only
    // effective when `auto_vacuum = INCREMENTAL` (set for new databases in
    // `sqlite::open`); legacy databases convert once via a full VACUUM and
    // are incremental thereafter.
    if auto_vacuum_mode(conn) == INCREMENTAL_AUTO_VACUUM {
        incremental_vacuum(conn)
    } else {
        convert_to_incremental_vacuum(conn)
    }
}

/// `PRAGMA auto_vacuum` value for `INCREMENTAL`.
const INCREMENTAL_AUTO_VACUUM: i64 = 2;

fn auto_vacuum_mode(conn: &Connection) -> i64 {
    conn.query_row("PRAGMA auto_vacuum", [], |row| row.get(0))
        .unwrap_or(0)
}

/// One-time conversion for databases created before incremental auto-vacuum
/// was enabled at creation. Until this runs, `incremental_vacuum` reclaims
/// nothing, so quota eviction would delete rows without ever shrinking the
/// file.
fn convert_to_incremental_vacuum(conn: &Connection) -> Result<(), StorageError> {
    tracing::info!("converting database to incremental auto-vacuum (one-time full VACUUM)");
    conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL; VACUUM;")?;
    Ok(())
}

/// Reclaims the whole freelist.
///
/// `PRAGMA incremental_vacuum` emits one zero-column row per page it
/// reclaims, and `Connection::execute_batch` steps a statement exactly once,
/// so the naive form frees a single page per call. Stepping until
/// `SQLITE_DONE` reclaims everything the freelist holds.
fn incremental_vacuum(conn: &Connection) -> Result<(), StorageError> {
    let mut statement = conn.prepare("PRAGMA incremental_vacuum;")?;
    let mut rows = statement.query([])?;
    while rows.next()?.is_some() {}
    Ok(())
}

/// What one prune pass deleted, per table.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PruneReport {
    pub log_events: usize,
    pub metric_points: usize,
    /// Dimension rows orphaned by the prune and garbage-collected with it.
    pub metric_series: usize,
    pub metrics: usize,
    pub scopes: usize,
    pub resources: usize,
}

impl PruneReport {
    pub(crate) const fn total(&self) -> usize {
        self.log_events + self.metric_points
    }

    pub(crate) const fn dimensions_removed(&self) -> usize {
        self.metric_series + self.metrics + self.scopes + self.resources
    }
}

/// Deletes expired log events and metric data points according to the
/// per-signal retention windows, then garbage-collects the dimension rows the
/// deletions orphaned.
///
/// Deletes run in bounded batches (one transaction per batch) so a large
/// retention backlog never holds a single giant transaction open: each batch
/// commits separately and a crash mid-prune resumes cleanly on the next
/// interval. Orphan-dimension cleanup runs once at the end when anything was
/// pruned.
///
/// The `logs_fts_ad` trigger removes matching search-index rows together with
/// each deleted event, so a prune can never leave stale search hits behind.
///
/// `now_unix_nano` is injected so callers control the cutoff clock.
pub(crate) fn prune(
    conn: &mut Connection,
    policy: &RetentionPolicy,
    now_unix_nano: i64,
) -> Result<PruneReport, StorageError> {
    let mut report = PruneReport::default();

    if let Some(window) = policy.logs {
        let cutoff = retention_cutoff(now_unix_nano, window);
        let batch = PRUNE_BATCH_ROWS as i64;
        loop {
            let tx = conn.transaction()?;
            let deleted = tx.execute(
                "DELETE FROM log_event WHERE id IN (
                    SELECT id FROM log_event WHERE timestamp_ns < ?1 LIMIT ?2
                 )",
                rusqlite::params![cutoff, batch],
            )?;
            tx.commit()?;
            report.log_events += deleted;
            if deleted < PRUNE_BATCH_ROWS {
                break;
            }
        }
    }
    if let Some(window) = policy.metrics {
        let cutoff = retention_cutoff(now_unix_nano, window);
        let batch = PRUNE_BATCH_ROWS as i64;
        loop {
            let tx = conn.transaction()?;
            let deleted = tx.execute(
                "DELETE FROM metric_data_point WHERE id IN (
                    SELECT id FROM metric_data_point WHERE timestamp_ns < ?1 LIMIT ?2
                 )",
                rusqlite::params![cutoff, batch],
            )?;
            tx.commit()?;
            report.metric_points += deleted;
            if deleted < PRUNE_BATCH_ROWS {
                break;
            }
        }
    }

    if report.total() > 0 {
        let tx = conn.transaction()?;
        collect_orphan_dimensions(&tx, &mut report)?;
        tx.commit()?;
    }

    Ok(report)
}

/// `now − window`, saturated instead of panicking on extreme windows.
fn retention_cutoff(now_unix_nano: i64, window: std::time::Duration) -> i64 {
    now_unix_nano.saturating_sub(i64::try_from(window.as_nanos()).unwrap_or(i64::MAX))
}

/// Removes rows that lost their last child to a prune. Order matters:
/// children first, so every later step sees the graph after the previous
/// removal. All probes hit existing indexes (`series_id`, `metric_id`,
/// `scope_id`, `resource_id` leading columns), keeping the scans cheap even
/// on large dimension tables. Resources are shared between logs and metrics,
/// so both reference directions must be checked before dropping one.
fn collect_orphan_dimensions(
    tx: &rusqlite::Transaction<'_>,
    report: &mut PruneReport,
) -> Result<(), StorageError> {
    report.metric_series = tx.execute(
        "DELETE FROM metric_series WHERE NOT EXISTS (
            SELECT 1 FROM metric_data_point WHERE series_id = metric_series.id
         )",
        [],
    )?;
    report.metrics = tx.execute(
        "DELETE FROM metric WHERE NOT EXISTS (
            SELECT 1 FROM metric_series WHERE metric_id = metric.id
         )",
        [],
    )?;
    report.scopes = tx.execute(
        "DELETE FROM scope WHERE NOT EXISTS (
            SELECT 1 FROM metric WHERE scope_id = scope.id
         )",
        [],
    )?;
    report.resources = tx.execute(
        "DELETE FROM log_resource WHERE NOT EXISTS (
            SELECT 1 FROM log_event WHERE resource_id = log_resource.id
         )
         AND NOT EXISTS (
            SELECT 1 FROM scope WHERE resource_id = log_resource.id
         )",
        [],
    )?;
    Ok(())
}

/// Drops, recreates and repopulates the search index. Inserts and deletes
/// maintain the index incrementally via triggers; this full rebuild exists
/// for recovery from a corrupted or manually emptied index (and for schema
/// changes to the indexed columns).
///
/// The whole rebuild is one transaction: SQLite DDL is transactional, so a
/// crash mid-rebuild rolls back completely and leaves either the old or the
/// new index in place — never a database whose migration stamp claims an
/// index that does not exist.
pub(crate) fn rebuild_fts(conn: &mut Connection) -> Result<usize, StorageError> {
    let tx = conn.transaction()?;
    // The sync triggers hang off `log_event`, not the FTS table, so dropping
    // the table alone would leave them behind and block recreation.
    tx.execute_batch(
        "DROP TRIGGER IF EXISTS logs_fts_ai;
         DROP TRIGGER IF EXISTS logs_fts_ad;
         DROP TABLE IF EXISTS logs_fts;",
    )?;
    tx.execute_batch(FTS_SCHEMA)?;

    let indexed = tx.execute(
        "INSERT INTO logs_fts (rowid, body, service_name, host_name, severity_text, event_name, scope_name)
         SELECT
             id,
             COALESCE(body, ''),
             COALESCE(service_name, ''),
             COALESCE(host_name, ''),
             COALESCE(severity_text, ''),
             COALESCE(event_name, ''),
             COALESCE(scope_name, '')
         FROM logs",
        [],
    )?;
    tx.commit()?;

    tracing::info!(indexed, "rebuilt log search index");
    Ok(indexed)
}

/// What one size-quota enforcement pass evicted.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuotaReport {
    pub log_events: usize,
    pub metric_points: usize,
    /// File size before enforcement (bytes).
    pub bytes_before: u64,
    /// File size after enforcement (bytes).
    pub bytes_after: u64,
}

/// Maximum eviction rounds per enforcement call. Each round deletes one
/// bounded batch per signal plus orphan dimensions; the cap bounds how long
/// one insert batch can stall behind quota enforcement.
const MAX_QUOTA_ROUNDS: usize = 8;

/// Enforces `quota_bytes` on the database file at `db_path`: while the file
/// exceeds the quota, evicts the oldest rows first (size-based retention —
///
/// newest telemetry survives) in bounded batches, then reclaims freelist
/// pages. Stops early once under quota or when nothing remains to evict.
///
/// Crash-safe: every batch commits separately, so a crash mid-enforcement
/// resumes cleanly on the next insert.
pub(crate) fn enforce_size_quota(
    conn: &mut Connection,
    db_path: &Path,
    quota_bytes: u64,
) -> Result<QuotaReport, StorageError> {
    let mut report = QuotaReport {
        bytes_before: database_bytes(db_path),
        ..Default::default()
    };
    // Fold the WAL into the main file so sizes observed below reflect
    // committed state. A checkpoint failure (busy reader) makes reclaimed
    // bytes unobservable: stop rather than evict blindly.
    if !try_checkpoint_truncate(conn) {
        report.bytes_after = database_bytes(db_path);
        return Ok(report);
    }
    // Legacy databases were created without auto-vacuum and need a one-time
    // converting VACUUM before deletions can shrink the file; without it the
    // loop below would evict batch after batch without ever fitting.
    if auto_vacuum_mode(conn) != INCREMENTAL_AUTO_VACUUM {
        convert_to_incremental_vacuum(conn)?;
    }
    for _ in 0..MAX_QUOTA_ROUNDS {
        if file_size(db_path) <= quota_bytes {
            break;
        }
        let deleted = prune_oldest_batch(conn)?;
        report.log_events += deleted.0;
        report.metric_points += deleted.1;
        if deleted.0 + deleted.1 == 0 {
            // Nothing left to evict: the remaining bytes are schema/freelist
            // that cannot be reclaimed further.
            break;
        }
        // `DELETE` lands in the WAL and never shrinks the main file, so fold
        // it in, reclaim the freelist, and fold the reclamation too before
        // the next size check. Without this every round deletes another full
        // batch while the measured size never decreases, wiping far more
        // history than the quota requires.
        if !try_checkpoint_truncate(conn) {
            break;
        }
        incremental_vacuum(conn)?;
        if !try_checkpoint_truncate(conn) {
            break;
        }
    }
    // One orphan-dimension sweep for everything evicted above (cheaper than
    // per-batch when several rounds ran).
    if report.log_events + report.metric_points > 0 {
        let tx = conn.transaction()?;
        let mut sweep = PruneReport::default();
        collect_orphan_dimensions(&tx, &mut sweep)?;
        tx.commit()?;
    }
    report.bytes_after = database_bytes(db_path);
    Ok(report)
}

/// Best-effort `wal_checkpoint(TRUNCATE)`; `true` when the WAL is fully
/// folded into the main file.
fn try_checkpoint_truncate(conn: &Connection) -> bool {
    match checkpoint(conn, CheckpointMode::Truncate) {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                %error,
                "size quota: wal checkpoint failed; stopping eviction early"
            );
            false
        }
    }
}

/// Best-effort file size; missing/unreadable counts as zero so enforcement
/// degrades to a no-op instead of failing the insert it guards.
pub(crate) fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |metadata| metadata.len())
}

/// SQLite sidecar path (`<db>-wal`), matching SQLite's suffix convention
/// rather than `Path::with_extension`.
fn wal_path(db_path: &Path) -> std::path::PathBuf {
    let mut os = db_path.as_os_str().to_owned();
    os.push("-wal");
    std::path::PathBuf::from(os)
}

/// On-disk bytes used by the database: main file plus WAL. The quota is a
/// disk budget, so the WAL must count; it is bounded by
/// `journal_size_limit` but can still hold megabytes between checkpoints.
pub(crate) fn database_bytes(db_path: &Path) -> u64 {
    file_size(db_path).saturating_add(file_size(&wal_path(db_path)))
}

/// Deletes one bounded batch of the oldest rows per signal (oldest-first by
/// event timestamp, using the existing timestamp indexes). Returns
/// `(log_events, metric_points)` deleted by this batch.
fn prune_oldest_batch(conn: &mut Connection) -> Result<(usize, usize), StorageError> {
    let batch = PRUNE_BATCH_ROWS as i64;
    let tx = conn.transaction()?;
    let logs = tx.execute(
        "DELETE FROM log_event WHERE id IN (
            SELECT id FROM log_event ORDER BY timestamp_ns ASC LIMIT ?1
         )",
        [batch],
    )?;
    let metrics = tx.execute(
        "DELETE FROM metric_data_point WHERE id IN (
            SELECT id FROM metric_data_point ORDER BY timestamp_ns ASC LIMIT ?1
         )",
        [batch],
    )?;
    tx.commit()?;
    Ok((logs, metrics))
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_sqlite_core::storage::SyncMode;

    /// Seeds 6000 log events for quota tests.
    fn seed_logs(conn: &mut Connection, resource_id: &str, count: i64) {
        conn.execute(
            "INSERT INTO log_resource (id, service_name) VALUES (?1, 'svc')",
            [resource_id],
        )
        .expect("insert resource");
        {
            let tx = conn.transaction().expect("transaction");
            for i in 0..count {
                tx.execute(
                    "INSERT INTO log_event (
                        resource_id, timestamp_ns, observed_timestamp_ns, severity_number, body
                     ) VALUES (?1, ?2, ?2, 9, ?3)",
                    rusqlite::params![resource_id, i, format!("body-{i}")],
                )
                .expect("insert log");
            }
            tx.commit().expect("commit");
        }
    }

    fn remaining_logs(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM log_event", [], |row| row.get(0))
            .expect("count")
    }

    /// Regression guard for the quota loop: `DELETE` alone never shrinks the
    /// file, so eviction must reclaim pages between rounds and stop as soon
    /// as the file fits the budget instead of wiping every row.
    #[test]
    fn quota_eviction_reclaims_pages_and_stops_at_the_budget() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("quota.db");
        let mut conn = crate::sqlite::open(&path, SyncMode::default()).expect("open");
        crate::migration::migrate(&mut conn).expect("migrate");
        seed_logs(&mut conn, "r", 6_000);
        checkpoint(&conn, CheckpointMode::Truncate).expect("checkpoint");
        let size = file_size(&path);
        let quota = size.saturating_sub(16 * 1024);

        let report = enforce_size_quota(&mut conn, &path, quota).expect("enforce quota");

        assert!(
            report.log_events > 0,
            "over-budget database must evict rows"
        );
        assert!(
            remaining_logs(&conn) > 0,
            "eviction must stop once the file fits the quota, not wipe the table"
        );
        assert!(
            report.bytes_after < report.bytes_before,
            "quota enforcement must actually reclaim file space"
        );
    }

    /// Databases created before incremental auto-vacuum was enabled need a
    /// one-time converting VACUUM: without it `incremental_vacuum` is a no-op
    /// and the eviction loop would delete rows while never fitting.
    #[test]
    fn quota_eviction_converts_legacy_databases_before_deleting() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("legacy.db");
        let mut conn = Connection::open(&path).expect("open legacy db");
        crate::migration::migrate(&mut conn).expect("migrate");
        seed_logs(&mut conn, "r", 6_000);
        let size = file_size(&path);
        assert_eq!(
            auto_vacuum_mode(&conn),
            0,
            "legacy default has no auto-vacuum"
        );
        let quota = size.saturating_sub(16 * 1024);

        let report = enforce_size_quota(&mut conn, &path, quota).expect("enforce quota");

        assert_eq!(
            auto_vacuum_mode(&conn),
            INCREMENTAL_AUTO_VACUUM,
            "quota enforcement must convert the database once"
        );
        assert!(
            report.log_events > 0,
            "over-budget database must evict rows"
        );
        assert!(
            remaining_logs(&conn) > 0,
            "eviction must stop once the file fits the quota, not wipe the table"
        );
    }
}

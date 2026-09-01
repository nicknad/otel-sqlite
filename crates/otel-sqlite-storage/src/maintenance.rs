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
    conn.execute_batch(&format!("PRAGMA wal_checkpoint({});", mode.as_str()))?;
    Ok(())
}

pub(crate) fn analyze(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch("ANALYZE;")?;
    Ok(())
}

pub(crate) fn vacuum(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch("VACUUM;")?;
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
/// deletions orphaned — all in one transaction.
///
/// The `logs_fts_ad` trigger removes matching search-index rows together with
/// each deleted event, so a prune can never leave stale search hits behind.
/// Dimension cleanup (series → metric → scope → resource) only runs when
/// something was actually pruned; because the whole pass is transactional,
/// no partially-pruned state that could strand dimensions can ever commit.
///
/// `now_unix_nano` is injected so callers control the cutoff clock.
pub(crate) fn prune(
    conn: &mut Connection,
    policy: &RetentionPolicy,
    now_unix_nano: i64,
) -> Result<PruneReport, StorageError> {
    let mut report = PruneReport::default();
    let tx = conn.transaction()?;

    if let Some(window) = policy.logs {
        report.log_events = tx.execute(
            "DELETE FROM log_event WHERE timestamp_ns < ?1",
            [retention_cutoff(now_unix_nano, window)],
        )?;
    }
    if let Some(window) = policy.metrics {
        report.metric_points = tx.execute(
            "DELETE FROM metric_data_point WHERE timestamp_ns < ?1",
            [retention_cutoff(now_unix_nano, window)],
        )?;
    }

    if report.total() > 0 {
        collect_orphan_dimensions(&tx, &mut report)?;
    }

    tx.commit()?;
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

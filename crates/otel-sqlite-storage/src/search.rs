//! Log search index as a reconciled derived index.
//!
//! The index is optional (`FtsMode::Off`), indexes the body only by default
//! (`FtsMode::Body`), or keeps the legacy six-column shape (`FtsMode::Full`).
//! Its shape is derived data: [`SearchIndex::reconcile`] installs the configured
//! mode at startup, and a bounded, resumable rebuild backfills rows that
//! predate the current triggers.
//!
//! Nothing here runs during normal inserts except the SQLite triggers; the
//! writer only performs a rebuild batch when the command queue is idle or at a
//! slow cadence under sustained load. A rebuild batch is one transaction that
//! advances `search_index_state.last_rowid`, so a crash resumes at the last
//! committed batch instead of restarting the scan.

use rusqlite::{Connection, OptionalExtension, params};

use crate::error::StorageError;

const STATE_TABLE: &str = "
CREATE TABLE IF NOT EXISTS search_index_state (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    mode          TEXT    NOT NULL CHECK (mode IN ('off', 'body', 'full')),
    status        TEXT    NOT NULL CHECK (status IN ('ready', 'rebuilding')),
    last_rowid    INTEGER NOT NULL DEFAULT 0,
    rebuild_until INTEGER,
    updated_at    TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (mode = 'off'
            AND status = 'ready'
            AND last_rowid = 0
            AND rebuild_until IS NULL)
        OR
        (mode <> 'off'
            AND (
                (status = 'ready' AND rebuild_until IS NULL)
                OR
                (status = 'rebuilding'
                    AND rebuild_until IS NOT NULL
                    AND rebuild_until >= last_rowid)
            ))
    )
);";

const BODY_TABLE: &str = "
CREATE VIRTUAL TABLE logs_fts USING fts5(
    body,
    tokenize = 'unicode61 remove_diacritics 2',
    content = '',
    contentless_delete = 1
);";

const BODY_TRIGGERS: &str = "
CREATE TRIGGER logs_fts_ai AFTER INSERT ON log_event BEGIN
    INSERT INTO logs_fts (rowid, body)
    VALUES (new.id, COALESCE(new.body, ''));
END;

CREATE TRIGGER logs_fts_ad AFTER DELETE ON log_event BEGIN
    DELETE FROM logs_fts WHERE rowid = old.id;
END;";

const FULL_TABLE: &str = "
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
);";

const FULL_TRIGGERS: &str = "
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

const DROP_INDEX: &str = "
DROP TRIGGER IF EXISTS logs_fts_ai;
DROP TRIGGER IF EXISTS logs_fts_ad;
DROP TABLE IF EXISTS logs_fts;";

/// Operator-facing search index mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FtsMode {
    /// The table exists but is empty and has no triggers. Search returns no
    /// rows; nothing is written for it during ingestion.
    Off,
    /// Index `log_event.body` only. This is the default: it is the only
    /// free-text field and the insert trigger needs no resource join.
    #[default]
    Body,
    /// Legacy six-column index (`body`, service, host, severity, event and
    /// scope), kept for operators who rely on column-scoped `MATCH`.
    Full,
}

impl FtsMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Body => "body",
            Self::Full => "full",
        }
    }

    /// Parses the operator vocabulary at the config boundary.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" => Some(Self::Off),
            "body" => Some(Self::Body),
            "full" => Some(Self::Full),
            _ => None,
        }
    }

    pub const fn enabled(self) -> bool {
        !matches!(self, Self::Off)
    }

    /// Columns in DDL order. `Off` keeps the body table shape so a query that
    /// names `body` still parses; it simply has no triggers and no rows.
    pub const fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Off | Self::Body => &["body"],
            Self::Full => &[
                "body",
                "service_name",
                "host_name",
                "severity_text",
                "event_name",
                "scope_name",
            ],
        }
    }
}

/// Read-only view of the installed search index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct SearchIndexStatus {
    pub mode: FtsMode,
    pub status: SearchStatus,
    /// Highest `log_event.id` covered by the index rebuild.
    pub indexed_through: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchStatus {
    Ready,
    Rebuilding,
}

impl SearchStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Rebuilding => "rebuilding",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "ready" => Some(Self::Ready),
            "rebuilding" => Some(Self::Rebuilding),
            _ => None,
        }
    }
}

/// State recovered from `search_index_state`, plus the in-memory cursor.
pub(crate) struct SearchIndex {
    mode: FtsMode,
    cursor: Option<RebuildCursor>,
}

#[derive(Debug, Clone, Copy)]
struct RebuildCursor {
    last_rowid: i64,
    until: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RebuildStep {
    Progress { rows: usize },
    Complete,
}

#[derive(Debug, Clone, Copy)]
struct InstalledIndex {
    mode: FtsMode,
    status: SearchStatus,
    last_rowid: i64,
    rebuild_until: Option<i64>,
}

impl InstalledIndex {
    fn cursor(self) -> Option<RebuildCursor> {
        match (self.status, self.rebuild_until) {
            (SearchStatus::Rebuilding, Some(until)) => Some(RebuildCursor {
                last_rowid: self.last_rowid,
                until,
            }),
            _ => None,
        }
    }

    fn summary(self) -> SearchIndexStatus {
        SearchIndexStatus {
            mode: self.mode,
            status: self.status,
            indexed_through: self.last_rowid,
        }
    }
}

fn table_ddl(mode: FtsMode) -> &'static str {
    match mode {
        FtsMode::Off | FtsMode::Body => BODY_TABLE,
        FtsMode::Full => FULL_TABLE,
    }
}

fn trigger_ddl(mode: FtsMode) -> &'static str {
    match mode {
        FtsMode::Off => "",
        FtsMode::Body => BODY_TRIGGERS,
        FtsMode::Full => FULL_TRIGGERS,
    }
}

/// Reads the installed state without mutating the database.
pub fn inspect_search_index(conn: &Connection) -> Result<Option<SearchIndexStatus>, StorageError> {
    Ok(read_state(conn)?.map(InstalledIndex::summary))
}

fn read_state(conn: &Connection) -> Result<Option<InstalledIndex>, StorageError> {
    if !crate::migration::table_exists(conn, "search_index_state")? {
        return Ok(None);
    }
    let row: Option<(String, String, i64, Option<i64>)> = conn
        .query_row(
            "SELECT mode, status, last_rowid, rebuild_until
               FROM search_index_state
              WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((mode, status, last_rowid, rebuild_until)) = row else {
        return Ok(None);
    };
    let mode = FtsMode::parse(&mode).ok_or_else(|| StorageError::StartupFailed {
        message: format!("search_index_state has unknown mode `{mode}`"),
    })?;
    let status = SearchStatus::parse(&status).ok_or_else(|| StorageError::StartupFailed {
        message: format!("search_index_state has unknown status `{status}`"),
    })?;
    Ok(Some(InstalledIndex {
        mode,
        status,
        last_rowid,
        rebuild_until,
    }))
}

/// Whether the physical index objects match `mode`.
fn objects_match(conn: &Connection, mode: FtsMode) -> Result<bool, StorageError> {
    if !crate::migration::table_exists(conn, "logs_fts")? {
        return Ok(false);
    }
    let mut statement = conn.prepare("PRAGMA table_info(logs_fts)")?;
    let columns: Vec<String> = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<_, _>>()?;
    if !columns
        .iter()
        .map(String::as_str)
        .eq(mode.columns().iter().copied())
    {
        return Ok(false);
    }
    let triggers: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master
          WHERE type = 'trigger' AND name LIKE 'logs_fts%'",
        [],
        |row| row.get(0),
    )?;
    let expected = if mode.enabled() { 2 } else { 0 };
    Ok(triggers == expected)
}

/// Installs `mode` in one transaction. DDL only; never scans `log_event`
/// content except `MAX(id)` on the integer primary key.
fn install(conn: &mut Connection, mode: FtsMode) -> Result<InstalledIndex, StorageError> {
    let tx = conn.transaction()?;
    tx.execute_batch(DROP_INDEX)?;
    tx.execute_batch(table_ddl(mode))?;
    if mode.enabled() {
        tx.execute_batch(trigger_ddl(mode))?;
    }
    tx.execute_batch(STATE_TABLE)?;

    let highest: i64 = tx.query_row("SELECT COALESCE(MAX(id), 0) FROM log_event", [], |row| {
        row.get(0)
    })?;
    let (status, rebuild_until) = if mode.enabled() && highest > 0 {
        (SearchStatus::Rebuilding, Some(highest))
    } else {
        (SearchStatus::Ready, None)
    };
    tx.execute(
        "INSERT OR REPLACE INTO search_index_state
             (id, mode, status, last_rowid, rebuild_until, updated_at)
         VALUES (1, ?1, ?2, 0, ?3, CURRENT_TIMESTAMP)",
        params![mode.as_str(), status.as_str(), rebuild_until],
    )?;
    tx.commit()?;
    Ok(InstalledIndex {
        mode,
        status,
        last_rowid: 0,
        rebuild_until,
    })
}

impl SearchIndex {
    /// Reconciles the installed index with `mode` before storage reports ready.
    /// A matching shape is a no-op; anything else is dropped and recreated
    /// empty, with existing rows marked for the background rebuild.
    pub(crate) fn reconcile(conn: &mut Connection, mode: FtsMode) -> Result<Self, StorageError> {
        if let Some(state) = read_state(conn)?
            && state.mode == mode
            && objects_match(conn, mode)?
        {
            if state.status == SearchStatus::Rebuilding {
                warn_rebuilding(mode);
            }
            set_rebuilding_gauge(state.cursor().is_some());
            return Ok(Self {
                mode,
                cursor: state.cursor(),
            });
        }

        let installed = install(conn, mode)?;
        if installed.status == SearchStatus::Rebuilding {
            warn_rebuilding(mode);
        } else if mode == FtsMode::Off {
            tracing::info!(
                "log search index disabled; the index is empty and triggers are removed"
            );
        }
        set_rebuilding_gauge(installed.cursor().is_some());
        Ok(Self {
            mode,
            cursor: installed.cursor(),
        })
    }

    pub(crate) const fn mode(&self) -> FtsMode {
        self.mode
    }

    pub(crate) const fn is_rebuilding(&self) -> bool {
        self.cursor.is_some()
    }

    /// Drops and recreates the index for the configured mode and restarts the
    /// background rebuild. A disabled index is never re-enabled by a scheduled
    /// rebuild.
    pub(crate) fn restart(&mut self, conn: &mut Connection) -> Result<bool, StorageError> {
        if !self.mode.enabled() {
            tracing::warn!("rebuild_fts ignored: log search index is disabled");
            return Ok(false);
        }
        let installed = install(conn, self.mode)?;
        self.cursor = installed.cursor();
        set_rebuilding_gauge(self.cursor.is_some());
        Ok(true)
    }

    /// Advances the rebuild by one bounded transaction.
    pub(crate) fn step(
        &mut self,
        conn: &mut Connection,
        max_rows: usize,
    ) -> Result<RebuildStep, StorageError> {
        let Some(cursor) = self.cursor else {
            return Ok(RebuildStep::Complete);
        };
        let max_rows = i64::try_from(max_rows.max(1)).unwrap_or(i64::MAX);
        let tx = conn.transaction()?;
        let next: Option<i64> = tx.query_row(
            "SELECT MAX(id) FROM (
                 SELECT id FROM log_event
                  WHERE id > ?1 AND id <= ?2
                  ORDER BY id
                  LIMIT ?3
             )",
            params![cursor.last_rowid, cursor.until, max_rows],
            |row| row.get(0),
        )?;

        let Some(next) = next else {
            tx.execute(
                "UPDATE search_index_state
                    SET status = 'ready',
                        last_rowid = rebuild_until,
                        rebuild_until = NULL,
                        updated_at = CURRENT_TIMESTAMP
                  WHERE id = 1",
                [],
            )?;
            tx.commit()?;
            self.cursor = None;
            set_rebuilding_gauge(false);
            return Ok(RebuildStep::Complete);
        };

        // Delete first so replaying a batch converges and entries for rows
        // deleted before the scan reached them cannot survive.
        tx.execute(
            "DELETE FROM logs_fts WHERE rowid > ?1 AND rowid <= ?2",
            params![cursor.last_rowid, next],
        )?;
        let inserted = match self.mode {
            FtsMode::Off => 0,
            FtsMode::Body => tx.execute(
                "INSERT INTO logs_fts (rowid, body)
                 SELECT id, COALESCE(body, '')
                   FROM log_event
                  WHERE id > ?1 AND id <= ?2",
                params![cursor.last_rowid, next],
            )?,
            FtsMode::Full => tx.execute(
                "INSERT INTO logs_fts
                     (rowid, body, service_name, host_name, severity_text, event_name, scope_name)
                 SELECT le.id,
                        COALESCE(le.body, ''),
                        COALESCE(lr.service_name, ''),
                        COALESCE(lr.host_name, ''),
                        COALESCE(le.severity_text, ''),
                        COALESCE(le.event_name, ''),
                        COALESCE(le.scope_name, '')
                   FROM log_event AS le
                   JOIN log_resource AS lr ON lr.id = le.resource_id
                  WHERE le.id > ?1 AND le.id <= ?2",
                params![cursor.last_rowid, next],
            )?,
        };
        tx.execute(
            "UPDATE search_index_state
                SET last_rowid = ?1, updated_at = CURRENT_TIMESTAMP
              WHERE id = 1",
            params![next],
        )?;
        tx.commit()?;
        self.cursor = Some(RebuildCursor {
            last_rowid: next,
            until: cursor.until,
        });
        Ok(RebuildStep::Progress { rows: inserted })
    }
}

fn warn_rebuilding(mode: FtsMode) {
    tracing::warn!(
        mode = mode.as_str(),
        "log search index is rebuilding; results are partial until it completes"
    );
}

fn set_rebuilding_gauge(rebuilding: bool) {
    ::metrics::gauge!("storage_search_index_rebuilding").set(if rebuilding { 1.0 } else { 0.0 });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_vocabulary_round_trips() {
        for mode in [FtsMode::Off, FtsMode::Body, FtsMode::Full] {
            assert_eq!(FtsMode::parse(mode.as_str()), Some(mode));
            assert_eq!(
                FtsMode::parse(&mode.as_str().to_ascii_uppercase()),
                Some(mode)
            );
        }
        assert_eq!(FtsMode::parse("enabled"), None);
        assert_eq!(FtsMode::default(), FtsMode::Body);
        assert!(FtsMode::Body.enabled());
        assert!(!FtsMode::Off.enabled());
    }

    #[test]
    fn mode_columns_match_the_ddl() {
        for mode in [FtsMode::Off, FtsMode::Body, FtsMode::Full] {
            let ddl = format!("{}{}", table_ddl(mode), trigger_ddl(mode));
            for column in mode.columns() {
                assert!(ddl.contains(column), "{mode:?} DDL must contain `{column}`");
            }
        }
        assert_eq!(trigger_ddl(FtsMode::Off), "");
    }
}

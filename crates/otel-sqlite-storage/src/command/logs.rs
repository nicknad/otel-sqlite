//! Log-event persistence: one insert per record, one transaction per batch.

use std::collections::BTreeMap;

use otel_sqlite_core::model::{LogRecord, Resource, write_attributes_json_into};
use otel_sqlite_core::storage::LogWriteBatch;
use rusqlite::{Connection, Statement, params};

use super::InsertScratch;
use super::identity::resolve_resource;
use super::non_empty;
use crate::error::StorageError;

/// Result of a tolerant insert pass, including which records were dropped for
/// each durability ticket.
#[derive(Debug, Default)]
pub(crate) struct SalvageOutcome {
    pub(crate) kept: u64,
    pub(crate) dropped: u64,
    /// `ticket -> (kept, dropped)` for every ticket touched by the batch.
    pub(crate) by_ticket: BTreeMap<u64, (u64, u64)>,
}

const SQL_INSERT_LOG_EVENT: &str = "
INSERT INTO log_event (
    resource_id,
    timestamp_ns,
    observed_timestamp_ns,
    severity_number,
    severity_text,
    trace_id,
    span_id,
    body,
    event_name,
    flags,
    dropped_attributes_count,
    scope_name,
    scope_version,
    scope_attributes_json,
    scope_schema_url,
    attributes_json
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)";

/// Persists one storage-sized log write batch. Called from the writer inside
/// an open transaction that may also carry other batches.
pub(crate) fn insert_logs(
    conn: &Connection,
    batch: &LogWriteBatch,
    scratch: &mut InsertScratch,
) -> Result<u64, StorageError> {
    // Empty batches have no rows, resource, or statement work to do.
    if batch.records.is_empty() {
        return Ok(0);
    }
    debug_assert_segments(batch);
    resolve_batch_resource(conn, batch, scratch)?;

    let mut statement = conn.prepare_cached(SQL_INSERT_LOG_EVENT)?;
    let mut written = 0u64;
    for record in batch.records.records() {
        execute_record(&mut statement, scratch, record)?;
        written += 1;
    }
    Ok(written)
}

/// Salvage variant of [`insert_logs`]: rows that fail with poison-class
/// errors are skipped (and counted) while healthy rows are still inserted.
///
/// The per-ticket counts tell the writer which tickets must fail their
/// durable acknowledgement; only fatal-class errors abort the pass. Ticket
/// accounting is built only here, never on the strict hot path.
pub(crate) fn insert_logs_tolerant(
    conn: &Connection,
    batch: &LogWriteBatch,
    scratch: &mut InsertScratch,
) -> Result<SalvageOutcome, StorageError> {
    if batch.records.is_empty() {
        return Ok(SalvageOutcome::default());
    }
    debug_assert_segments(batch);
    resolve_batch_resource(conn, batch, scratch)?;

    let mut statement = conn.prepare_cached(SQL_INSERT_LOG_EVENT)?;
    let mut written = 0u64;
    let mut dropped = 0u64;
    let mut by_ticket: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
    let tickets = batch
        .segments
        .iter()
        .flat_map(|segment| std::iter::repeat_n(segment.commit_seq, segment.records))
        .chain(std::iter::repeat_n(0, batch.records.len()));
    for (record, ticket) in batch.records.records().iter().zip(tickets) {
        match execute_record(&mut statement, scratch, record) {
            Ok(_) => {
                written += 1;
                if ticket != 0 {
                    by_ticket.entry(ticket).or_default().0 += 1;
                }
            }
            Err(error)
                if crate::error::classify_sqlite(&error) == crate::error::FailureClass::Poison =>
            {
                dropped += 1;
                if ticket != 0 {
                    by_ticket.entry(ticket).or_default().1 += 1;
                }
                tracing::warn!(
                    body_preview = %super::sanitized_preview(&record.body),
                    timestamp_ns = record.time_unix_nano,
                    %error,
                    "poisonous log record quarantined (dropped); healthy rows keep committing"
                );
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(SalvageOutcome {
        kept: written,
        dropped,
        by_ticket,
    })
}

fn debug_assert_segments(batch: &LogWriteBatch) {
    debug_assert!(
        batch.segments.is_empty()
            || batch
                .segments
                .iter()
                .map(|segment| segment.records)
                .sum::<usize>()
                == batch.records.len(),
        "record segments must partition the batch"
    );
}

fn resolve_batch_resource(
    conn: &Connection,
    batch: &LogWriteBatch,
    scratch: &mut InsertScratch,
) -> Result<(), StorageError> {
    // The present origin is the common case; only synthesize (and clone the
    // schema URL into) a default resource when the batch has none.
    if let Some(resource) = &batch.origin.resource {
        resolve_resource(conn, resource, scratch)
    } else {
        let resource = Resource {
            schema_url: batch.origin.schema_url.clone(),
            ..Resource::default()
        };
        resolve_resource(conn, &resource, scratch)
    }
}

fn execute_record(
    statement: &mut Statement<'_>,
    scratch: &mut InsertScratch,
    record: &LogRecord,
) -> rusqlite::Result<usize> {
    write_attributes_json_into(&record.attributes, &mut scratch.order, &mut scratch.json);
    // Plain text is the common case; only fall back to a structured body.
    let body = non_empty(record.body.as_str()).or(record.body_json.as_deref());

    statement.execute(params![
        scratch.resource_id.as_str(),
        record.time_unix_nano,
        record.observed_time_unix_nano,
        i64::from(record.severity_number as u8),
        non_empty(record.severity_text.as_str()),
        // Borrowed slices bind the fixed-size ids without per-row copies.
        record.has_trace().then_some(record.trace_id.as_slice()),
        record.has_span().then_some(record.span_id.as_slice()),
        body,
        non_empty(record.event_name.as_str()),
        record.flags,
        record.dropped_attributes_count,
        non_empty(record.scope.name()),
        non_empty(record.scope.version()),
        record.scope.attributes_json(),
        non_empty(record.scope.schema_url()),
        &*scratch.json,
    ])
}

//! Log-event persistence: one insert per record, one transaction per batch.

use otel_sqlite_core::model::Resource;
use otel_sqlite_core::storage::LogWriteBatch;
use rusqlite::{Connection, params};

use super::InsertScratch;
use super::identity::resolve_resource;
use super::json::{write_attributes_json, write_attributes_json_into};
use super::non_empty;
use crate::error::StorageError;

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
/// an open transaction; every call belongs to exactly one transaction.
pub(crate) fn insert_logs(conn: &Connection, batch: &LogWriteBatch) -> Result<u64, StorageError> {
    insert_logs_inner(conn, batch, false).map(|(written, _dropped)| written)
}

/// Salvage variant of [`insert_logs`]: rows that fail with poison-class
/// errors are skipped (and counted) while healthy rows are still inserted.
///
/// Returns `(written, dropped)`; only fatal-class errors abort the pass.
pub(crate) fn insert_logs_tolerant(
    conn: &Connection,
    batch: &LogWriteBatch,
) -> Result<(u64, u64), StorageError> {
    insert_logs_inner(conn, batch, true)
}

fn insert_logs_inner(
    conn: &Connection,
    batch: &LogWriteBatch,
    tolerant: bool,
) -> Result<(u64, u64), StorageError> {
    // Row-level serialization reuses these buffers; the second scratch only
    // serves the rare records carrying their own resource so the batch-level
    // id in `scratch` survives them.
    let mut scratch = InsertScratch::new();
    let mut record_scratch = InsertScratch::new();
    let default_resource;
    let batch_resource = if let Some(resource) = &batch.origin.resource {
        resource
    } else {
        default_resource = Resource::default();
        &default_resource
    };
    resolve_resource(conn, batch_resource, &mut scratch)?;

    let mut statement = conn.prepare_cached(SQL_INSERT_LOG_EVENT)?;
    let mut written = 0u64;
    let mut dropped = 0u64;
    for record in batch.records.records() {
        write_attributes_json(&record.attributes, &mut scratch);
        write_attributes_json_into(
            &record.scope_attributes,
            &mut scratch.order,
            &mut scratch.scope_json,
        );
        let resource_id = match &record.resource {
            Some(resource) => {
                resolve_resource(conn, resource, &mut record_scratch)?;
                record_scratch.resource_id.as_str()
            }
            None => scratch.resource_id.as_str(),
        };
        let body = record
            .body_json
            .as_deref()
            .or(non_empty(record.body.as_str()));

        match statement.execute(params![
            resource_id,
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
            non_empty(record.scope_name.as_str()),
            non_empty(record.scope_version.as_str()),
            &*scratch.scope_json,
            non_empty(record.scope_schema_url.as_str()),
            &*scratch.json,
        ]) {
            Ok(_) => written += 1,
            Err(error)
                if tolerant
                    && crate::error::classify_sqlite(&error)
                        == crate::error::FailureClass::Poison =>
            {
                dropped += 1;
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
    Ok((written, dropped))
}

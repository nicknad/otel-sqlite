//! Stable identity derivation and dimension-table upserts.
//!
//! Every entity ID (resource, scope, metric, series) is a SHA-256 fingerprint
//! over its identifying attributes, so re-ingesting the same dimensions is a
//! no-op (`ON CONFLICT (id) DO NOTHING`) and joins need no lookup tables.
//! [`fingerprint_into`] hashes length-prefixed parts to keep boundaries
//! unambiguous and writes the hex digest into a reused buffer.

use std::fmt::Write as _;

use otel_sqlite_core::model::{Attribute, Resource, Temporality};
use rusqlite::{Connection, Statement, params};
use sha2::{Digest, Sha256};

use super::InsertScratch;
use super::json::write_attributes_json;
use super::non_empty;
use crate::error::StorageError;

const SQL_INSERT_RESOURCE: &str = "
INSERT INTO log_resource (id, service_name, host_name, schema_url, attributes_json)
VALUES (?1, ?2, ?3, ?4, ?5)
ON CONFLICT (id) DO NOTHING";

const SQL_INSERT_SCOPE: &str = "
INSERT INTO scope (id, resource_id, name, version, schema_url, attributes_json)
VALUES (?1, ?2, ?3, ?4, ?5, ?6)
ON CONFLICT (id) DO NOTHING";

const SQL_INSERT_METRIC: &str = "
INSERT INTO metric (id, scope_id, name, description, unit, type, is_monotonic, aggregation_temporality, metadata_json)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
ON CONFLICT (id) DO NOTHING";

/// Hashes length-prefixed parts and writes the hex digest into `out`,
/// clearing it first. The digest matches the former `format!("{:x}")`
/// rendering; the reused buffer avoids a fresh allocation per call.
pub(super) fn fingerprint_into(parts: &[&str], out: &mut String) {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    out.clear();
    // Writing hex into a String never fails.
    let _ = write!(out, "{:x}", hasher.finalize());
}

/// Resolves (and upserts) the resource dimension row. The id lands in
/// `scratch.resource_id`, the canonical attributes JSON in `scratch.json`;
/// both buffers are reused across calls.
pub(super) fn resolve_resource(
    conn: &Connection,
    resource: &Resource,
    scratch: &mut InsertScratch,
) -> Result<(), StorageError> {
    let service_name = resource.service_name();
    let host_name = non_empty(resource.host_name());
    let schema_url = non_empty(resource.schema_url.as_str());

    write_attributes_json(&resource.attributes, scratch);
    fingerprint_into(
        &[
            service_name,
            host_name.unwrap_or(""),
            schema_url.unwrap_or(""),
            &scratch.json,
        ],
        &mut scratch.resource_id,
    );

    conn.prepare_cached(SQL_INSERT_RESOURCE)?.execute(params![
        &*scratch.resource_id,
        service_name,
        host_name,
        schema_url,
        &*scratch.json,
    ])?;

    Ok(())
}

pub(super) fn resolve_scope(
    conn: &Connection,
    name: &str,
    version: &str,
    schema_url: &str,
    attributes: &[Attribute],
    scratch: &mut InsertScratch,
) -> Result<(), StorageError> {
    // Scope identity includes the schema URL (two scopes that differ only in
    // schema are distinct) but not the descriptive scope attributes, which
    // are persisted first-wins like metric description. Writing the scope
    // attributes into `scratch.json` is safe: resolve_scope runs before any
    // point attributes overwrite the buffer for the same record.
    write_attributes_json(attributes, scratch);
    fingerprint_into(
        &[scratch.resource_id.as_str(), name, version, schema_url],
        &mut scratch.scope_id,
    );
    conn.prepare_cached(SQL_INSERT_SCOPE)?.execute(params![
        &*scratch.scope_id,
        &*scratch.resource_id,
        non_empty(name),
        non_empty(version),
        non_empty(schema_url),
        &*scratch.json,
    ])?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn resolve_metric(
    conn: &Connection,
    name: &str,
    description: &str,
    unit: &str,
    metadata: &[Attribute],
    metric_type: i64,
    is_monotonic: bool,
    temporality: Temporality,
    scratch: &mut InsertScratch,
) -> Result<(), StorageError> {
    fingerprint_into(
        &[
            scratch.scope_id.as_str(),
            name,
            unit,
            &metric_type.to_string(),
        ],
        &mut scratch.metric_id,
    );
    write_attributes_json(metadata, scratch);
    conn.prepare_cached(SQL_INSERT_METRIC)?.execute(params![
        &*scratch.metric_id,
        &*scratch.scope_id,
        name,
        non_empty(description),
        non_empty(unit),
        metric_type,
        i64::from(is_monotonic),
        i64::from(temporality as u8),
        &*scratch.json,
    ])?;
    Ok(())
}

pub(super) fn resolve_series(
    statement: &mut Statement<'_>,
    metric_id: &str,
    attributes_json: &str,
    id_out: &mut String,
) -> Result<(), StorageError> {
    fingerprint_into(&[metric_id, attributes_json], id_out);
    statement.execute(params![&*id_out, metric_id, attributes_json])?;
    Ok(())
}

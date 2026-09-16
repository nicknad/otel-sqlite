//! Stable identity derivation and dimension-table upserts.
//!
//! The resource ID is a SHA-256 fingerprint over its identifying attributes,
//! so re-ingesting the same dimensions is a no-op
//! (`ON CONFLICT (id) DO NOTHING`) and joins need no lookup tables.
//! [`fingerprint_into`] hashes length-prefixed parts to keep boundaries
//! unambiguous and writes the hex digest into a reused buffer.

use std::fmt::Write as _;

use otel_sqlite_core::model::{Resource, write_attributes_json_into};
use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};

use super::InsertScratch;
use super::non_empty;
use crate::error::StorageError;

const SQL_INSERT_RESOURCE: &str = "
INSERT INTO log_resource (id, service_name, host_name, schema_url, attributes_json)
VALUES (?1, ?2, ?3, ?4, ?5)
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

    write_attributes_json_into(&resource.attributes, &mut scratch.order, &mut scratch.json);
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

//! SQL persistence for storage-sized write batches.
//!
//! Every function here is called from the writer thread inside an open
//! transaction; each call belongs to exactly one transaction. The module is
//! split by concern:
//!
//! * `logs` - log-event inserts,
//! * `metrics` - metric/series/data-point inserts across all metric kinds,
//! * `identity` - stable SHA-256 fingerprint IDs and the resource/scope/
//!   metric/series upserts keyed by them,
//! * `json` - attribute and metric-payload serialization into the JSON
//!   columns.
//!
//! Statement reuse relies on rusqlite's `prepare_cached`; the SQL strings are
//! module-private constants next to their only callers.

mod identity;
mod json;
mod logs;
mod metrics;

pub(crate) use logs::{insert_logs, insert_logs_tolerant};
pub(crate) use metrics::{insert_metrics, insert_metrics_tolerant};

/// Empty strings are stored as NULL so sparse columns compress better and
/// queries can distinguish "absent" from "empty".
pub(crate) fn non_empty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

/// Reusable serialization scratch for one persistence call.
///
/// The insert hot paths serialize an attribute set and a SHA-256 fingerprint
/// per record/data point. These buffers are created once per batch and
/// cleared/reused per row instead of allocating fresh strings every time.
/// Fields are exposed to the sibling `command` modules so callers can work
/// with disjoint borrows (e.g. hash `[metric_id, json]` into `series_id`
/// while reading both).
pub(crate) struct InsertScratch {
    /// Batch-level resource id (`resolve_resource`).
    pub(crate) resource_id: String,
    /// Current metric record's scope id (`resolve_scope`).
    pub(crate) scope_id: String,
    /// Current metric record's metric id (`resolve_metric`).
    pub(crate) metric_id: String,
    /// Current data point's series id (`resolve_series`).
    pub(crate) series_id: String,
    /// Output buffer for the current row's JSON documents.
    pub(crate) json: String,
    /// Output buffer for the current row's scope attributes JSON. Kept
    /// separate so scope dimensions and point attributes never overwrite
    /// each other within one record.
    pub(crate) scope_json: String,
    /// Output buffer for the current row's exemplars JSON document.
    pub(crate) exemplars_json: String,
    /// Key-ordering indices used by `write_attributes_json`.
    pub(crate) order: Vec<u32>,
}

impl InsertScratch {
    pub(crate) fn new() -> Self {
        // Fingerprints are 64 hex chars; JSON sizes vary and grow on demand.
        Self {
            resource_id: String::with_capacity(64),
            scope_id: String::with_capacity(64),
            metric_id: String::with_capacity(64),
            series_id: String::with_capacity(64),
            json: String::new(),
            scope_json: String::new(),
            exemplars_json: String::new(),
            order: Vec::new(),
        }
    }
}

impl Default for InsertScratch {
    fn default() -> Self {
        Self::new()
    }
}

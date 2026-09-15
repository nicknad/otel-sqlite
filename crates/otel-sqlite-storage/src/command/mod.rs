//! SQL persistence for storage-sized write batches.
//!
//! Every function here is called from the writer thread inside an open
//! transaction; each call belongs to exactly one transaction. The module is
//! split by concern:
//!
//! * `logs` - log-event inserts,
//! * `identity` - stable SHA-256 fingerprint IDs and the resource upsert
//!   keyed by them,
//! * `json` - attribute serialization into the JSON columns.
//!
//! Statement reuse relies on rusqlite's `prepare_cached`; the SQL strings are
//! module-private constants next to their only callers.

mod identity;
mod json;
mod logs;

pub(crate) use logs::{insert_logs, insert_logs_tolerant};

/// Empty strings are stored as NULL so sparse columns compress better and
/// queries can distinguish "absent" from "empty".
pub(crate) fn non_empty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

/// Maximum characters of attacker-controlled text ever rendered into a log
/// line (L6). Quarantine warnings must identify the offending row without
/// becoming a log-flooding primitive for 1 MiB bodies.
pub(crate) const LOG_PREVIEW_CHARS: usize = 200;

/// Renders `value` safe for a single log line: truncated to
/// [`LOG_PREVIEW_CHARS`] characters with control characters (newlines,
/// carriage returns, ANSI escapes, other `is_control` codepoints) replaced
/// by U+FFFD, so one record can neither forge log lines nor inject terminal
/// escape sequences into operators' viewers.
pub(crate) fn sanitized_preview(value: &str) -> String {
    value
        .chars()
        .take(LOG_PREVIEW_CHARS)
        .map(|c| if c.is_control() { '\u{FFFD}' } else { c })
        .collect()
}

/// Reusable serialization scratch for one persistence call.
///
/// The insert hot path serializes an attribute set and a SHA-256 fingerprint
/// per record. These buffers are created once per batch and cleared/reused
/// per row instead of allocating fresh strings every time. Fields are exposed
/// to the sibling `command` modules so callers can work with disjoint
/// borrows.
pub(crate) struct InsertScratch {
    /// Batch-level resource id (`resolve_resource`).
    pub(crate) resource_id: String,
    /// Output buffer for the current row's JSON documents.
    pub(crate) json: String,
    /// Output buffer for the current row's scope attributes JSON. Kept
    /// separate so scope dimensions and record attributes never overwrite
    /// each other within one record.
    pub(crate) scope_json: String,
    /// Key-ordering indices used by `write_attributes_json`.
    pub(crate) order: Vec<u32>,
}

impl InsertScratch {
    pub(crate) fn new() -> Self {
        // Fingerprints are 64 hex chars; JSON sizes vary and grow on demand.
        Self {
            resource_id: String::with_capacity(64),
            json: String::new(),
            scope_json: String::new(),
            order: Vec::new(),
        }
    }
}

impl Default for InsertScratch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_passes_plain_text_through() {
        assert_eq!(sanitized_preview("hello"), "hello");
        assert_eq!(sanitized_preview(""), "");
    }

    #[test]
    fn preview_neutralizes_log_forgery_and_escapes() {
        let preview = sanitized_preview("line1\nline2\r\n\x1b[31mred\x07");
        assert!(
            !preview.chars().any(char::is_control),
            "no control character may survive: {preview:?}"
        );
        assert!(!preview.contains('\n'));
        assert!(!preview.contains('\x1b'));
        assert!(preview.contains("line1"));
        assert!(preview.contains("line2"));
    }

    #[test]
    fn preview_truncates_long_bodies() {
        let body = "a".repeat(LOG_PREVIEW_CHARS + 50);
        assert_eq!(sanitized_preview(&body).chars().count(), LOG_PREVIEW_CHARS);
    }
}

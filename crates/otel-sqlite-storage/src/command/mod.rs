//! SQL persistence for storage-sized write batches.
//!
//! Every function here is called from the writer thread inside an open
//! transaction; the transaction may include several consecutive batches. The
//! module is split by concern:
//!
//! * `logs` - log-event inserts,
//! * `identity` - stable SHA-256 fingerprint IDs and the resource upsert
//!   keyed by them.
//!
//! Statement reuse relies on rusqlite's `prepare_cached`; the SQL strings are
//! module-private constants next to their only callers.

use otel_sqlite_core::model::Resource;

mod identity;
mod logs;

pub(crate) use logs::{SalvageOutcome, insert_logs, insert_logs_tolerant};

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

/// Reusable serialization scratch for the writer.
///
/// The insert hot path serializes an attribute set and a SHA-256 fingerprint
/// per record. These buffers are created once and cleared/reused per row
/// instead of allocating fresh strings every time. Fields are exposed to the
/// sibling `command` modules so callers can work with disjoint borrows.
pub(crate) struct InsertScratch {
    /// Batch-level resource id (`resolve_resource`).
    pub(crate) resource_id: String,
    /// Output buffer for the current row's attributes JSON.
    pub(crate) json: String,
    /// Key-ordering indices used by the canonical attribute encoder.
    pub(crate) order: Vec<u32>,
    /// Recently resolved origins. Fingerprints and canonical attributes JSON
    /// are invariant across batches, so repeated traffic from the same handful
    /// of resources avoids re-serializing and re-hashing.
    pub(crate) resources: ResourceCache,
}

impl InsertScratch {
    pub(crate) fn new() -> Self {
        // Everything grows on first use: idle writers allocate no scratch.
        Self {
            resource_id: String::new(),
            json: String::new(),
            order: Vec::new(),
            resources: ResourceCache::new(),
        }
    }
}

impl ResourceCache {
    /// Bounded because the active origin set is small; the oldest entry is
    /// evicted once the cap is reached.
    const MAX_ENTRIES: usize = 8;

    const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Returns the cached fingerprint and canonical attributes JSON for
    /// `resource`, refreshing its position so the hot origin stays at the
    /// front.
    pub(crate) fn lookup(&mut self, resource: &Resource) -> Option<(&str, &str)> {
        let index = self
            .entries
            .iter()
            .position(|(cached, _, _)| cached == resource)?;
        if index > 0 {
            let entry = self.entries.remove(index);
            self.entries.insert(0, entry);
        }
        self.entries
            .first()
            .map(|(_, id, json)| (id.as_str(), json.as_str()))
    }

    pub(crate) fn insert(&mut self, resource: &Resource, id: &str, json: &str) {
        if self.entries.len() == Self::MAX_ENTRIES {
            self.entries.pop();
        }
        self.entries
            .insert(0, (resource.clone(), id.to_owned(), json.to_owned()));
    }
}

pub(crate) struct ResourceCache {
    entries: Vec<(Resource, String, String)>,
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

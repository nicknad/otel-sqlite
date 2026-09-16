use std::sync::{Arc, LazyLock};

use super::attribute::{Attribute, write_attributes_json_into};
use super::resource::Resource;
use super::severity::Severity;

/// Instrumentation scope dimensions, shared by every record of one OTLP
/// `ScopeLogs` group.
///
/// The canonical flat JSON rendering of `attributes` is computed exactly once
/// in [`LogScope::new`] and cached, so the mapping layer can precompute it on
/// the parallel ingest path and the writer binds the bytes without per-record
/// sorting or serialization.
#[derive(Debug, Clone, PartialEq)]
pub struct LogScope {
    name: String,
    version: String,
    attributes: Vec<Attribute>,
    schema_url: String,
    attributes_json: String,
}

impl LogScope {
    pub fn new(
        name: String,
        version: String,
        attributes: Vec<Attribute>,
        schema_url: String,
    ) -> Self {
        let mut attributes_json = String::new();
        let mut order = Vec::new();
        write_attributes_json_into(&attributes, &mut order, &mut attributes_json);
        Self {
            name,
            version,
            attributes,
            schema_url,
            attributes_json,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn attributes(&self) -> &[Attribute] {
        &self.attributes
    }

    pub fn schema_url(&self) -> &str {
        &self.schema_url
    }

    /// Canonical JSON object of [`LogScope::attributes`], rendered once at
    /// construction.
    pub fn attributes_json(&self) -> &str {
        &self.attributes_json
    }
}

impl Default for LogScope {
    fn default() -> Self {
        Self::new(String::new(), String::new(), Vec::new(), String::new())
    }
}

/// Shared empty scope for [`LogRecord::default`], so defaulted records (tests,
/// benches, fuzz harnesses) neither allocate a scope nor render `{}` per call.
static EMPTY_SCOPE: LazyLock<Arc<LogScope>> = LazyLock::new(|| Arc::new(LogScope::default()));

#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub time_unix_nano: i64,
    pub observed_time_unix_nano: i64,
    pub severity_number: Severity,
    pub severity_text: String,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub body: String,
    /// Canonical JSON rendering of a structured body (`ArrayValue` /
    /// `KvlistValue`). Scalar bodies stay in `body`; when present this is what
    /// gets persisted in the `body` column.
    pub body_json: Option<String>,
    pub attributes: Vec<Attribute>,
    pub dropped_attributes_count: u32,
    pub flags: u32,
    pub event_name: String,
    /// Reserved: resource identity is derived at persist time from [`resource`]
    /// (or the batch origin); the writer never reads this field.
    ///
    /// [`resource`]: LogRecord::resource
    pub resource_id: String,
    pub resource: Option<Resource>,
    /// Shared scope metadata; records from one OTLP `ScopeLogs` group point at
    /// the same allocation, so neither the strings nor the attribute set are
    /// cloned per record.
    pub scope: Arc<LogScope>,
}

impl Default for LogRecord {
    fn default() -> Self {
        Self {
            time_unix_nano: 0,
            observed_time_unix_nano: 0,
            severity_number: Severity::default(),
            severity_text: String::new(),
            trace_id: [0; 16],
            span_id: [0; 8],
            body: String::new(),
            body_json: None,
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            flags: 0,
            event_name: String::new(),
            resource_id: String::new(),
            resource: None,
            scope: Arc::clone(&EMPTY_SCOPE),
        }
    }
}

impl LogRecord {
    pub fn has_trace(&self) -> bool {
        self.trace_id != [0; 16]
    }

    pub fn has_span(&self) -> bool {
        self.span_id != [0; 8]
    }

    pub fn has_trace_context(&self) -> bool {
        self.has_trace() && self.has_span()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LogBatch {
    pub records: Vec<LogRecord>,
    pub resource: Option<Resource>,
    pub schema_url: String,
}

impl LogBatch {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            records: Vec::with_capacity(capacity),
            ..Self::default()
        }
    }

    pub fn push(&mut self, record: LogRecord) {
        self.records.push(record);
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

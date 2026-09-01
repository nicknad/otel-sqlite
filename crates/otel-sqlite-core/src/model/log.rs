use super::attribute::Attribute;
use super::resource::Resource;
use super::severity::Severity;

#[derive(Debug, Clone, PartialEq, Default)]
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
    pub resource_id: String,
    pub resource: Option<Resource>,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attributes: Vec<Attribute>,
    pub scope_schema_url: String,
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

    pub fn clear(&mut self) {
        self.records.clear();
    }
}

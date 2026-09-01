//! Shared run parameters and per-run execution context.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::metrics::TelemetryHandle;
use crate::server::EmbeddedServer;

#[derive(Clone)]
pub struct RunContext {
    pub endpoint: String,
    pub db_path: PathBuf,
    pub seed: u64,
    pub body_size: usize,
    pub attributes_per_record: usize,
    pub resource_count: usize,
    pub warmup_secs: u64,
    pub request_timeout: Duration,
    pub drain_timeout: Duration,
    pub telemetry: TelemetryHandle,
    /// Present only in embedded mode; enables explicit Flush commands.
    pub server: Option<Arc<EmbeddedServer>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Overrides {
    pub duration_secs: Option<u64>,
    pub clients: Option<usize>,
    pub records_per_request: Option<usize>,
    pub offered_records_per_second: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StepParams {
    /// Stable identifier used inside the `bench.run_id` attribute.
    pub label: String,
    pub clients: usize,
    pub records_per_request: usize,
    /// Offered load; `None` runs closed-loop (workers send without pacing).
    pub offered_records_per_second: Option<u64>,
    pub duration_secs: u64,
    /// Override for distinct OTLP resources in the workload.
    ///
    /// Closed-loop overload runs pin this to 1: together with
    /// `records_per_request <= 512` every request then maps to exactly one
    /// storage batch, so rejections are atomic whole requests and sequence
    /// positions stay exact for validation.
    pub resource_count: Option<usize>,
}

pub(crate) fn sanitize(label: &str) -> String {
    label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

pub(crate) fn fresh_run_id(label: &str, seed: u64) -> String {
    let unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    format!("{}-{seed}-{unix_secs}", sanitize(label))
}

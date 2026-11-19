//! Library surface of the otel-sqlite package.
//!
//! The package exists to ship the `otel-sqlite` binary; its library target
//! exists so the end-to-end harnesses can boot the exact same pipeline
//! assembly the binary boots (see `src/pipeline.rs`). Nothing else is
//! exported.

mod pipeline;

pub use pipeline::{DrainReport, FlushRefused, Pipeline, PipelineError, PipelineSpecs};

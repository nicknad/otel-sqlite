//! Benchmark harness library: reusable pieces behind the
//! `otel-sqlite-e2e` binary, also exercised by integration tests.

// Test harness: assertions and quick probes use unwrap() throughout.
#![allow(clippy::unwrap_used)]

pub mod client;
pub mod crash;
pub mod db;
pub mod generator;
pub mod interval_set;
pub mod ledger;
pub mod metrics;
pub mod pacer;
pub mod params;
pub mod report;
pub mod runner;
pub mod scenarios;
pub mod server;
pub mod validation;

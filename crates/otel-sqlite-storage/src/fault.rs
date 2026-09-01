//! Fault-injection knobs for crash/durability tests (chaos testing).
//!
//! Each knob is a plain environment variable that is **inert unless set** to
//! a positive integer: production deployments never set them, so the writer
//! and batcher behave exactly as before. When armed, the named component
//! exits through its normal death path after the configured number of
//! operations — clearing thread liveness, closing the durability ledger (for
//! the writer) and stopping ingestion — precisely what a real unrecoverable
//! failure does.
//!
//! The e2e crash test (`crates/otel-sqlite/tests/crash_durability.rs`) uses
//! these to verify that a *single* pipeline thread can die while the process
//! lives: pending durable acks fail with `UNAVAILABLE`, health flips to
//! `NOT_SERVING`, and the watchdog halts ingestion for a supervisor restart.

/// `OTEL_SQLITE_FAULT_WRITER_AFTER_N`: make the SQLite writer exit via its
/// fatal-error path after executing `N` commands.
pub(crate) const WRITER_AFTER_N: &str = "OTEL_SQLITE_FAULT_WRITER_AFTER_N";

/// `OTEL_SQLITE_FAULT_BATCHER_AFTER_N`: make the insert batcher stop
/// consuming after receiving `N` events.
pub(crate) const BATCHER_AFTER_N: &str = "OTEL_SQLITE_FAULT_BATCHER_AFTER_N";

/// Parses a fault knob: `Some(limit)` when the env var is set to a positive
/// integer, `None` otherwise (the safe default for every production run).
/// Parses a fault knob: `Some(limit)` when the env var is set to a positive
/// integer, `None` otherwise (the safe default for every production run).
pub(crate) fn armed_after(env: &str) -> Option<u64> {
    parse_armed(std::env::var(env).ok())
}

/// Pure parsing half of [`armed_after`]; kept separate so tests need not
/// mutate process environment (which is `unsafe` under edition 2024).
fn parse_armed(value: Option<String>) -> Option<u64> {
    value
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|limit| *limit > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_or_invalid_knobs_are_inert() {
        assert_eq!(parse_armed(None), None);
        for bad in ["", " ", "zero", "-1", "0", "1.5", "abc"] {
            assert_eq!(parse_armed(Some(bad.to_owned())), None, "value `{bad}`");
        }
    }

    #[test]
    fn positive_integer_knobs_arm_the_fault() {
        assert_eq!(parse_armed(Some(" 42 ".to_owned())), Some(42));
        assert_eq!(parse_armed(Some("1".to_owned())), Some(1));
    }
}

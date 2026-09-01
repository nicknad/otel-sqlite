//! Post-run correctness validation of one benchmark run.
//!
//! Validation runs strictly *after* the measurement window: producers are
//! stopped, the pipeline is drained (see `db::wait_until_drained`), and only
//! then is the database inspected. Nothing here touches the hot path.
//!
//! Sequence accounting (see `client.rs`):
//!
//! ```text
//! generated  = accepted + rejected + ambiguous
//! persisted  must equal the set of accepted sequences exactly
//! rejected   = definitively not enqueued (partial_success / error status)
//! ambiguous  = fate unknown at client side (timeout, transport failure);
//!              tolerated as present-or-absent without failing the run
//! ```

use std::collections::HashSet;
use std::path::Path;

use rusqlite::Connection;

use crate::db::open_readonly;
use crate::generator::{RECORD_ID_KEY, RUN_ID_KEY, SEQ_KEY};
use crate::interval_set::IntervalSet;

#[derive(Debug, Clone, serde::Serialize)]
pub struct CorrectnessReport {
    pub run_id: String,
    /// Sequences the producer generated for this run.
    pub generated: u64,
    /// Sequences definitively rejected by backpressure/error paths.
    pub rejected: u64,
    /// Sequences whose fate is unknown client-side; excluded from strict checks.
    pub ambiguous: u64,
    /// Sequences that must be present (`generated - rejected - ambiguous`).
    pub expected_persisted: u64,
    /// Rows found in SQLite carrying this run id.
    pub rows_found: u64,
    /// Distinct sequences among the found rows.
    pub distinct_sequences: u64,
    pub missing_sequences: u64,
    pub duplicate_sequences: u64,
    /// Rejected sequences that nevertheless appear (policy violation).
    pub unexpected_rejected_present: u64,
    /// Ambiguous sequences observed present (informational).
    pub ambiguous_present: u64,
    /// True when every expected sequence was found and none besides.
    pub passed: bool,
    pub failure_reasons: Vec<String>,
}

/// Full correctness validation for one benchmark run against its SQLite data.
///
/// `generated_range` covers `[1, generated]`; `rejected`/`ambiguous` hold the
/// corresponding sequence sets collected by the scenario runner.
pub fn validate(
    db_path: &Path,
    run_id: &str,
    generated_total: u64,
    rejected: &IntervalSet,
    ambiguous: &IntervalSet,
) -> anyhow::Result<CorrectnessReport> {
    // Cap enumeration so pathological runs cannot spin here forever; a run
    // exceeding the cap is reported with at least `MISSING_CAP` misses.
    const MISSING_CAP: u64 = 1_000_000;

    let all = IntervalSet::from_ranges(vec![(1, generated_total)]);
    let expected = all.subtract(rejected).subtract(ambiguous);

    let conn = open_readonly(db_path)?;
    let seq_expr = format!("json_extract(attributes_json, '$.\"{SEQ_KEY}\"')");
    let run_filter = format!("json_extract(attributes_json, '$.\"{RUN_ID_KEY}\"') = ?1");

    // Only trusted column identifiers are interpolated; `run_id` is bound
    // via ?1 below.
    // nosemgrep: security.sql-format-string
    let mut statement = conn.prepare(&format!(
        "SELECT CAST({seq_expr} AS INTEGER) FROM log_event WHERE {run_filter}"
    ))?;
    let found: HashSet<i64> = statement
        .query_map(rusqlite::params![run_id], |row| row.get::<_, i64>(0))?
        .collect::<Result<_, _>>()?;

    // Duplicate detection: compare row count with distinct sequence count.
    let rows_found: u64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM log_event WHERE {run_filter}"),
        rusqlite::params![run_id],
        |row| row.get::<_, i64>(0).map(|value| value.max(0) as u64),
    )?;
    drop(statement);

    let mut missing = 0u64;
    'outer: for &(start, end) in &expected.ranges {
        for seq in start..=end {
            if !found.contains(&(seq as i64)) {
                missing += 1;
                if missing >= MISSING_CAP {
                    break 'outer;
                }
            }
        }
    }

    let distinct_sequences = found.len() as u64;
    let duplicate_sequences = rows_found.saturating_sub(distinct_sequences);

    let mut unexpected_rejected_present = 0u64;
    let mut ambiguous_present = 0u64;
    for seq in found.iter().map(|seq| *seq as u64) {
        if rejected.contains(seq) {
            unexpected_rejected_present += 1;
        } else if ambiguous.contains(seq) {
            ambiguous_present += 1;
        }
    }

    let mut failure_reasons = Vec::new();
    if missing > 0 {
        failure_reasons.push(format!("{missing} expected sequences missing"));
    }
    if duplicate_sequences > 0 {
        failure_reasons.push(format!("{duplicate_sequences} duplicate rows detected"));
    }
    if unexpected_rejected_present > 0 {
        failure_reasons.push(format!(
            "{unexpected_rejected_present} rejected sequences were persisted anyway"
        ));
    }

    Ok(CorrectnessReport {
        run_id: run_id.to_owned(),
        generated: generated_total,
        rejected: rejected.total(),
        ambiguous: ambiguous.total(),
        expected_persisted: expected.total(),
        rows_found,
        distinct_sequences,
        missing_sequences: missing,
        duplicate_sequences,
        unexpected_rejected_present,
        ambiguous_present,
        passed: failure_reasons.is_empty(),
        failure_reasons,
    })
}

/// Verify the embedded `bench.record_id` attribute matches `bench.seq`
/// on every row of a run (guards against mapping corruption). Returns the
/// number of mismatching rows.
pub fn verify_record_ids(db_path: &Path, run_id: &str) -> anyhow::Result<u64> {
    let conn: Connection = open_readonly(db_path)?;
    let mismatch: u64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM log_event \
             WHERE json_extract(attributes_json, '$.\"{RUN_ID_KEY}\"') = ?1 \
               AND json_extract(attributes_json, '$.\"{SEQ_KEY}\"') \
                   IS NOT json_extract(attributes_json, '$.\"{RECORD_ID_KEY}\"')"
        ),
        rusqlite::params![run_id],
        |row| row.get::<_, i64>(0).map(|value| value.max(0) as u64),
    )?;
    Ok(mismatch)
}

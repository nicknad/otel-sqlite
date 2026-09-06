use std::io;

/// How the writer should react to a failed persistence attempt.
///
/// * [`FailureClass::Retryable`] — transient contention (`SQLITE_BUSY`,
///   `SQLITE_LOCKED`): the identical transaction is worth retrying after a
///   bounded backoff.
/// * [`FailureClass::Poison`] — deterministic data problems (constraint
///   violations, parameter conversion): retrying can never help; the batch is
///   re-run in tolerant mode so healthy rows survive and offending rows are
///   dropped-and-counted.
/// * [`FailureClass::Fatal`] — environmental or corruption-level failures
///   (disk full, IO error, corrupt database): continuing would silently lose
///   data, so the writer halts and the watchdog/supervisor take over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureClass {
    Retryable,
    Poison,
    Fatal,
}

/// Classifies a raw SQLite error.
///
/// Deliberately **fatal-by-default**: only error shapes we can attribute to
/// transient contention or to the specific persisted row downgrade the
/// class. Unknown shapes may be defects in our own SQL usage, and silently
/// dropping user data over one of those would convert our bug into hidden
/// loss — failing loud is the safe direction.
pub(crate) fn classify_sqlite(error: &rusqlite::Error) -> FailureClass {
    use rusqlite::ffi::ErrorCode as Code;
    match error {
        rusqlite::Error::SqliteFailure(failure, _) => match failure.code {
            Code::DatabaseBusy | Code::DatabaseLocked => FailureClass::Retryable,
            code if is_constraint_code(code) => FailureClass::Poison,
            _ => FailureClass::Fatal,
        },
        // Data-driven binding failures are row-local: salvage can drop just
        // the offending record.
        rusqlite::Error::ToSqlConversionFailure(_) => FailureClass::Poison,
        // Everything else is a defect in our own statement/usage — halt.
        _ => FailureClass::Fatal,
    }
}

fn is_constraint_code(code: rusqlite::ffi::ErrorCode) -> bool {
    // libsqlite3-sys exposes only the primary result code; the specific
    // violated constraint lives in the extended code, which we do not need
    // for classification purposes.
    code == rusqlite::ffi::ErrorCode::ConstraintViolation
}

impl StorageError {
    /// Classification driving the writer's retry/salvage/halt policy.
    pub(crate) fn class(&self) -> FailureClass {
        match self {
            Self::Sqlite(error) => classify_sqlite(error),
            Self::Io(_)
            | Self::WriterPanicked
            | Self::BatcherPanicked
            | Self::FaultInjected { .. }
            | Self::StartupTimeout { .. }
            | Self::StartupFailed { .. }
            | Self::ShutdownTimeout { .. } => FailureClass::Fatal,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("sqlite failure: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("storage io failure: {0}")]
    Io(#[from] io::Error),
    #[error("writer thread panicked")]
    WriterPanicked,
    #[error("insert batcher thread panicked")]
    BatcherPanicked,
    /// Crash/durability-test fault injection: the named component exited
    /// deliberately after an env-configured number of operations (see the
    /// `OTEL_SQLITE_FAULT_*` knobs in `crate::fault`). Never produced by a
    /// real workload; classified fatal because it walks the same exit path as
    /// an unrecoverable storage error.
    #[error("fault injection: {component} exited deliberately")]
    FaultInjected { component: &'static str },
    #[error("storage did not become ready within {timeout_secs}s")]
    StartupTimeout { timeout_secs: u64 },
    #[error("storage startup failed: {message}")]
    StartupFailed { message: String },
    /// The writer/batcher did not finish draining within the shutdown
    /// budget. Threads are detached, not killed: the caller should treat the
    /// process as unsafe to reuse and exit so an external supervisor can
    /// restart it.
    #[error("storage did not shut down within {timeout_secs}s")]
    ShutdownTimeout { timeout_secs: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::ffi;

    fn sqlite_failure(code: i32) -> StorageError {
        StorageError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            None,
        ))
    }

    #[test]
    fn busy_and_locked_are_retryable() {
        assert_eq!(
            classify_sqlite(&rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                None
            )),
            FailureClass::Retryable
        );
        assert_eq!(
            classify_sqlite(&rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_LOCKED),
                None
            )),
            FailureClass::Retryable
        );
        // Extended BUSY codes share the primary code.
        assert_eq!(
            sqlite_failure(ffi::SQLITE_BUSY).class(),
            FailureClass::Retryable
        );
    }

    #[test]
    fn constraint_violations_are_poison() {
        use rusqlite::ffi;
        // libsqlite3-sys exposes only the primary constraint code; extended
        // codes (which constraint fired) are not distinguishable here.
        assert_eq!(
            sqlite_failure(ffi::SQLITE_CONSTRAINT).class(),
            FailureClass::Poison,
            "constraint violations must classify as poison"
        );
    }

    #[test]
    fn row_local_binding_failures_are_poison_but_our_bugs_are_fatal() {
        // Data-driven conversion failures affect a single row -> poison.
        let conversion = StorageError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(
            std::io::Error::new(std::io::ErrorKind::InvalidData, "bad value"),
        )));
        assert_eq!(conversion.class(), FailureClass::Poison);

        // Parameter-count mismatches are OUR statement bugs: fail loud
        // instead of silently dropping user data.
        let our_bug = StorageError::Sqlite(rusqlite::Error::InvalidParameterCount(1, 2));
        assert_eq!(our_bug.class(), FailureClass::Fatal);
    }
    #[test]
    fn environmental_failures_are_fatal() {
        use rusqlite::ffi;
        for code in [ffi::SQLITE_FULL, ffi::SQLITE_IOERR, ffi::SQLITE_NOTADB] {
            assert_eq!(
                sqlite_failure(code).class(),
                FailureClass::Fatal,
                "code {code:?} must classify as fatal"
            );
        }
        assert_eq!(
            StorageError::Io(std::io::ErrorKind::Other.into()).class(),
            FailureClass::Fatal
        );
        assert_eq!(
            StorageError::WriterPanicked.class(),
            FailureClass::Fatal,
            "a dead writer thread walks the same halt path as a fatal sqlite failure"
        );
        assert_eq!(
            classify_sqlite(&rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CORRUPT),
                None
            )),
            FailureClass::Fatal
        );
    }
}

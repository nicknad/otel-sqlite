//! Durability policy vocabulary shared by ingress, storage and the binary
//! config layer.
//!
//! * [`DurabilityMode`] decides **when an OTLP request is acknowledged**:
//!   after the records are enqueued into the in-memory pipeline (fast, but a
//!   crash may lose acknowledged records) or only after the SQLite writer has
//!   committed them (the default; the ack is then backed by a transaction).
//! * [`SyncMode`] maps onto SQLite's `synchronous` pragma and decides how
//!   aggressively commits are flushed to disk.

/// When a successful OTLP export response is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum DurabilityMode {
    /// Acknowledge once the writer has committed the accepted records. A 2xx
    /// means the data survives an application crash (with
    /// [`SyncMode::Normal`]) or a power failure (with [`SyncMode::Full`]).
    /// Backpressure propagates to clients as extra ack latency instead of an
    /// at-risk in-memory backlog.
    #[default]
    Commit,
    /// Acknowledge as soon as the records entered the bounded ingest queue.
    /// Lowest latency; acknowledged records may still be lost to a crash
    /// before the writer commits them.
    Enqueue,
}

impl DurabilityMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Enqueue => "enqueue",
        }
    }

    /// Parses the canonical lowercase spelling used in configuration files.
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "commit" => Some(Self::Commit),
            "enqueue" => Some(Self::Enqueue),
            _ => None,
        }
    }
}

/// SQLite `synchronous` durability level.
///
/// Under WAL, [`SyncMode::Normal`] syncs the write-ahead log only at
/// checkpoints: an application crash loses nothing already committed, but a
/// power failure may roll back the most recent transactions.
/// [`SyncMode::Full`] syncs on every commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum SyncMode {
    #[default]
    Normal,
    Full,
}

impl SyncMode {
    /// Pragma value understood by SQLite (`PRAGMA synchronous = ..`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
        }
    }

    /// Parses the canonical spelling used in configuration files.
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "normal" => Some(Self::Normal),
            "full" => Some(Self::Full),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durability_mode_round_trips() {
        assert_eq!(
            DurabilityMode::parse("commit"),
            Some(DurabilityMode::Commit)
        );
        assert_eq!(
            DurabilityMode::parse(" ENQUEUE "),
            Some(DurabilityMode::Enqueue)
        );
        assert_eq!(DurabilityMode::parse("durable"), None);
        assert_eq!(DurabilityMode::default().as_str(), "commit");
    }

    #[test]
    fn sync_mode_round_trips() {
        assert_eq!(SyncMode::parse("full"), Some(SyncMode::Full));
        assert_eq!(SyncMode::parse("NORMAL"), Some(SyncMode::Normal));
        assert_eq!(SyncMode::parse("off"), None);
        assert_eq!(SyncMode::default().as_str(), "NORMAL");
    }
}

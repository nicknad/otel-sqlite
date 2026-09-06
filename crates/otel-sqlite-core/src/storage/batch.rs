//! Storage-sized write batches.
//!
//! A [`WriteBatch`] is a batch of records that is ready for persistence. It is
//! produced by the storage-owned [`InsertBatcher`](super::batcher::InsertBatcher)
//! and consumed by the SQLite writer, which persists each batch in exactly one
//! transaction. Records are moved into (and out of) the batch by ownership
//! transfer; nothing on this path clones record payloads.

/// A batch of records sized for one storage transaction.
///
/// `T` is the record type (`LogRecord`, `MetricRecord`, ...). The batch owns
/// its records outright so handing it over to a writer never copies payloads.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteBatch<T> {
    records: Vec<T>,
}

impl<T> WriteBatch<T> {
    /// Creates an empty batch with room for `capacity` records.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            records: Vec::with_capacity(capacity),
        }
    }

    /// Wraps an existing vector without copying or reallocating.
    pub fn from_vec(records: Vec<T>) -> Self {
        Self { records }
    }

    /// Number of records held by this batch.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// `true` when the batch carries no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Read-only view of the buffered records.
    pub fn records(&self) -> &[T] {
        &self.records
    }

    /// Unwraps the batch, transferring ownership of all records.
    pub fn into_records(self) -> Vec<T> {
        self.records
    }
}

impl<T> Default for WriteBatch<T> {
    fn default() -> Self {
        Self {
            records: Vec::new(),
        }
    }
}

impl<T> From<Vec<T>> for WriteBatch<T> {
    fn from(records: Vec<T>) -> Self {
        Self::from_vec(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_vector_without_copy() {
        let records = vec![1u64, 2, 3];
        let pointer = records.as_ptr();

        let batch = WriteBatch::from_vec(records);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch.records().as_ptr(), pointer);

        let unwrapped = batch.into_records();
        assert_eq!(unwrapped.as_ptr(), pointer);
        assert_eq!(unwrapped, vec![1, 2, 3]);
    }
}

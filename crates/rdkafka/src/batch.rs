//! Reusable typed column batches.
use crate::{BufferPolicy, Record, Result, WireBatch};
/// Structure-of-arrays storage for one typed record schema.
pub trait Batch: Default {
    /// Schema stored by this batch.
    type Record: Record;
    /// Allocate capacity for the requested record count.
    /// # Panics
    /// Panics if the requested inline allocation exceeds the vector size limit.
    fn with_capacity(records: usize) -> Self;
    /// Number of stored records.
    fn len(&self) -> usize;
    /// Whether the batch contains no records.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Aggregate allocated bytes across every column.
    fn allocated_bytes(&self) -> usize;
    /// Clear using the default aggregate retention budget.
    fn clear(&mut self) {
        self.clear_with_policy(BufferPolicy::default());
    }
    /// Clear lengths and enforce an aggregate retention budget.
    fn clear_with_policy(&mut self, policy: BufferPolicy);
    /// Clear without releasing warmed capacity.
    fn clear_unbounded(&mut self) {
        self.clear_with_policy(BufferPolicy::unbounded());
    }
    /// Release unused capacity without discarding records.
    fn shrink_to_fit(&mut self);
    /// Append one owned record's fields to the columns.
    fn push(&mut self, record: &Self::Record);
    /// Append a borrowed projection to the columns after validating its fields.
    /// # Errors
    /// Returns an error if a projected collection cannot be decoded. No columns
    /// are appended until all fallible field decoding succeeds.
    fn push_ref<'a>(&mut self, record: <Self::Record as Record>::View<'a>) -> Result<()>;
    /// Append records from a slice.
    fn extend_from_slice(&mut self, records: &[Self::Record]) {
        for record in records {
            self.push(record);
        }
    }
    /// Encode all columns into wire arenas.
    /// # Errors
    /// Returns an error for inconsistent column lengths or invalid fields.
    fn to_wire_batch(&self) -> Result<WireBatch>;
}

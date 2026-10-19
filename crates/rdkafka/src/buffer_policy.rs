//! Aggregate capacity budgets for reusable record buffers.

/// Maximum capacities retained after clearing a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPolicy {
    /// Maximum retained record capacity.
    pub max_records: usize,
    /// Maximum aggregate retained vector capacity in bytes.
    pub max_bytes: usize,
}
impl Default for BufferPolicy {
    fn default() -> Self {
        Self {
            max_records: 65_536,
            max_bytes: 8 * 1024 * 1024,
        }
    }
}
impl BufferPolicy {
    /// Keep all warmed capacity.
    pub const fn unbounded() -> Self {
        Self {
            max_records: usize::MAX,
            max_bytes: usize::MAX,
        }
    }
    /// Release all capacity when cleared.
    pub const fn minimal() -> Self {
        Self {
            max_records: 0,
            max_bytes: 0,
        }
    }
    /// Reserve retained inline slots before distributing the remaining byte budget.
    #[doc(hidden)]
    pub fn retention(self, records: usize, bytes: usize, inline_bytes: usize) -> Retention {
        let record_scale = Scale::bounded(self.max_records, records)
            .min(Scale::bounded(self.max_bytes, inline_bytes));
        let reserved = record_scale.target(inline_bytes);
        let nested_bytes = bytes.saturating_sub(inline_bytes);
        Retention {
            records: record_scale,
            nested: Scale::bounded(self.max_bytes.saturating_sub(reserved), nested_bytes),
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct Scale {
    numerator: usize,
    denominator: usize,
}
impl Scale {
    fn bounded(limit: usize, current: usize) -> Self {
        if current > limit {
            Self {
                numerator: limit,
                denominator: current,
            }
        } else {
            Self {
                numerator: 1,
                denominator: 1,
            }
        }
    }
    fn min(self, other: Self) -> Self {
        if (self.numerator as u128) * (other.denominator as u128)
            < (other.numerator as u128) * (self.denominator as u128)
        {
            self
        } else {
            other
        }
    }
    fn target(self, capacity: usize) -> usize {
        ((capacity as u128 * self.numerator as u128) / self.denominator as u128) as usize
    }
}
/// Separate outer record-slot and nested allocation retention budgets.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct Retention {
    records: Scale,
    nested: Scale,
}
impl Retention {
    /// Nested capacities share one scale; rounding down bounds their aggregate sum.
    pub fn target(self, capacity: usize) -> usize {
        self.nested.target(capacity)
    }
    /// Bound inline record slots independently of nested payload bursts.
    pub fn record_target(self, capacity: usize) -> usize {
        self.records.target(capacity)
    }
}
/// Capacity accounting used by generated column buffers.
pub trait MemoryBuffer {
    /// Aggregate vector allocation bytes, excluding allocator bookkeeping.
    fn allocated_bytes(&self) -> usize;
    /// Bytes occupied by the outer record slots, excluding nested allocations.
    fn inline_bytes(&self) -> usize {
        self.allocated_bytes()
    }
    /// Capacity in records rather than bytes.
    fn record_capacity(&self) -> usize;
    /// Clear and trim using a shared aggregate scale.
    fn clear_retained(&mut self, retention: Retention);
    /// Clear using the default record and aggregate byte retention budget.
    fn clear(&mut self) {
        self.clear_with_policy(BufferPolicy::default());
    }
    /// Clear while preserving all warmed allocations.
    fn clear_unbounded(&mut self) {
        self.clear_with_policy(BufferPolicy::unbounded());
    }
    /// Clear using independent record-slot and nested allocation budgets.
    fn clear_with_policy(&mut self, policy: BufferPolicy) {
        let retention = policy.retention(
            self.record_capacity(),
            self.allocated_bytes(),
            self.inline_bytes(),
        );
        self.clear_retained(retention);
    }
    /// Release unused capacity while retaining values.
    fn shrink_to_fit(&mut self);
}
impl<T> MemoryBuffer for Vec<T> {
    fn allocated_bytes(&self) -> usize {
        self.capacity().saturating_mul(size_of::<T>())
    }
    fn record_capacity(&self) -> usize {
        if size_of::<T>() == 0 {
            0
        } else {
            self.capacity()
        }
    }
    fn clear_retained(&mut self, retention: Retention) {
        let target = retention.record_target(self.record_capacity());
        self.clear();
        if target < self.capacity() {
            self.shrink_to(target);
        }
    }
    fn shrink_to_fit(&mut self) {
        Vec::shrink_to_fit(self);
    }
}

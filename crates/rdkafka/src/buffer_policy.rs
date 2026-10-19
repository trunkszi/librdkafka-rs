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
    /// Compute one proportional budget shared by all child buffers.
    #[doc(hidden)]
    pub fn retention(self, records: usize, bytes: usize) -> Retention {
        let mut result = Retention {
            numerator: 1,
            denominator: 1,
        };
        for (limit, current) in [(self.max_records, records), (self.max_bytes, bytes)] {
            if current > limit
                && (limit as u128) * (result.denominator as u128)
                    < (result.numerator as u128) * (current as u128)
            {
                result = Retention {
                    numerator: limit,
                    denominator: current,
                };
            }
        }
        result
    }
}
/// Shared integer scaling factor for retained capacities.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct Retention {
    numerator: usize,
    denominator: usize,
}
impl Retention {
    /// Round each child budget down so their sum stays within the aggregate limit.
    pub fn target(self, capacity: usize) -> usize {
        ((capacity as u128 * self.numerator as u128) / self.denominator as u128) as usize
    }
}
/// Capacity accounting used by generated column buffers.
pub trait MemoryBuffer {
    /// Aggregate vector allocation bytes, excluding allocator bookkeeping.
    fn allocated_bytes(&self) -> usize;
    /// Capacity in records rather than bytes.
    fn record_capacity(&self) -> usize;
    /// Clear and trim using a shared aggregate scale.
    fn clear_retained(&mut self, retention: Retention);
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
        let target = retention.target(self.record_capacity());
        self.clear();
        if target < self.capacity() {
            self.shrink_to(target);
        }
    }
    fn shrink_to_fit(&mut self) {
        Vec::shrink_to_fit(self);
    }
}

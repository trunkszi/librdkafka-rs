//! Register-passed policy values retain the same column storage budget.
use rdkafka::{BufferPolicy, buffer_policy::MemoryBuffer, column::ReusableColumn};
use uarch::layout::NoPadding;

fn pass_policy<T: NoPadding + Copy>(policy: T) -> T {
    policy
}

/// Crossing the value boundary must retain both the record and byte limits.
#[test]
fn register_policy_keeps_retention() {
    let policy = pass_policy(BufferPolicy {
        max_records: 2,
        max_bytes: 64,
    });
    let mut column = ReusableColumn::with_capacity(16);
    column.push(&"burst".repeat(1024));
    column.clear_with_policy(policy);
    assert!(column.is_empty());
    assert!(column.record_capacity() <= 2);
    assert!(column.allocated_bytes() <= 64);
}

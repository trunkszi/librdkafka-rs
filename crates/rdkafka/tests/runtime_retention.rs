use rdkafka::{
    BufferPolicy, Error, Result, SerdeBridge, WireBatch, WireRecord,
    buffer_policy::MemoryBuffer,
    column::{ColumnValue, ReusableColumn},
};

#[test]
fn unit_scenario_nested_byte_burst_preserves_outer_record_slots() {
    let mut column = ReusableColumn::with_capacity(16);
    column.push(&"x".repeat(1024 * 1024));
    let slots = column.record_capacity();
    column.clear_with_policy(BufferPolicy {
        max_records: 16,
        max_bytes: 4096,
    });
    assert_eq!(column.record_capacity(), slots);
    assert!(column.allocated_bytes() <= 4096);
    let retained = column.allocated_bytes();
    column.push(&"small".to_owned());
    column.clear_with_policy(BufferPolicy {
        max_records: 16,
        max_bytes: 4096,
    });
    assert_eq!(column.record_capacity(), slots);
    assert_eq!(column.allocated_bytes(), retained);
}

#[test]
fn unit_scenario_inline_reservation_bounds_multiple_columns_without_losing_warm_slots() {
    let mut text = ReusableColumn::with_capacity(8);
    let mut number = ReusableColumn::with_capacity(8);
    text.push(&"x".repeat(1024 * 1024));
    number.push(&42u64);
    let policy = BufferPolicy {
        max_records: 8,
        max_bytes: 1024,
    };
    let retention = policy.retention(
        8,
        text.allocated_bytes() + number.allocated_bytes(),
        text.inline_bytes() + number.inline_bytes(),
    );
    text.clear_retained(retention);
    number.clear_retained(retention);
    assert_eq!(text.record_capacity(), 8);
    assert_eq!(number.record_capacity(), 8);
    assert!(text.allocated_bytes() + number.allocated_bytes() <= policy.max_bytes);
    let tiny = BufferPolicy {
        max_records: 1,
        max_bytes: 7,
    };
    let retention = tiny.retention(
        8,
        text.allocated_bytes() + number.allocated_bytes(),
        text.inline_bytes() + number.inline_bytes(),
    );
    text.clear_retained(retention);
    number.clear_retained(retention);
    assert!(text.allocated_bytes() + number.allocated_bytes() <= tiny.max_bytes);
}

#[test]
fn integration_scenario_wire_byte_burst_keeps_record_descriptors_and_bounds_arenas() -> Result<()> {
    let mut batch = WireBatch::with_capacity(16);
    batch.push(WireRecord::new("topic", Some(&vec![0; 1024 * 1024])))?;
    let capacity = batch.capacity();
    batch.clear_with_policy(BufferPolicy {
        max_records: 16,
        max_bytes: 4096,
    });
    assert_eq!(batch.capacity(), capacity);
    assert!(batch.allocated_bytes() <= 4096);
    batch.clear_with_policy(BufferPolicy::minimal());
    assert_eq!(batch.allocated_bytes(), 0);
    Ok(())
}

#[test]
fn integration_scenario_serde_scratch_byte_policy_ignores_record_limit_and_cleans_errors()
-> Result<()> {
    let mut scratch = Vec::with_capacity(128 * 1024);
    let mut batch = WireBatch::new();
    SerdeBridge::encode_batch_into_with_policy(
        "topic",
        &["value"],
        &mut batch,
        &mut scratch,
        BufferPolicy {
            max_records: 0,
            max_bytes: 256 * 1024,
        },
    )?;
    assert!(scratch.is_empty());
    assert_eq!(scratch.capacity(), 128 * 1024);
    let error = SerdeBridge::encode_batch_into_using_with_policy(
        "topic",
        &[()],
        &mut batch,
        &mut scratch,
        BufferPolicy {
            max_records: 100,
            max_bytes: 32,
        },
        |_, scratch| {
            scratch.resize(1024, 0);
            Err(Error::Codec("rejected".into()))
        },
    );
    assert!(error.is_err());
    assert!(scratch.is_empty());
    assert!(scratch.capacity() <= 32);
    assert_eq!(batch.len(), 1);
    Ok(())
}

#[test]
fn unit_lifecycle_tuple_and_map_values_release_nested_allocations() {
    let mut tree = std::collections::BTreeMap::new();
    tree.insert("key".to_owned(), "value".repeat(1024));
    let mut hash = std::collections::HashMap::new();
    hash.insert("key".to_owned(), vec![1u8; 4096]);
    let mut value = (tree, hash, "text".repeat(1024));
    assert!(value.extra_bytes() > 8192);
    value.clear_value(BufferPolicy::minimal().retention(1, value.extra_bytes(), 0));
    assert_eq!(value.extra_bytes(), 0);
    assert!(value.0.is_empty());
    assert!(value.1.is_empty());
}

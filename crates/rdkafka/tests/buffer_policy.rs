//! Storage budgets, nested allocation retention, and scratch cleanup.
use rdkafka::{
    BufferPolicy, Error, Result, SerdeBridge, WireBatch, WireHeader, WireRecord,
    buffer_policy::MemoryBuffer,
    column::{ColumnValue, ReusableColumn},
};

#[test]
fn integration_scenario_aggregate_budget_reclaims_burst_and_reuses_warm_arenas() {
    let payload = vec![7; 4096];
    let headers = [WireHeader {
        name: "trace",
        value: Some(&payload),
    }];
    let mut batch = WireBatch::with_capacity(128);
    for _ in 0..128 {
        batch
            .push(
                WireRecord::new("orders", Some(&payload))
                    .with_key(Some(&payload))
                    .with_headers(&headers),
            )
            .unwrap();
    }
    batch.clear_with_policy(BufferPolicy {
        max_records: 32,
        max_bytes: 32 * 1024,
    });
    assert!(batch.is_empty());
    assert!(batch.capacity() <= 32);
    assert!(batch.allocated_bytes() <= 32 * 1024);
    let retained = batch.allocated_bytes();
    batch
        .push(WireRecord::new("orders", Some(b"small")))
        .unwrap();
    batch.clear_unbounded();
    assert_eq!(batch.allocated_bytes(), retained);
    batch.clear_with_policy(BufferPolicy::minimal());
    assert_eq!(batch.allocated_bytes(), 0);
}

#[test]
fn unit_scenario_default_budget_bounds_record_descriptors() {
    let mut batch = WireBatch::with_capacity(70_000);
    batch.clear();
    assert!(batch.capacity() <= 65_536);
    assert!(batch.allocated_bytes() <= 8 * 1024 * 1024);
}

#[test]
fn unit_lifecycle_scenario_owned_cow_burst_counts_and_trims_nested_capacity() {
    use std::borrow::Cow;

    use rdkafka::{
        buffer_policy::MemoryBuffer,
        column::{ColumnValue, ReusableColumn},
    };
    let borrowed: Cow<'_, str> = Cow::Borrowed("borrowed");
    assert_eq!(borrowed.extra_bytes(), 0);
    let owned: Cow<'static, str> = Cow::Owned("x".repeat(4096));
    let mut column = ReusableColumn::with_capacity(32);
    for _ in 0..32 {
        column.push(&owned);
    }
    assert!(column.allocated_bytes() >= 32 * 4096);
    let retention = BufferPolicy {
        max_records: 8,
        max_bytes: 4096,
    }
    .retention(
        column.record_capacity(),
        column.allocated_bytes(),
        column.inline_bytes(),
    );
    column.clear_retained(retention);
    assert!(column.is_empty());
    assert!(column.allocated_bytes() <= 4096);
    assert!(column.record_capacity() <= 8);
    let bytes: Cow<'static, [u8]> = Cow::Owned(vec![0; 128]);
    assert_eq!(bytes.extra_bytes(), 128);
}

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

#[test]
fn unit_scenario_plain_vector_retention_clears_values_before_shrinking() {
    let mut values = vec![1u8; 4096];
    let retention = BufferPolicy {
        max_records: 1,
        max_bytes: 128,
    }
    .retention(0, values.extra_bytes(), 0);
    values.clear_value(retention);
    assert!(values.is_empty());
    assert!(values.capacity() <= 128);
}

#[test]
fn unit_scenario_nested_vector_retention_reuses_remaining_payload_budget() {
    let mut values = vec!["x".repeat(1000); 4];
    let bytes = values.extra_bytes();
    let budget = bytes / 2;
    let retention = BufferPolicy {
        max_records: 1,
        max_bytes: budget,
    }
    .retention(0, bytes, 0);
    values.clear_value(retention);
    assert_eq!(values.len(), 2);
    assert!(values.iter().all(String::is_empty));
    assert!(values.extra_bytes() <= budget);
    assert!(values.iter().map(String::capacity).sum::<usize>() > 1500);
}

#[test]
fn unit_lifecycle_hash_map_clear_retains_budgeted_slots() {
    let mut values = std::collections::HashMap::with_capacity(128);
    values.insert(1u32, 2u32);
    let capacity = values.capacity();
    values.clear_value(BufferPolicy::unbounded().retention(0, values.extra_bytes(), 0));
    assert!(values.is_empty());
    assert_eq!(values.capacity(), capacity);
    values.clear_value(BufferPolicy::minimal().retention(0, values.extra_bytes(), 0));
    assert_eq!(values.capacity(), 0);
}

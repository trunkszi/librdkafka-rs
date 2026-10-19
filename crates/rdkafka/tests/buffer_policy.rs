use rdkafka::{BufferPolicy, WireBatch, WireHeader, WireRecord};

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
    .retention(column.record_capacity(), column.allocated_bytes());
    column.clear_retained(retention);
    assert!(column.is_empty());
    assert!(column.allocated_bytes() <= 4096);
    assert!(column.record_capacity() <= 8);
    let bytes: Cow<'static, [u8]> = Cow::Owned(vec![0; 128]);
    assert_eq!(bytes.extra_bytes(), 128);
}

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

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct PanicClone {
    value: u32,
    panic_on_clone: bool,
}

impl Clone for PanicClone {
    fn clone(&self) -> Self {
        assert!(!self.panic_on_clone, "field clone rejected");
        Self {
            value: self.value,
            panic_on_clone: false,
        }
    }
}

impl ColumnValue for PanicClone {}

#[derive(rdkafka::Record)]
struct TransactionalRecord {
    label: String,
    late: PanicClone,
}

fn transactional_record(value: u32, panic_on_clone: bool) -> TransactionalRecord {
    TransactionalRecord {
        label: format!("record-{value}"),
        late: PanicClone {
            value,
            panic_on_clone,
        },
    }
}

fn transactional_batch(warm: bool) -> TransactionalRecordBatch {
    let mut batch = TransactionalRecordBatch::with_capacity(2);
    if warm {
        batch.push(&transactional_record(8, false));
        batch.push(&transactional_record(9, false));
        batch.clear_unbounded();
    }
    batch.push(&transactional_record(1, false));
    batch
}

fn assert_transactional_batch(batch: &TransactionalRecordBatch) -> Result<()> {
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.column_label(), &["record-1"]);
    assert_eq!(batch.column_late().len(), 1);
    assert_eq!(batch.column_late()[0].value, 1);
    let wire = batch.to_wire_batch()?;
    assert_eq!(wire.len(), 1);
    let restored = TransactionalRecordBatch::from_wire_batch(&wire)?;
    assert_eq!(restored.column_label(), batch.column_label());
    assert_eq!(restored.column_late(), batch.column_late());
    Ok(())
}

#[test]
fn integration_scenario_owned_batch_append_rolls_back_late_clone_panic() -> Result<()> {
    for warm in [false, true] {
        let mut batch = transactional_batch(warm);
        let rejected = transactional_record(2, true);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            batch.push(&rejected);
        }));
        assert!(panic.is_err());
        assert_transactional_batch(&batch)?;
        batch.push(&transactional_record(3, false));
        assert_eq!(batch.column_label(), &["record-1", "record-3"]);
        assert_eq!(batch.column_late()[1].value, 3);
        assert_eq!(batch.to_wire_batch()?.len(), 2);
    }
    Ok(())
}

#[test]
fn integration_scenario_borrowed_batch_append_rolls_back_late_clone_panic() -> Result<()> {
    use rdkafka::Record;

    let mut wire = WireBatch::new();
    transactional_record(2, true).encode_into(&mut wire)?;
    for warm in [false, true] {
        let mut batch = transactional_batch(warm);
        let view = TransactionalRecord::view(wire.iter().next().unwrap())?;
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            batch.push_ref(view).unwrap();
        }));
        assert!(panic.is_err());
        assert_transactional_batch(&batch)?;
        batch.push(&transactional_record(3, false));
        assert_eq!(batch.column_label(), &["record-1", "record-3"]);
        assert_eq!(batch.column_late()[1].value, 3);
        assert_eq!(batch.to_wire_batch()?.len(), 2);
    }
    Ok(())
}

#[test]
fn integration_scenario_batch_rejects_unequal_columns_before_append_or_encoding() -> Result<()> {
    use rdkafka::Record;

    let mut batch = transactional_batch(false);
    batch.late.truncate(0);
    assert!(matches!(batch.to_wire_batch(), Err(Error::Layout(_))));
    let record = transactional_record(2, false);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        batch.push(&record);
    }));
    assert!(panic.is_err());
    assert_eq!(batch.column_label(), &["record-1"]);
    assert!(batch.column_late().is_empty());
    let mut wire = WireBatch::new();
    record.encode_into(&mut wire)?;
    let view = TransactionalRecord::view(wire.iter().next().unwrap())?;
    assert!(matches!(batch.push_ref(view), Err(Error::Layout(_))));
    assert_eq!(batch.column_label(), &["record-1"]);
    assert!(batch.column_late().is_empty());
    Ok(())
}

#[derive(rdkafka::Record)]
struct ByteArrayRecord {
    #[record(bytes)]
    bytes: [u8; 4],
    #[record(bytes)]
    optional: Option<[u8; 4]>,
    #[record(key, bytes)]
    key: [u8; 4],
    #[record(header = "optional", bytes)]
    header: Option<[u8; 4]>,
}

#[test]
fn integration_scenario_byte_arrays_round_trip_payload_and_metadata() -> Result<()> {
    use rdkafka::Record;
    for optional in [None, Some([5, 6, 7, 8])] {
        let record = ByteArrayRecord {
            bytes: [1, 2, 3, 4],
            optional,
            key: [9, 10, 11, 12],
            header: optional,
        };
        let mut wire = WireBatch::new();
        record.encode_into(&mut wire)?;
        let view = ByteArrayRecord::view(wire.get(0).unwrap())?;
        assert_eq!(view.bytes, record.bytes);
        assert_eq!(view.optional, optional);
        assert_eq!(view.key, record.key);
        assert_eq!(view.header, optional);
        let mut batch = ByteArrayRecordBatch::default();
        batch.push_ref(view)?;
        let wire = batch.to_wire_batch()?;
        let restored = ByteArrayRecord::decode(wire.get(0).unwrap())?;
        assert_eq!(restored.bytes, record.bytes);
        assert_eq!(restored.optional, optional);
        assert_eq!(restored.key, record.key);
        assert_eq!(restored.header, optional);
    }
    Ok(())
}

#[derive(rdkafka::Record)]
struct MetadataOnlyRecord {
    #[record(key)]
    key: String,
    #[record(timestamp)]
    timestamp: i64,
}

#[test]
fn integration_scenario_metadata_only_records_keep_payload_absent() -> Result<()> {
    use rdkafka::Record;
    let record = MetadataOnlyRecord {
        key: "key".into(),
        timestamp: 42,
    };
    let mut batch = MetadataOnlyRecordBatch::default();
    batch.push(&record);
    let wire = batch.to_wire_batch()?;
    let frame = wire.get(0).unwrap();
    assert!(frame.payload.is_none());
    let restored = MetadataOnlyRecord::decode(frame)?;
    assert_eq!(restored.key, record.key);
    assert_eq!(restored.timestamp, record.timestamp);
    Ok(())
}

#[derive(
    Clone,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(transparent)]
struct OnlyPod([u8; 4]);
impl ColumnValue for OnlyPod {}

#[derive(
    Clone,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
    rdkafka::Record,
)]
#[record(codec = "zerocopy")]
#[repr(transparent)]
struct GenericPod<T> {
    value: T,
}

#[test]
fn integration_scenario_generic_zerocopy_records_require_no_serde_codec() -> Result<()> {
    use rdkafka::Record;
    let record = GenericPod {
        value: OnlyPod([1, 2, 3, 4]),
    };
    let mut batch = GenericPodBatch::default();
    batch.push(&record);
    let wire = batch.to_wire_batch()?;
    assert_eq!(wire.get(0).unwrap().payload, Some(&[1, 2, 3, 4][..]));
    let restored = GenericPod::<OnlyPod>::decode(wire.get(0).unwrap())?;
    assert_eq!(restored.value.0, record.value.0);
    Ok(())
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

struct ReplacedSequenceCursor;

impl<'de> rdkafka::serde_bridge::BorrowDecode<'de> for ReplacedSequenceCursor {
    fn borrow_decode(decoder: &mut rdkafka::serde_bridge::Deserializer<'de>) -> Result<Self> {
        *decoder = rdkafka::serde_bridge::Deserializer::new(&[0; 64]);
        Ok(Self)
    }
}

#[test]
fn unit_scenario_sequence_rejects_replaced_cursor_without_advancing_caller() -> Result<()> {
    use rdkafka::serde_bridge::{Deserializer, EncodedSequence};
    let mut decoder = Deserializer::new(&[1, 7]);
    let result = EncodedSequence::<ReplacedSequenceCursor>::decode(&mut decoder);
    assert!(
        matches!(result, Err(Error::Codec(message)) if message.contains("replaced the input cursor"))
    );
    let first = <u8 as serde::Deserialize>::deserialize(&mut decoder)?;
    assert_eq!(first, 1);
    Ok(())
}

//! Typed schema, metadata, borrowed views, and column conversion boundaries.
use rdkafka::{Record, WireBatch};

#[derive(Debug, PartialEq, Record)]
#[serde(rename_all = "camelCase")]
#[record(rename_all = "PascalCase")]
#[kafka(rename_all = "SCREAMING-KEBAB-CASE")]
struct Event {
    #[record(key)]
    id: String,
    #[record(timestamp)]
    at: i64,
    #[record(partition)]
    shard: i32,
    #[record(header = "old")]
    #[kafka(header = "trace")]
    trace: String,
    #[serde(rename = "serdeName")]
    #[record(rename = "recordName")]
    #[kafka(rename = "finalName")]
    event_name: String,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
    note: Option<String>,
}

#[test]
fn integration_scenario_metadata_and_borrowed_columns_round_trip() {
    let event = Event {
        id: "key".into(),
        at: 1234,
        shard: 2,
        trace: "trace-value".into(),
        event_name: "created".into(),
        data: vec![0, 255, 3],
        note: Some("memo".into()),
    };
    let mut wire = WireBatch::new();
    event.encode_into(&mut wire).unwrap();
    let frame = wire.get(0).unwrap();
    assert_eq!(frame.partition, 2);
    assert_eq!(frame.timestamp, Some(1234));
    assert_eq!(frame.headers.get(0).unwrap().name, "trace");
    assert_eq!(
        frame.payload.unwrap(),
        &[
            7, b'c', b'r', b'e', b'a', b't', b'e', b'd', 3, 0, 255, 3, 1, 4, b'm', b'e', b'm', b'o'
        ]
    );
    let view = Event::view(frame).unwrap();
    let payload = frame.payload.unwrap();
    assert!(view.event_name.as_ptr() >= payload.as_ptr());
    assert!(view.event_name.as_ptr() < payload.as_ptr().wrapping_add(payload.len()));
    assert!(view.data.as_ptr() >= payload.as_ptr());
    assert!(view.data.as_ptr() < payload.as_ptr().wrapping_add(payload.len()));
    assert_eq!(view.to_owned().unwrap(), event);
    let mut batch = EventBatch::with_capacity(2);
    batch.push_ref(view).unwrap();
    batch.push(&event);
    assert_eq!(batch.column_event_name(), &["created", "created"]);
    assert_eq!(batch.column_data(), &[vec![0, 255, 3], vec![0, 255, 3]]);
    assert_eq!(
        EventBatch::COLUMN_NAMES,
        &["ID", "AT", "SHARD", "TRACE", "finalName", "DATA", "NOTE"]
    );
    let encoded = batch.to_wire_batch().unwrap();
    let decoded = EventBatch::from_wire_batch(&encoded).unwrap();
    assert_eq!(decoded.column_id(), &["key", "key"]);
    assert_eq!(Event::decode(encoded.get(1).unwrap()).unwrap(), event);
    batch.clear_unbounded();
    assert!(batch.is_empty());
    assert!(batch.allocated_bytes() > 0);
    batch.clear_with_policy(rdkafka::BufferPolicy::minimal());
    assert_eq!(batch.allocated_bytes(), 0);
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Record,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(C)]
#[record(codec = "zerocopy")]
struct Fixed {
    bytes: [u8; 4],
}
#[test]
fn integration_scenario_direct_layout_rejects_wrong_size() {
    let fixed = Fixed {
        bytes: [1, 2, 3, 4],
    };
    let mut wire = WireBatch::new();
    fixed.encode_into(&mut wire).unwrap();
    assert_eq!(wire.get(0).unwrap().payload.unwrap(), &[1, 2, 3, 4]);
    assert_eq!(
        Fixed::view(wire.get(0).unwrap()).unwrap().bytes,
        [1, 2, 3, 4]
    );
    assert_eq!(Fixed::decode(wire.get(0).unwrap()).unwrap(), fixed);
    assert!(Fixed::view(rdkafka::WireRecord::new("", Some(&[1, 2, 3]))).is_err());
}

#[derive(Debug, PartialEq, Record)]
struct Nullable {
    #[record(key)]
    key: Option<Vec<u8>>,
    #[record(header = "optional")]
    header: Option<String>,
    #[record(timestamp)]
    timestamp: Option<i64>,
    payload: String,
}
#[test]
fn integration_scenario_nullable_metadata_preserves_null_and_empty() {
    for record in [
        Nullable {
            key: None,
            header: None,
            timestamp: None,
            payload: "a".into(),
        },
        Nullable {
            key: Some(vec![]),
            header: Some(String::new()),
            timestamp: Some(0),
            payload: "b".into(),
        },
    ] {
        let mut wire = WireBatch::new();
        record.encode_into(&mut wire).unwrap();
        let frame = wire.get(0).unwrap();
        assert_eq!(frame.key, record.key.as_deref());
        assert_eq!(
            frame.headers.get(0).unwrap().value,
            record.header.as_ref().map(|v| v.as_bytes())
        );
        assert_eq!(Nullable::decode(frame).unwrap(), record);
    }
}

#[derive(Debug, PartialEq, Record)]
struct Nested {
    maybe: Option<Vec<String>>,
    nested: Vec<Option<Vec<String>>>,
    text: std::borrow::Cow<'static, str>,
    bytes: std::borrow::Cow<'static, [u8]>,
}
#[test]
fn integration_scenario_nested_sequences_and_borrowed_cow_inputs() {
    let record = Nested {
        maybe: Some(vec!["one".into(), "two".into()]),
        nested: vec![None, Some(vec!["three".into()])],
        text: std::borrow::Cow::Borrowed("text"),
        bytes: std::borrow::Cow::Borrowed(&[1, 2, 3]),
    };
    let mut wire = WireBatch::new();
    record.encode_into(&mut wire).unwrap();
    let view = Nested::view(wire.get(0).unwrap()).unwrap();
    assert_eq!(view.text, "text");
    assert_eq!(view.bytes, &[1, 2, 3]);
    assert_eq!(
        view.maybe
            .as_ref()
            .unwrap()
            .iter()
            .collect::<rdkafka::Result<Vec<_>>>()
            .unwrap(),
        vec!["one", "two"]
    );
    assert_eq!(view.to_owned().unwrap(), record);
    let mut batch = NestedBatch::default();
    batch.push_ref(view).unwrap();
    let encoded = batch.to_wire_batch().unwrap();
    assert_eq!(Nested::decode(encoded.get(0).unwrap()).unwrap(), record);
}

#[derive(Debug, PartialEq, Record)]
struct CollectionMetadata {
    #[record(key)]
    ids: Vec<u64>,
    #[record(header = "labels")]
    labels: Option<Vec<String>>,
    #[serde(with = "serde_bytes")]
    bytes: Option<Vec<u8>>,
}
#[test]
fn integration_scenario_collection_metadata_and_optional_bytes() {
    let record = CollectionMetadata {
        ids: vec![7, 9],
        labels: Some(vec!["a".into()]),
        bytes: Some(vec![0, 255]),
    };
    let mut wire = WireBatch::new();
    record.encode_into(&mut wire).unwrap();
    let view = CollectionMetadata::view(wire.get(0).unwrap()).unwrap();
    assert_eq!(
        view.ids
            .iter()
            .collect::<rdkafka::Result<Vec<_>>>()
            .unwrap(),
        vec![7, 9]
    );
    assert_eq!(view.bytes, Some(&[0, 255][..]));
    assert_eq!(view.to_owned().unwrap(), record);
    let mut batch = CollectionMetadataBatch::default();
    batch.push_ref(view).unwrap();
    assert_eq!(batch.column_ids(), &[vec![7, 9]]);
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Record)]
struct Detail {
    label: String,
    bytes: Vec<u8>,
}
#[derive(Debug, PartialEq, Record)]
struct CompositeColumns {
    detail: Detail,
    tuple: (String, Vec<u8>),
    labels: std::collections::BTreeMap<String, String>,
}
#[test]
fn integration_scenario_nested_record_tuple_and_map_columns_account_allocations() {
    let record = CompositeColumns {
        detail: Detail {
            label: "nested".repeat(32),
            bytes: vec![7; 128],
        },
        tuple: ("tuple".repeat(16), vec![8; 64]),
        labels: [("key".into(), "value".repeat(32))].into_iter().collect(),
    };
    let mut batch = CompositeColumnsBatch::with_capacity(1);
    batch.push(&record);
    let extra = rdkafka::column::ColumnValue::extra_bytes(&record);
    assert!(
        extra
            >= record.detail.label.capacity()
                + record.detail.bytes.capacity()
                + record.tuple.0.capacity()
                + record.tuple.1.capacity()
                + record.labels.values().map(String::capacity).sum::<usize>()
    );
    assert!(batch.allocated_bytes() >= extra);
    let wire = batch.to_wire_batch().unwrap();
    let decoded = <CompositeColumnsBatch as rdkafka::Batch>::from_wire_batch(&wire).unwrap();
    assert_eq!(
        decoded.column_detail(),
        std::slice::from_ref(&record.detail)
    );
    assert_eq!(decoded.column_tuple(), std::slice::from_ref(&record.tuple));
    assert_eq!(
        decoded.column_labels(),
        std::slice::from_ref(&record.labels)
    );
    batch.clear_with_policy(rdkafka::BufferPolicy::minimal());
    assert_eq!(batch.allocated_bytes(), 0);
}

#[derive(Debug, PartialEq, Record)]
struct OptionalCompactMetadata {
    #[record(key)]
    number: Option<u64>,
    #[record(header = "labels")]
    labels: Option<Vec<String>>,
    value: u8,
}
#[test]
fn integration_scenario_optional_compact_metadata_uses_wire_null_and_inner_encoding() {
    for record in [
        OptionalCompactMetadata {
            number: None,
            labels: None,
            value: 1,
        },
        OptionalCompactMetadata {
            number: Some(9),
            labels: Some(vec!["a".into()]),
            value: 2,
        },
    ] {
        let mut wire = WireBatch::new();
        record.encode_into(&mut wire).unwrap();
        let frame = wire.get(0).unwrap();
        let key_bytes = 9u64.to_le_bytes();
        assert_eq!(frame.key, record.number.map(|_| key_bytes.as_slice()));
        assert_eq!(
            frame.headers.get(0).unwrap().value,
            record.labels.as_ref().map(|_| &[1, 1, b'a'][..])
        );
        let view = OptionalCompactMetadata::view(frame).unwrap();
        assert_eq!(view.to_owned().unwrap(), record);
        assert_eq!(OptionalCompactMetadata::decode(frame).unwrap(), record);
    }
}

#[derive(
    Debug,
    PartialEq,
    Record,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(transparent)]
#[record(codec = "zerocopy")]
struct CloneSensitiveBytes {
    bytes: [u8; 4],
}
impl Clone for CloneSensitiveBytes {
    fn clone(&self) -> Self {
        Self { bytes: [0; 4] }
    }
}
#[derive(
    Record,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(transparent)]
#[record(codec = "zerocopy")]
struct DirectOwnedLayout {
    value: CloneSensitiveBytes,
}
#[test]
fn integration_scenario_zerocopy_owned_decode_reads_layout_without_field_clone() {
    let value = DirectOwnedLayout {
        value: CloneSensitiveBytes {
            bytes: [1, 2, 3, 4],
        },
    };
    let mut wire = WireBatch::new();
    value.encode_into(&mut wire).unwrap();
    let decoded = DirectOwnedLayout::decode(wire.get(0).unwrap()).unwrap();
    assert_eq!(decoded.value.bytes, [1, 2, 3, 4]);
}

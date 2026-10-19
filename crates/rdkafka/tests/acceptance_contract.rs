//! Typed column and serde interoperability through the public payload boundary.
use rdkafka::{BufferPolicy, Record, RecordView, SerdeBridge, WireBatch, WireRecord};
use serde::{Deserialize, Serialize};
use static_assertions::assert_not_impl_any;

#[derive(Debug, PartialEq, Record, Serialize, Deserialize)]
struct Payload {
    label: String,
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
    counts: Vec<u64>,
    note: Option<String>,
}

assert_not_impl_any!(WireBatch: Send, Sync);
assert_not_impl_any!(WireRecord<'static>: Send, Sync);
assert_not_impl_any!(PayloadBatch: Send, Sync);
assert_not_impl_any!(PayloadRecordRef<'static>: Send, Sync);

#[test]
fn integration_scenario_typed_payload_is_serde_interoperable() {
    let record = Payload {
        label: "opaque schema".into(),
        bytes: vec![0, 128, 255],
        counts: vec![0, u64::MAX],
        note: Some("borrowed".into()),
    };
    let mut wire = WireBatch::new();
    record.encode_into(&mut wire).unwrap();
    let frame = wire.get(0).unwrap();
    let decoded: Payload = SerdeBridge::decode_record_borrowed(frame).unwrap();
    assert_eq!(decoded, record);
    let mut encoded = Vec::new();
    SerdeBridge::encode_into(&record, &mut encoded).unwrap();
    assert_eq!(frame.payload.unwrap(), encoded);
    let projection = Payload::view(frame).unwrap();
    let mut columns = PayloadBatch::with_capacity(1);
    columns.push_ref(projection).unwrap();
    assert_eq!(columns.column_counts(), &[vec![0, u64::MAX]]);
    let restored = columns.to_wire_batch().unwrap();
    assert_eq!(Payload::decode(restored.get(0).unwrap()).unwrap(), record);
    columns.clear_with_policy(BufferPolicy::minimal());
    assert_eq!(columns.allocated_bytes(), 0);
}

#[test]
fn integration_scenario_trailing_or_missing_payload_is_rejected() {
    let record = Payload {
        label: String::new(),
        bytes: vec![],
        counts: vec![],
        note: None,
    };
    let mut bytes = Vec::new();
    SerdeBridge::encode_into(&record, &mut bytes).unwrap();
    bytes.push(0);
    assert!(Payload::view(WireRecord::new("", Some(&bytes))).is_err());
    assert!(Payload::view(WireRecord::new("", None)).is_err());
    bytes.pop();
    let view = Payload::view(WireRecord::new("", Some(&bytes))).unwrap();
    assert_eq!(RecordView::to_owned(&view).unwrap(), record);
}

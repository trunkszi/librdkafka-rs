//! Packed descriptors preserve nullable and signed timestamp identity.
use rdkafka::{Result, WireBatch, WireRecord};

#[test]
fn integration_wire_presence_roundtrip() -> Result<()> {
    let mut batch = WireBatch::new();
    let mut expected = Vec::new();
    for key in [None, Some(&b""[..]), Some(&b"key"[..])] {
        for payload in [None, Some(&b""[..]), Some(&b"payload"[..])] {
            for timestamp in [None, Some(-1), Some(0), Some(i64::MAX)] {
                batch.push(
                    WireRecord::new("topic", payload)
                        .with_key(key)
                        .with_timestamp(timestamp),
                )?;
                expected.push((key, payload, timestamp));
            }
        }
    }
    for (actual, expected) in batch.iter().zip(expected) {
        assert_eq!((actual.key, actual.payload, actual.timestamp), expected);
    }
    batch.clear_unbounded();
    batch.push(WireRecord::new("topic", None))?;
    let record = batch.get(0).expect("replacement record");
    assert_eq!(
        (record.key, record.payload, record.timestamp),
        (None, None, None)
    );
    Ok(())
}

//! Codec unwinding must not accumulate invisible arena records.
use std::panic::{AssertUnwindSafe, catch_unwind};

use rdkafka::{Result, WireBatch};

fn fill(batch: &mut WireBatch, fail: bool) -> Result<()> {
    batch.push_with("topic", 0, 0, None, |writer| {
        writer.key(Some(b"key"))?;
        writer.payload(Some(b"payload"))?;
        writer.header("trace", Some(b"value"))?;
        assert!(!fail, "codec panic");
        Ok(())
    })
}
#[test]
fn codec_unwind_preserves_warmed_arena_capacity() -> Result<()> {
    let mut batch = WireBatch::with_capacity(1);
    fill(&mut batch, false)?;
    batch.clear_unbounded();
    let capacity = batch.allocated_bytes();
    for _ in 0..32 {
        assert!(catch_unwind(AssertUnwindSafe(|| fill(&mut batch, true))).is_err());
        assert!(batch.is_empty());
    }
    assert_eq!(
        batch.allocated_bytes(),
        capacity,
        "panicked records leaked arena contents"
    );
    fill(&mut batch, false)?;
    let record = batch.get(0).expect("replacement record");
    assert_eq!(record.topic, "topic");
    assert_eq!(record.payload, Some(b"payload".as_slice()));
    assert_eq!(record.headers.len(), 1);
    Ok(())
}

#[test]
fn unicode_names_survive_checked_projection() -> Result<()> {
    use rdkafka::{WireHeader, WireRecord};
    let headers = [WireHeader {
        name: "key-\u{03bb}",
        value: Some(b"value"),
    }];
    let mut batch = WireBatch::new();
    batch.push(WireRecord::new("topic-\u{03bb}", None).with_headers(&headers))?;
    let record = batch.get(0).expect("Unicode record");
    assert_eq!(record.topic, "topic-\u{03bb}");
    assert_eq!(record.headers.get(0), Some(headers[0]));
    assert!(record.headers.get(1).is_none());
    Ok(())
}

#[test]
fn corrupt_header_prefix_is_not_unchecked() -> Result<()> {
    let mut batch = WireBatch::new();
    batch.push_with("topic", 0, 0, None, |writer| {
        writer.encode_header("name", |bytes| {
            bytes[0] = 0xff;
            bytes.push(1);
            Ok(())
        })
    })?;
    assert!(batch.get(0).expect("record").headers.get(0).is_none());
    Ok(())
}

#[test]
fn codec_panic_preserves_committed_record() -> Result<()> {
    use rdkafka::WireRecord;
    let mut batch = WireBatch::with_capacity(2);
    batch.push(WireRecord::new("kept", Some(b"committed")))?;
    assert!(catch_unwind(AssertUnwindSafe(|| fill(&mut batch, true))).is_err());
    assert_eq!(batch.len(), 1);
    let kept = batch.get(0).expect("committed record");
    assert_eq!(kept.topic, "kept");
    assert_eq!(kept.payload, Some(b"committed".as_slice()));
    assert!(kept.headers.is_empty());
    fill(&mut batch, false)?;
    assert_eq!(batch.len(), 2);
    let next = batch.get(1).expect("replacement record");
    assert_eq!(next.topic, "topic");
    assert_eq!(next.headers.len(), 1);
    Ok(())
}

#[test]
fn header_failure_restores_prior_bytes() -> Result<()> {
    use rdkafka::{WireHeader, WireRecord};
    let mut batch = WireBatch::new();
    batch.push(WireRecord::new("kept", Some(b"body")).with_headers(&[
        WireHeader {
            name: "h0",
            value: Some(b"aaa"),
        },
        WireHeader {
            name: "h1",
            value: Some(b"bbb"),
        },
    ]))?;
    let error = batch.push_with("next", 1, 9, None, |writer| {
        writer.header("n0", Some(b"xxx"))?;
        writer.header("n1", Some(b"yyy"))?;
        writer.encode_header("n2", |out| {
            out.extend_from_slice(b"partial");
            Err(rdkafka::Error::Codec("stop".into()))
        })?;
        Ok(())
    });
    assert!(error.is_err());
    assert_eq!(batch.len(), 1);
    let kept = batch.get(0).expect("prior record");
    assert_eq!(kept.topic, "kept");
    assert_eq!(kept.payload, Some(b"body".as_slice()));
    assert_eq!(kept.headers.len(), 2);
    assert_eq!(kept.headers.get(0).expect("first header").name, "h0");
    assert_eq!(
        kept.headers.get(0).expect("first header").value,
        Some(b"aaa".as_slice())
    );
    assert_eq!(kept.headers.get(1).expect("second header").name, "h1");
    assert_eq!(
        kept.headers.get(1).expect("second header").value,
        Some(b"bbb".as_slice())
    );
    Ok(())
}

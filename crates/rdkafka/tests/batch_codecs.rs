//! Caller-owned codec buffers and native batch decoding scenarios.
use std::time::{Duration, Instant};

use rdkafka::*;

fn encode_number(value: &u64, output: &mut Vec<u8>) -> Result<()> {
    output.extend_from_slice(value.to_string().as_bytes());
    Ok(())
}
fn decode_number(payload: &[u8]) -> Result<u64> {
    std::str::from_utf8(payload)
        .map_err(|error| Error::Codec(error.to_string()))?
        .parse::<u64>()
        .map_err(|error| Error::Codec(error.to_string()))
}
fn decode_text(payload: &[u8]) -> Result<&str> {
    std::str::from_utf8(payload).map_err(|error| Error::Codec(error.to_string()))
}
fn consumer(brokers: &str, topic: &str) -> Result<Consumer> {
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", brokers)?
        .set("group.id", topic)?
        .set("enable.auto.commit", "false")?;
    let consumer = Consumer::new(&conf)?;
    consumer.assign(&[TopicPartition::new(topic, 0, 0)])?;
    Ok(consumer)
}
fn until_record(mut receive: impl FnMut() -> Result<usize>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    while receive()? == 0 {
        assert!(Instant::now() < deadline, "native codec receive deadline");
    }
    Ok(())
}
#[test]
fn integration_scenario_custom_codecs_reuse_bounded_scratch() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?;
    conf.partitioner_cb(|_, _, _| 0);
    let producer = Producer::new(&conf)?;
    let topic = format!("custom-codec-{}", std::process::id());
    let mut batch = WireBatch::with_capacity(4096);
    let mut scratch = Vec::with_capacity(16384);
    let policy = BufferPolicy {
        max_records: 2,
        max_bytes: 64,
    };
    assert_eq!(
        producer
            .produce_batch_using_with_policy(
                &topic,
                &[7_u64],
                &mut batch,
                &mut scratch,
                policy,
                encode_number,
            )?
            .accepted(),
        1
    );
    assert!(scratch.is_empty());
    assert!(scratch.capacity() <= policy.max_bytes);
    assert_eq!(batch.get(0).unwrap().payload, Some(&b"7"[..]));
    // An empty cycle bounds all retained wire storage without enqueuing records.
    assert!(
        producer
            .produce_batch_using_with_policy(
                &topic,
                &[] as &[u64],
                &mut batch,
                &mut scratch,
                policy,
                encode_number,
            )?
            .all_accepted()
    );
    assert!(batch.is_empty());
    assert!(batch.capacity() <= policy.max_records);
    assert!(batch.allocated_bytes() <= policy.max_bytes);
    producer.flush(10000)?;
    let before = producer.outq_len();
    let mut calls = 0;
    let error = producer.produce_batch_using_with_policy(
        &topic,
        &[8_u64, 9],
        &mut batch,
        &mut scratch,
        BufferPolicy::minimal(),
        |value, out| {
            calls += 1;
            out.resize(8192, 0);
            if calls == 2 {
                return Err(Error::Codec("rejected codec value".into()));
            }
            encode_number(value, out)
        },
    );
    assert!(matches!(error, Err(Error::Codec(message)) if message == "rejected codec value"));
    assert_eq!(
        producer.outq_len(),
        before,
        "encoding must finish before enqueue"
    );
    assert!(scratch.is_empty());
    assert_eq!(scratch.capacity(), 0);
    assert_eq!(
        batch.len(),
        1,
        "retain successfully encoded prefix for inspection"
    );
    let brokers = producer.mock_bootstraps();
    for mode in 0..3 {
        let receiver = consumer(&brokers, &topic)?;
        let mut values = vec![];
        until_record(|| match mode {
            0 => receiver.consume_with_using(1, 200, decode_number, |value| {
                values.push(value);
                Ok(())
            }),
            1 => receiver.consume_into_using(1, 200, &mut values, decode_number),
            _ => {
                let mut slots = [99_u64, 100];
                let n = receiver.consume_into_slice_using(1, 200, &mut slots, decode_number)?;
                if n > 0 {
                    assert_eq!(slots, [7, 100]);
                    values.push(slots[0]);
                }
                Ok(n)
            }
        })?;
        assert_eq!(values, [7]);
        receiver.close()?;
    }
    let receiver = consumer(&brokers, &topic)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let received = loop {
        let batch = receiver.consume_batch(1, 200)?;
        if !batch.is_empty() {
            break batch;
        }
        assert!(Instant::now() < deadline, "retained codec receive deadline");
    };
    let mut borrowed = vec![];
    received.decode_into_using(&mut borrowed, decode_text)?;
    let native_payload = received.get(0)?.unwrap().payload.unwrap();
    assert_eq!(borrowed, ["7"]);
    assert_eq!(borrowed[0].as_ptr(), native_payload.as_ptr());
    let mut slots = ["tail", "retained"];
    assert_eq!(
        received.decode_into_slice_using(&mut slots, decode_text)?,
        1
    );
    assert_eq!(slots, ["7", "retained"]);
    let mut empty: [&str; 0] = [];
    let mut called = false;
    assert!(matches!(
        received.decode_into_slice_using(&mut empty, |p| {
            called = true;
            decode_text(p)
        }),
        Err(Error::BatchLengthMismatch {
            expected: 1,
            actual: 0
        })
    ));
    assert!(!called, "short slices are rejected before decoding");
    receiver.close()?;
    Ok(())
}
#[test]
fn integration_scenario_serde_batch_and_default_custom_codec() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?;
    conf.partitioner_cb(|_, _, _| 0);
    let producer = Producer::new(&conf)?;
    let topic = format!("serde-codec-{}", std::process::id());
    let mut batch = WireBatch::new();
    let mut scratch = Vec::new();
    assert!(
        producer
            .produce_serde_batch(&topic, &[17_u64], &mut batch, &mut scratch)?
            .all_accepted()
    );
    assert!(scratch.is_empty());
    producer.flush(10000)?;
    let receiver = consumer(&producer.mock_bootstraps(), &topic)?;
    let mut decoded = vec![];
    until_record(|| {
        receiver.consume_serde_with(1, 200, |value: u64| {
            decoded.push(value);
            Ok(())
        })
    })?;
    assert_eq!(decoded, [17]);
    receiver.close()?;
    assert!(
        producer
            .produce_batch_using(&topic, &[19_u64], &mut batch, &mut scratch, encode_number)?
            .all_accepted()
    );
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.get(0).unwrap().payload, Some(&b"19"[..]));
    producer.flush(10000)
}

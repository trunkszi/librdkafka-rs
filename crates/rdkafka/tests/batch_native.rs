//! Native batch receive ownership and typed record production scenarios.
use std::{
    cell::Cell,
    rc::Rc,
    time::{Duration, Instant},
};

use rdkafka::*;

static_assertions::assert_not_impl_any!(ReceivedBatch: Send, Sync);

#[derive(Debug, PartialEq, Record)]
struct Event {
    #[record(partition)]
    partition: i32,
    name: String,
    value: i64,
}
fn producer(brokers: &str) -> Result<Producer> {
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", brokers)?
        .set("message.timeout.ms", "10000")?;
    Producer::new(&conf)
}
fn consumer(brokers: &str, topic: &str) -> Result<Consumer> {
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", brokers)?
        .set("group.id", topic)?
        .set("enable.auto.commit", "false")?
        .set("auto.offset.reset", "earliest")?;
    let consumer = Consumer::new(&conf)?;
    consumer.assign(&[TopicPartition::new(topic, 0, 0)])?;
    Ok(consumer)
}
fn receive(consumer: &Consumer) -> Result<ReceivedBatch> {
    let start = Instant::now();
    loop {
        let received = consumer.consume_batch(32, 500)?;
        if !received.is_empty() {
            return Ok(received);
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "batch receive deadline"
        );
    }
}
fn wire_roundtrip(brokers: &str) -> Result<()> {
    let producer = producer(brokers)?;
    let topic = format!("wire-native-{}", std::process::id());
    let headers = [
        WireHeader {
            name: "duplicate",
            value: None,
        },
        WireHeader {
            name: "duplicate",
            value: Some(b""),
        },
    ];
    let mut wire = WireBatch::new();
    wire.push(
        WireRecord::new(&topic, None)
            .with_key(None)
            .with_partition(0)
            .with_headers(&headers),
    )?;
    wire.push(
        WireRecord::new(&topic, Some(b""))
            .with_key(Some(b""))
            .with_partition(0),
    )?;
    wire.push(WireRecord::new(&topic, Some(b"last")).with_partition(0))?;
    let mut serialized = Vec::new();
    SerdeBridge::encode_into(&(17_u64, "serde-native"), &mut serialized)?;
    wire.push(WireRecord::new(&topic, Some(&serialized)).with_partition(0))?;
    let result = producer.produce_wire_batch(&wire)?;
    assert_eq!(result.accepted(), 4, "{result:?}");
    assert!(result.all_accepted());
    producer.flush(10000)?;
    let consumer = consumer(brokers, &topic)?;
    let mut batches = Vec::new();
    let mut count = 0;
    while count < 4 {
        let batch = receive(&consumer)?;
        count += batch.len();
        batches.push(batch);
    }
    consumer.close()?;
    drop(consumer);
    let frames = batches
        .iter()
        .flat_map(ReceivedBatch::iter)
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(frames.len(), 4);
    assert_eq!(frames[0].key, None);
    assert_eq!(frames[0].payload, None);
    assert_eq!(frames[0].headers.iter().collect::<Vec<_>>(), headers);
    assert_eq!(frames[1].key, Some(&b""[..]));
    assert_eq!(frames[1].payload, Some(&b""[..]));
    assert_eq!(frames[2].payload, Some(&b"last"[..]));
    let (number, text): (u64, &str) = SerdeBridge::decode_borrowed(frames[3].payload.unwrap())?;
    assert_eq!((number, text), (17, "serde-native"));
    assert_eq!(
        frames.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    Ok(())
}
fn typed_roundtrip(brokers: &str) -> Result<()> {
    let producer = producer(brokers)?;
    let topic = format!("typed-native-{}", std::process::id());
    let mut batch = EventBatch::with_capacity(4);
    for value in 0..4 {
        batch.push(&Event {
            partition: 0,
            name: format!("event-{value}"),
            value,
        });
    }
    assert!(producer.produce_batch(&topic, &batch)?.all_accepted());
    producer.flush(10000)?;
    let consumer = consumer(brokers, &topic)?;
    let mut decoded = EventBatch::default();
    while decoded.len() < 4 {
        let received = receive(&consumer)?;
        for record in received.iter() {
            let record = record?;
            let payload = record.payload.unwrap();
            let view = Event::view(record)?;
            assert!(view.name.as_ptr() >= payload.as_ptr());
            assert!(view.name.as_ptr() < payload.as_ptr().wrapping_add(payload.len()));
        }
        received.decode_into(&mut decoded)?;
    }
    assert_eq!(decoded.column_value(), &[0, 1, 2, 3]);
    assert_eq!(
        decoded.column_name(),
        &["event-0", "event-1", "event-2", "event-3"]
    );
    consumer.assign(&[TopicPartition::new(&topic, 0, 0)])?;
    let mut count = 0;
    let deadline = Instant::now();
    while count < 4 {
        consumer.consume_views::<Event>(32, 500, |view| {
            assert_eq!(view.name, format!("event-{count}"));
            assert_eq!(view.value, count);
            count += 1;
            Ok(())
        })?;
        assert!(deadline.elapsed() < Duration::from_secs(20));
    }
    consumer.close()?;
    Ok(())
}
#[test]
fn integration_lifecycle_mock_native_batches() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "3")?;
    let owner = Producer::new(&conf)?;
    let brokers = owner.mock_bootstraps();
    wire_roundtrip(&brokers)?;
    typed_roundtrip(&brokers)
}
#[test]
#[ignore = "requires an explicitly provisioned Redpanda broker"]
fn integration_lifecycle_live_native_batches() -> Result<()> {
    let brokers =
        std::env::var("RDKAFKA_TEST_BROKERS").map_err(|error| Error::Config(error.to_string()))?;
    wire_roundtrip(&brokers)?;
    typed_roundtrip(&brokers)
}
#[test]
fn integration_scenario_partial_enqueue_and_validation() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?
        .set("message.max.bytes", "1000")?;
    let producer = Producer::new(&conf)?;
    let payload = vec![0; 2000];
    let mut wire = WireBatch::new();
    wire.push(WireRecord::new("partial-batch", Some(b"valid")).with_partition(0))?;
    wire.push(WireRecord::new("partial-batch", Some(&payload)).with_partition(0))?;
    let results = producer.produce_wire_batch(&wire)?;
    assert_eq!(results.results.len(), 2);
    assert!(results.results[0].is_ok());
    assert!(results.results[1].is_err());
    producer.flush(10000)?;
    let before = producer.outq_len();
    wire.push(WireRecord::new("invalid\0topic", Some(b"reject")))?;
    assert!(producer.produce_wire_batch(&wire).is_err());
    assert_eq!(producer.outq_len(), before);
    Ok(())
}
#[test]
fn integration_scenario_batch_caller_partitioner() -> Result<()> {
    let observed = Rc::new(Cell::new(0));
    let captured = Rc::clone(&observed);
    let current = std::thread::current().id();
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?;
    conf.partitioner_cb(move |_, _, _| {
        assert_eq!(std::thread::current().id(), current);
        captured.set(captured.get() + 1);
        0
    });
    let producer = Producer::new(&conf)?;
    // Prime native mock metadata before invoking a local automatic partitioner.
    producer.produce(ProduceRecord::new("batch-partitioner", Some(b"prime")))?;
    producer.flush(10000)?;
    let mut batch = WireBatch::new();
    batch.push(WireRecord::new("batch-partitioner", Some(b"one")))?;
    batch.push(WireRecord::new("batch-partitioner", Some(b"two")))?;
    let before = observed.get();
    assert_eq!(producer.produce_wire_batch(&batch)?.accepted(), 2);
    assert_eq!(observed.get(), before + 2);
    producer.flush(10000)
}

#[test]
fn integration_scenario_receive_observer_reentrant_guard() -> Result<()> {
    let mut host_conf = Conf::new();
    host_conf.set("test.mock.num.brokers", "1")?;
    let host = Producer::new(&host_conf)?;
    let brokers = host.mock_bootstraps();
    let topic = format!("batch-guard-{}", std::process::id());
    let producer = producer(&brokers)?;
    let mut wire = WireBatch::new();
    wire.push(WireRecord::new(&topic, Some(b"guarded")).with_partition(0))?;
    assert!(producer.produce_wire_batch(&wire)?.all_accepted());
    producer.flush(10000)?;
    let slot = Rc::new(std::cell::RefCell::new(None::<Consumer>));
    let captured = Rc::clone(&slot);
    let calls = Rc::new(Cell::new(0));
    let observed = Rc::clone(&calls);
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", &brokers)?
        .set("group.id", &topic)?
        .set("enable.auto.commit", "false")?;
    conf.consume_cb(move |message| {
        assert_eq!(message.payload.as_deref(), Some(&b"guarded"[..]));
        let slot = captured.borrow();
        assert!(matches!(
            slot.as_ref().unwrap().consume_batch(1, 0),
            Err(Error::ReentrantDrive)
        ));
        observed.set(observed.get() + 1);
    });
    let consumer = Consumer::new(&conf)?;
    consumer.assign(&[TopicPartition::new(&topic, 0, 0)])?;
    assert!(consumer.consume_batch(0, 0)?.is_empty());
    assert!(consumer.consume_batch(i32::MAX as usize + 1, 0).is_err());
    *slot.borrow_mut() = Some(consumer);
    let batch = receive(slot.borrow().as_ref().unwrap())?;
    assert_eq!(batch.get(0)?.unwrap().payload, Some(&b"guarded"[..]));
    assert_eq!(calls.get(), 1);
    let consumer = slot.borrow_mut().take().unwrap();
    consumer.close()?;
    Ok(())
}

#[test]
fn integration_scenario_retained_batch_recovers_unvisited_tail() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?;
    let host = Producer::new(&conf)?;
    let topic = format!("retained-tail-{}", std::process::id());
    let sender = producer(&host.mock_bootstraps())?;
    let mut wire = WireBatch::new();
    for payload in [b"good".as_slice(), b"bad", b"tail"] {
        wire.push(WireRecord::new(&topic, Some(payload)).with_partition(0))?;
    }
    assert!(sender.produce_wire_batch(&wire)?.all_accepted());
    sender.flush(10000)?;
    let receiver = consumer(&host.mock_bootstraps(), &topic)?;
    let mut batches = Vec::new();
    while batches.iter().map(ReceivedBatch::len).sum::<usize>() < 3 {
        batches.push(receive(&receiver)?);
    }
    let mut seen = Vec::new();
    for batch in &batches {
        let mut failed = None;
        let result = batch.visit_from(0, |index, record| {
            if record.payload == Some(b"bad") {
                failed = Some(index);
                return Err(Error::Codec("malformed record".into()));
            }
            seen.push(record.payload.unwrap().to_vec());
            Ok(())
        });
        if let Some(index) = failed {
            assert!(result.is_err());
            assert_eq!(batch.get(index)?.unwrap().payload, Some(&b"bad"[..]));
            batch.visit_from(index + 1, |_, record| {
                seen.push(record.payload.unwrap().to_vec());
                Ok(())
            })?;
        } else {
            result?;
        }
    }
    assert_eq!(seen, [b"good".to_vec(), b"tail".to_vec()]);
    assert!(
        batches[0]
            .visit_from(batches[0].len() + 1, |_, _| Ok(()))
            .is_err()
    );
    receiver.close()?;
    Ok(())
}

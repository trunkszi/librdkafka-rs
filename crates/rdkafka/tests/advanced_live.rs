//! Broker-backed transaction visibility, cooperative ownership, and administration.
use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    rc::Rc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rdkafka::*;
use serde::{Deserialize, Serialize};

const TIMEOUT: i32 = 20000;
fn brokers() -> String {
    std::env::var("RDKAFKA_TEST_BROKERS")
        .expect("RDKAFKA_TEST_BROKERS must name an explicitly provisioned broker")
}
fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
fn config(brokers: &str) -> Result<Conf> {
    let mut c = Conf::new();
    c.set("bootstrap.servers", brokers)?;
    Ok(c)
}
fn admin(brokers: &str) -> Result<AdminClient> {
    AdminClient::new(&config(brokers)?)
}
fn success(result: AdminResult) -> AdminResult {
    assert!(
        !result.has_errors(),
        "administrative resource failure: {result:?}"
    );
    result
}
fn topic(admin: &AdminClient, name: &str, count: i32) -> Result<()> {
    success(admin.create_topics(&[NewTopic::new(name, count, 1)], TIMEOUT)?);
    Ok(())
}
fn producer(brokers: &str) -> Result<Producer> {
    Producer::new(&config(brokers)?)
}
fn consumer(brokers: &str, group: &str) -> Result<Consumer> {
    let mut c = config(brokers)?;
    c.set("group.id", group)?
        .set("enable.auto.commit", "false")?
        .set("auto.offset.reset", "earliest")?
        .set("isolation.level", "read_committed")?;
    Consumer::new(&c)
}
fn send(p: &Producer, topic: &str, payload: &[u8]) -> Result<()> {
    let mut record = ProduceRecord::new(topic, Some(payload));
    record.partition = 0;
    p.produce(record)
}
fn message(c: &Consumer) -> Result<Message> {
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(message) = c.consume(250)? {
            return Ok(message);
        }
        assert!(Instant::now() < end, "record delivery deadline");
    }
}

#[test]
#[ignore = "requires an explicitly provisioned transactional Redpanda broker"]
fn e2e_lifecycle_live_atomic_transform_and_offsets() -> Result<()> {
    let brokers = brokers();
    let admin = admin(&brokers)?;
    let input = unique("atomic-input");
    let output = unique("atomic-output");
    let group = unique("atomic-group");
    topic(&admin, &input, 1)?;
    topic(&admin, &output, 1)?;
    let seed = producer(&brokers)?;
    send(&seed, &input, b"one")?;
    send(&seed, &input, b"two")?;
    seed.flush(TIMEOUT)?;
    let source = consumer(&brokers, &group)?;
    source.subscribe(&[&input])?;
    let first = message(&source)?;
    assert_eq!(first.payload.as_deref(), Some(&b"one"[..]));
    let mut conf = config(&brokers)?;
    conf.set("transactional.id", &unique("atomic-writer"))?;
    let tx = Producer::new(&conf)?;
    tx.init_transactions(TIMEOUT)?;
    let offset = TopicPartition::new(&input, 0, first.offset + 1);
    tx.begin_transaction()?;
    send(&tx, &output, b"aborted-one")?;
    tx.flush(TIMEOUT)?;
    tx.send_offsets_to_transaction(
        std::slice::from_ref(&offset),
        &source.consumer_group_metadata()?,
        TIMEOUT,
    )?;
    tx.abort_transaction(TIMEOUT)?;
    let committed = source.committed(std::slice::from_ref(&offset), Duration::from_secs(10))?;
    assert!(
        committed[0].offset < offset.offset,
        "aborted input offset became committed"
    );
    tx.begin_transaction()?;
    send(&tx, &output, b"ONE")?;
    tx.send_offsets_to_transaction(
        std::slice::from_ref(&offset),
        &source.consumer_group_metadata()?,
        TIMEOUT,
    )?;
    tx.commit_transaction(TIMEOUT)?;
    assert_eq!(
        source.committed(std::slice::from_ref(&offset), Duration::from_secs(10))?[0].offset,
        offset.offset
    );
    let second = message(&source)?;
    assert_eq!(second.payload.as_deref(), Some(&b"two"[..]));
    tx.begin_transaction()?;
    send(&tx, &output, b"TWO")?;
    let end = TopicPartition::new(&input, 0, second.offset + 1);
    tx.send_offsets_to_transaction(
        std::slice::from_ref(&end),
        &source.consumer_group_metadata()?,
        TIMEOUT,
    )?;
    tx.commit_transaction(TIMEOUT)?;
    assert_eq!(
        source.committed(std::slice::from_ref(&end), Duration::from_secs(10))?[0].offset,
        end.offset
    );
    source.close()?;
    let sink = consumer(&brokers, &unique("atomic-reader"))?;
    sink.assign(&[TopicPartition::new(&output, 0, 0)])?;
    assert_eq!(message(&sink)?.payload.as_deref(), Some(&b"ONE"[..]));
    assert_eq!(message(&sink)?.payload.as_deref(), Some(&b"TWO"[..]));
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until {
        assert!(
            sink.consume(200)?.is_none(),
            "aborted transaction leaked or output duplicated"
        );
    }
    sink.close()?;
    success(admin.delete_topics(&[&input, &output], TIMEOUT)?);
    Ok(())
}
fn owned(c: &Consumer) -> Result<BTreeSet<i32>> {
    Ok(c.assignment()?.iter().map(|p| p.partition).collect())
}
fn cooperative_consumer(
    brokers: &str,
    group: &str,
    callbacks: &Rc<Cell<usize>>,
) -> Result<Rc<Consumer>> {
    let mut conf = config(brokers)?;
    conf.set("group.id", group)?
        .set("partition.assignment.strategy", "cooperative-sticky")?
        .set("enable.auto.commit", "false")?
        .set("auto.offset.reset", "earliest")?;
    let slot: Rc<RefCell<std::rc::Weak<Consumer>>> = Rc::default();
    let handle = Rc::clone(&slot);
    let seen = Rc::clone(callbacks);
    let thread = std::thread::current().id();
    conf.rebalance_cb(move |code, partitions| {
        assert_eq!(std::thread::current().id(), thread);
        let consumer = handle
            .borrow()
            .upgrade()
            .expect("callback consumer remains alive");
        assert_eq!(consumer.rebalance_protocol(), "COOPERATIVE");
        assert!(
            !consumer.assignment_lost(),
            "graceful handoff must not lose ownership"
        );
        if code == -175 {
            consumer
                .incremental_assign(partitions)
                .expect("apply cooperative assignment");
            let from_start = partitions
                .iter()
                .map(|p| TopicPartition::new(&p.topic, p.partition, 0))
                .collect::<Vec<_>>();
            if !from_start.is_empty() {
                consumer
                    .seek(&from_start, Duration::from_secs(5))
                    .expect("seek newly assigned partitions inside callback");
            }
            let owned = consumer
                .assignment()
                .expect("query assigned partitions inside callback");
            assert!(partitions.iter().all(|p| {
                owned
                    .iter()
                    .any(|a| a.topic == p.topic && a.partition == p.partition)
            }));
        } else if code == -174 {
            consumer
                .incremental_unassign(partitions)
                .expect("apply cooperative revocation");
            let owned = consumer
                .assignment()
                .expect("query remaining partitions inside callback");
            assert!(partitions.iter().all(|p| {
                !owned
                    .iter()
                    .any(|a| a.topic == p.topic && a.partition == p.partition)
            }));
        } else {
            panic!("unexpected rebalance error {code}");
        }
        seen.set(seen.get() + 1);
    });
    let consumer = Rc::new(Consumer::new(&conf)?);
    *slot.borrow_mut() = Rc::downgrade(&consumer);
    Ok(consumer)
}
#[test]
#[ignore = "requires an explicitly provisioned cooperative Redpanda broker"]
fn e2e_lifecycle_live_cooperative_partition_handoff() -> Result<()> {
    let brokers = brokers();
    let admin = admin(&brokers)?;
    let name = unique("cooperative");
    topic(&admin, &name, 4)?;
    let group = unique("cooperative-group");
    let callbacks = Rc::new(Cell::new(0));
    let first = cooperative_consumer(&brokers, &group, &callbacks)?;
    first.subscribe(&[&name])?;
    let until = Instant::now() + Duration::from_secs(40);
    while owned(&first)?.len() != 4 {
        first.consume(100)?;
        assert!(
            Instant::now() < until,
            "initial cooperative assignment deadline"
        );
    }
    let second = cooperative_consumer(&brokers, &group, &callbacks)?;
    second.subscribe(&[&name])?;
    let until = Instant::now() + Duration::from_secs(40);
    loop {
        first.consume(100)?;
        second.consume(100)?;
        let a = owned(&first)?;
        let b = owned(&second)?;
        if a.len() == 2 && b.len() == 2 {
            assert!(a.is_disjoint(&b));
            assert_eq!(a.union(&b).copied().collect::<Vec<_>>(), [0, 1, 2, 3]);
            break;
        }
        assert!(Instant::now() < until, "cooperative handoff deadline");
    }
    assert_eq!(first.rebalance_protocol(), "COOPERATIVE");
    assert!(!first.assignment_lost());
    assert!(!second.assignment_lost());
    second.close()?;
    let until = Instant::now() + Duration::from_secs(40);
    while owned(&first)?.len() != 4 {
        first.consume(100)?;
        assert!(Instant::now() < until, "partition return deadline");
    }
    assert!(callbacks.get() >= 3);
    first.close()?;
    success(admin.delete_topics(&[&name], TIMEOUT)?);
    Ok(())
}

#[derive(Debug, PartialEq, Record)]
struct TypedEvent {
    #[record(partition)]
    partition: i32,
    label: String,
    value: i64,
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct LegacyEvent<'a> {
    #[serde(borrow)]
    label: &'a str,
    value: u64,
}
#[test]
#[ignore = "requires an explicitly provisioned Redpanda broker"]
fn e2e_lifecycle_live_typed_and_serde_native_batches() -> Result<()> {
    let brokers = brokers();
    let admin = admin(&brokers)?;
    let name = unique("advanced-batch");
    topic(&admin, &name, 1)?;
    let p = producer(&brokers)?;
    let mut batch = TypedEventBatch::default();
    for value in 0..5 {
        batch.push(&TypedEvent {
            partition: 0,
            label: format!("event-{value}"),
            value,
        });
    }
    assert_eq!(p.produce_batch(&name, &batch)?.accepted(), 5);
    p.flush(TIMEOUT)?;
    let c = consumer(&brokers, &unique("advanced-batch-reader"))?;
    c.assign(&[TopicPartition::new(&name, 0, 0)])?;
    let mut count = 0;
    let until = Instant::now() + Duration::from_secs(30);
    while count < 5 {
        c.consume_views::<TypedEvent>(32, 250, |view| {
            assert_eq!(view.label, format!("event-{count}"));
            assert_eq!(view.value, count);
            count += 1;
            Ok(())
        })?;
        assert!(Instant::now() < until);
    }
    let mut bytes = Vec::new();
    let legacy = LegacyEvent {
        label: "borrowed legacy",
        value: 99,
    };
    SerdeBridge::encode_into(&legacy, &mut bytes)?;
    let mut wire = WireBatch::new();
    wire.push(WireRecord::new(&name, Some(&bytes)).with_partition(0))?;
    assert!(p.produce_wire_batch(&wire)?.all_accepted());
    p.flush(TIMEOUT)?;
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let received = c.consume_batch(32, 250)?;
        if let Some(record) = received.get(0)? {
            let decoded: LegacyEvent<'_> = SerdeBridge::decode_record_borrowed(record)?;
            assert_eq!(decoded, legacy);
            let payload = record.payload.unwrap();
            assert!(decoded.label.as_ptr() >= payload.as_ptr());
            assert!(decoded.label.as_ptr() < payload.as_ptr().wrapping_add(payload.len()));
            assert_eq!(received.len(), 1);
            break;
        }
        assert!(Instant::now() < until);
    }
    c.close()?;
    success(admin.delete_topics(&[&name], TIMEOUT)?);
    Ok(())
}

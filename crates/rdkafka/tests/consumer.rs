//! Consumer partition ownership, offsets, subscription, and queue lifecycles.
use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use rdkafka::*;
fn host() -> Result<(Producer, String)> {
    let mut c = Conf::new();
    c.set("test.mock.num.brokers", "3")?;
    let p = Producer::new(&c)?;
    let b = p.mock_bootstraps();
    Ok((p, b))
}
fn producer(brokers: &str) -> Result<Producer> {
    let mut c = Conf::new();
    c.set("bootstrap.servers", brokers)?;
    Producer::new(&c)
}
fn send(p: &Producer, topic: &str) -> Result<()> {
    for n in 0..3 {
        p.produce(ProduceRecord {
            topic,
            partition: 0,
            key: Some(b"key"),
            payload: Some(&[n]),
            headers: None,
            timestamp: None,
            token: u64::from(n),
        })?;
    }
    p.flush(10000)
}
#[test]
fn integration_lifecycle_simple_seek_and_child_retention() -> Result<()> {
    let (_host, b) = host()?;
    let p = producer(&b)?;
    let topic = format!("simple-{}", std::process::id());
    send(&p, &topic)?;
    let mut c = Conf::new();
    c.set("bootstrap.servers", &b)?
        .set("enable.partition.eof", "false")?;
    let observed = Rc::new(RefCell::new(Vec::new()));
    let obs = Rc::clone(&observed);
    let thread = std::thread::current().id();
    c.consume_cb(move |m| {
        assert_eq!(thread, std::thread::current().id());
        obs.borrow_mut().push(m.offset);
    });
    let consumer = SimpleConsumer::new(&c)?;
    let t = Topic::for_consumer(&consumer, &topic, &Conf::topic())?;
    consumer.start(&t, 0, Offset::Beginning)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let first = loop {
        if let Some(m) = consumer.consume(&t, 0, 100)? {
            break m;
        }
        assert!(Instant::now() < deadline);
    };
    assert_eq!(first.payload.as_deref(), Some(&[0][..]));
    consumer.seek(&t, 0, Offset::Absolute(2), 5000)?;
    let last = loop {
        if let Some(m) = consumer.consume(&t, 0, 100)? {
            break m;
        }
        assert!(Instant::now() < deadline);
    };
    assert_eq!(last.payload.as_deref(), Some(&[2][..]));
    consumer.stop(&t, 0)?;
    assert_eq!(*observed.borrow(), vec![0, 2]);
    let queue = Queue::new(&consumer)?;
    drop(consumer);
    assert_eq!(t.name(), topic);
    assert!(queue.poll(0)? >= 0);
    drop(t);
    drop(queue);
    Ok(())
}
#[test]
fn integration_lifecycle_group_offsets_and_metadata() -> Result<()> {
    let (_host, b) = host()?;
    let p = producer(&b)?;
    let topic = format!("group-{}", std::process::id());
    send(&p, &topic)?;
    let meta = p.metadata(Some(&topic), 5000)?;
    assert!(
        meta.topics
            .iter()
            .any(|t| t.name == topic && !t.partitions.is_empty())
    );
    assert_eq!(p.query_watermark_offsets(&topic, 0, 5000)?, (0, 3));
    let mut c = Conf::new();
    c.set("bootstrap.servers", &b)?
        .set("group.id", &format!("management-{}", std::process::id()))?
        .set("enable.auto.commit", "false")?
        .set("enable.auto.offset.store", "false")?
        .set("auto.offset.reset", "earliest")?;
    let commits = Rc::new(RefCell::new(Vec::new()));
    let cc = Rc::clone(&commits);
    let thread = std::thread::current().id();
    c.offset_commit_cb(move |err, parts| {
        assert_eq!(thread, std::thread::current().id());
        cc.borrow_mut().push((err, parts.len()));
    });
    let group = Consumer::new(&c)?;
    let part = TopicPartition::new(&topic, 0, 0);
    group.assign(std::slice::from_ref(&part))?;
    assert_eq!(group.assignment()?.len(), 1);
    group.pause(std::slice::from_ref(&part))?;
    group.resume(&[part])?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if group.consume(100)?.is_some() {
            break;
        }
        assert!(Instant::now() < deadline);
    }
    let pos = group.position(&[TopicPartition::new(&topic, 0, 0)])?;
    assert!(pos[0].offset >= 1);
    let target = TopicPartition::new(&topic, 0, 1);
    group.offsets_store(std::slice::from_ref(&target))?;
    group.commit_sync(std::slice::from_ref(&target))?;
    assert_eq!(
        group.committed(std::slice::from_ref(&target), Duration::from_secs(5))?[0].offset,
        1
    );
    group.commit_async(&[target])?;
    while commits.borrow().is_empty() {
        group.consume(100)?;
        assert!(Instant::now() < deadline);
    }
    assert_eq!(commits.borrow()[0].0, 0);
    group.unassign()?;
    group.close()?;
    Ok(())
}
#[cfg(unix)]
#[test]
fn integration_lifecycle_queue_forward_and_fd_wakeup() -> Result<()> {
    use std::{
        io::Read,
        os::{fd::AsRawFd, unix::net::UnixStream},
    };
    let (_host, b) = host()?;
    let p = producer(&b)?;
    let topic = format!("queue-{}", std::process::id());
    // Topic metadata must exist before native partition consumption starts.
    send(&p, &topic)?;
    let mut c = Conf::new();
    c.set("bootstrap.servers", &b)?;
    let consumer = SimpleConsumer::new(&c)?;
    let t = Topic::for_consumer(&consumer, &topic, &Conf::topic())?;
    let source = Rc::new(Queue::new(&consumer)?);
    let destination = Rc::new(Queue::new(&consumer)?);
    source.forward(Rc::clone(&destination))?;
    assert!(destination.forward(Rc::clone(&source)).is_err());
    let (mut reader, writer) = UnixStream::pair().map_err(|e| Error::Layout(e.to_string()))?;
    reader
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| Error::Layout(e.to_string()))?;
    destination.io_event_enable(writer.as_raw_fd(), b"wake");
    consumer.start_queue(&t, 0, Offset::Beginning, &source)?;
    let mut event = [0u8; 4];
    reader
        .read_exact(&mut event)
        .map_err(|e| Error::Layout(e.to_string()))?;
    assert_eq!(&event, b"wake");
    let deadline = Instant::now() + Duration::from_secs(10);
    let m = loop {
        if let Some(m) = destination.consume(100)? {
            break m;
        }
        assert!(Instant::now() < deadline);
    };
    assert_eq!(m.payload.as_deref(), Some(&[0][..]));
    consumer.stop(&t, 0)?;
    destination.io_event_enable(-1, &[]);
    source.unforward();
    drop(consumer);
    drop(t);
    assert!(source.poll(0)? >= 0);
    Ok(())
}

#[test]
fn integration_lifecycle_subscribe_rebalance_and_unsubscribe() -> Result<()> {
    let (_host, b) = host()?;
    let p = producer(&b)?;
    let topic = format!("subscribe-{}", std::process::id());
    send(&p, &topic)?;
    let mut c = Conf::new();
    c.set("bootstrap.servers", &b)?
        .set("group.id", &format!("subscribe-{}", std::process::id()))?
        .set("auto.offset.reset", "earliest")?;
    let calls = Rc::new(RefCell::new(Vec::new()));
    let cc = Rc::clone(&calls);
    let thread = std::thread::current().id();
    let handle: Rc<RefCell<Option<std::rc::Weak<Consumer>>>> = Rc::default();
    let local = Rc::clone(&handle);
    c.rebalance_cb(move |code, parts| {
        assert_eq!(thread, std::thread::current().id());
        cc.borrow_mut().push((code, parts.len()));
        let consumer = local
            .borrow()
            .as_ref()
            .and_then(std::rc::Weak::upgrade)
            .expect("consumer remains alive");
        assert_eq!(consumer.rebalance_protocol(), "EAGER");
        if code == -175 {
            consumer
                .assign(parts)
                .expect("rebalance callback assigns explicitly");
            assert_eq!(
                consumer.assignment().expect("assignment query").len(),
                parts.len()
            );
        } else if code == -174 {
            consumer
                .unassign()
                .expect("rebalance callback revokes explicitly");
        }
    });
    let consumer = Rc::new(Consumer::new(&c)?);
    *handle.borrow_mut() = Some(Rc::downgrade(&consumer));
    consumer.subscribe(&[&topic])?;
    assert!(consumer.subscription()?.iter().any(|p| p.topic == topic));
    let deadline = Instant::now() + Duration::from_secs(15);
    let m = loop {
        if let Some(m) = consumer.consume(100)? {
            break m;
        }
        assert!(Instant::now() < deadline);
    };
    assert_eq!(m.payload.as_deref(), Some(&[0][..]));
    assert!(calls.borrow().iter().any(|(_, n)| *n > 0));
    consumer.unsubscribe()?;
    consumer.close()?;
    Ok(())
}

#[test]
fn integration_scenario_duration_limits_reject_before_native_seek() -> Result<()> {
    let mut config = Conf::new();
    config
        .set("test.mock.num.brokers", "1")?
        .set("group.id", "duration-limits")?;
    let consumer = Consumer::new(&config)?;
    let excessive = Duration::from_secs(u64::MAX);
    assert!(matches!(
        consumer.seek(&[], excessive),
        Err(Error::Config(_))
    ));
    assert!(matches!(
        consumer.committed(&[], excessive),
        Err(Error::Config(_))
    ));
    consumer.close()?;
    consumer.close()?;
    Ok(())
}

#[test]
fn integration_scenario_queue_forward_rejects_other_client_without_side_effects() -> Result<()> {
    let config = Conf::new();
    let first = SimpleConsumer::new(&config)?;
    let second = SimpleConsumer::new(&config)?;
    let source = Queue::new(&first)?;
    let foreign = Rc::new(Queue::new(&second)?);
    assert!(
        matches!(source.forward(Rc::clone(&foreign)), Err(Error::Config(message)) if message == "queue belongs to another client")
    );
    assert_eq!(Rc::strong_count(&foreign), 1);
    let own = Rc::new(Queue::new(&first)?);
    source.forward(Rc::clone(&own))?;
    assert_eq!(Rc::strong_count(&own), 2);
    source.unforward();
    assert_eq!(Rc::strong_count(&own), 1);
    Ok(())
}

#[test]
fn integration_scenario_incremental_deltas_preserve_other_partitions() -> Result<()> {
    let mut host = Conf::new();
    host.set("test.mock.num.brokers", "3")?;
    let producer = Producer::new(&host)?;
    let mut config = Conf::new();
    config
        .set("bootstrap.servers", &producer.mock_bootstraps())?
        .set("group.id", "incremental-contract")?
        .set("partition.assignment.strategy", "cooperative-sticky")?;
    let consumer = Consumer::new(&config)?;
    assert!(!consumer.assignment_lost());
    assert_eq!(consumer.rebalance_protocol(), "NONE");
    let a = TopicPartition::new("incremental-topic", 0, Offset::Beginning.value());
    let b = TopicPartition::new("incremental-topic", 1, Offset::Beginning.value());
    consumer.incremental_assign(std::slice::from_ref(&a))?;
    consumer.incremental_assign(std::slice::from_ref(&a))?;
    consumer.incremental_assign(std::slice::from_ref(&b))?;
    assert_eq!(consumer.assignment()?.len(), 2);
    consumer.incremental_unassign(std::slice::from_ref(&a))?;
    consumer.incremental_unassign(std::slice::from_ref(&a))?;
    let remaining = consumer.assignment()?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].partition, b.partition);
    let invalid = TopicPartition::new("invalid\0topic", 2, 0);
    assert!(consumer.incremental_assign(&[invalid]).is_err());
    assert_eq!(consumer.assignment()?.len(), 1);
    consumer.incremental_unassign(&[b])?;
    consumer.close()?;
    assert!(consumer.assignment()?.is_empty());
    consumer.close()?;
    assert!(consumer.assignment()?.is_empty());
    Ok(())
}

#[path = "support/live_broker.rs"]
pub mod live_broker;

mod live {
    use std::{
        cell::{Cell, RefCell},
        collections::BTreeSet,
        rc::Rc,
        time::{Duration, Instant},
    };

    use rdkafka::*;

    use super::live_broker::*;

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
}

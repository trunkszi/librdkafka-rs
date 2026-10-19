//! Real mock-broker partition, group, callback, and ownership scenarios.
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
#[test]
fn integration_scenario_partitioner_and_reentrant_delivery() -> Result<()> {
    let (_host, b) = host()?;
    let mut c = Conf::new();
    c.set("bootstrap.servers", &b)?;
    let partition_calls = Rc::new(RefCell::new(0));
    let pc = Rc::clone(&partition_calls);
    c.partitioner_cb(move |_, key, n| {
        assert_eq!(key, Some(&b"k"[..]));
        assert!(n > 0);
        *pc.borrow_mut() += 1;
        0
    });
    let handle: Rc<RefCell<Option<std::rc::Weak<Producer>>>> = Rc::default();
    let local = Rc::clone(&handle);
    c.delivery_report_cb(move |r| {
        assert!(r.error.is_none());
        if r.token == 0
            && let Some(p) = local.borrow().as_ref().and_then(std::rc::Weak::upgrade)
        {
            p.produce(ProduceRecord {
                topic: &r.topic,
                partition: 0,
                key: None,
                payload: Some(b"followup"),
                headers: None,
                timestamp: None,
                token: 1,
            })
            .expect("callback enqueue must succeed");
        }
    });
    let p = Rc::new(Producer::new(&c)?);
    *handle.borrow_mut() = Some(Rc::downgrade(&p));
    let topic = format!("partitioner-{}", std::process::id());
    let mut record = ProduceRecord::new(&topic, Some(b"value"));
    record.key = Some(b"k");
    p.produce(record)?;
    p.flush(10000)?;
    p.flush(10000)?;
    assert_eq!(*partition_calls.borrow(), 1);
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
fn integration_scenario_native_delivery_burst_is_lossless() -> Result<()> {
    let (_host, b) = host()?;
    let delivered = Rc::new(RefCell::new(vec![false; 5000]));
    let observed = Rc::clone(&delivered);
    let callback_thread = std::thread::current().id();
    let mut config = Conf::new();
    config.set("bootstrap.servers", &b)?;
    config.delivery_report_cb(move |report| {
        assert_eq!(std::thread::current().id(), callback_thread);
        assert!(report.error.is_none(), "delivery failed: {report:?}");
        let mut seen = observed.borrow_mut();
        let token = usize::try_from(report.token).expect("valid delivery token");
        assert!(token < seen.len(), "unexpected delivery token");
        assert!(!seen[token], "duplicate delivery report");
        seen[token] = true;
    });
    let p = Producer::new(&config)?;
    let topic = format!("delivery-burst-{}", std::process::id());
    for token in 0..5000 {
        p.produce(ProduceRecord {
            topic: &topic,
            partition: 0,
            key: None,
            payload: Some(b"x"),
            headers: None,
            timestamp: None,
            token,
        })?;
    }
    p.flush(15000)?;
    assert!(delivered.borrow().iter().all(|seen| *seen));
    assert_eq!(p.outq_len(), 0);
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
fn integration_scenario_nested_drive_preserves_pending_deliveries() -> Result<()> {
    let (_host, b) = host()?;
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", &b)?
        .set("group.id", "nested-drive")?;
    let producers: Rc<RefCell<Vec<std::rc::Weak<Producer>>>> = Rc::default();
    let clients = Rc::clone(&producers);
    let consumer_handle: Rc<RefCell<Option<std::rc::Weak<Consumer>>>> = Rc::default();
    let group = Rc::clone(&consumer_handle);
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let reports = Rc::clone(&delivered);
    conf.delivery_report_cb(move |report| {
        assert!(report.error.is_none());
        reports.borrow_mut().push(report.token);
        for client in clients.borrow().iter().filter_map(std::rc::Weak::upgrade) {
            assert!(matches!(client.poll(0), Err(Error::ReentrantDrive)));
            let error = client
                .flush(0)
                .expect_err("nested flush must not fetch delivery events");
            assert!(matches!(error, Error::ReentrantDrive));
            assert!(error.to_string().contains("REENTRANT_CALLBACK_DRIVE"));
        }
        if let Some(consumer) = group.borrow().as_ref().and_then(std::rc::Weak::upgrade) {
            assert!(matches!(consumer.consume(0), Err(Error::ReentrantDrive)));
            assert!(matches!(
                consumer.consume_native(0),
                Err(Error::ReentrantDrive)
            ));
        }
        if report.token == 0 {
            let p = clients.borrow()[0]
                .upgrade()
                .expect("producer remains alive");
            p.produce(ProduceRecord {
                topic: &report.topic,
                partition: 0,
                key: None,
                payload: Some(b"callback-followup"),
                headers: None,
                timestamp: None,
                token: 2,
            })
            .expect("plain callback enqueue remains supported");
        }
    });
    let first = Rc::new(Producer::new(&conf)?);
    let second = Rc::new(Producer::new(&conf)?);
    let consumer = Rc::new(Consumer::new(&conf)?);
    *producers.borrow_mut() = vec![Rc::downgrade(&first), Rc::downgrade(&second)];
    *consumer_handle.borrow_mut() = Some(Rc::downgrade(&consumer));
    let topic = format!("nested-drive-{}", std::process::id());
    for token in 0..2 {
        first.produce(ProduceRecord {
            topic: &topic,
            partition: 0,
            key: None,
            payload: Some(b"initial"),
            headers: None,
            timestamp: None,
            token,
        })?;
    }
    first.flush(10000)?;
    first.flush(10000)?;
    first.poll(0)?;
    delivered.borrow_mut().sort_unstable();
    assert_eq!(*delivered.borrow(), vec![0, 1, 2]);
    consumer.close()?;
    Ok(())
}

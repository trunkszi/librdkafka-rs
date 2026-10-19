//! Native caller-thread consumption and topic-scoped producer scenarios.
use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use rdkafka::*;

#[test]
fn integration_lifecycle_queue_poll_delivers_full_native_consume_callback() -> Result<()> {
    let mut host = Conf::new();
    host.set("test.mock.num.brokers", "1")?;
    let producer = Producer::new(&host)?;
    let topic_name = format!("queue-native-callback-{}", std::process::id());
    let mut topic_config = Conf::topic();
    let calls = Rc::new(RefCell::new(Vec::new()));
    let captured = Rc::clone(&calls);
    let thread = std::thread::current().id();
    topic_config.partitioner_cb(move |topic, key, count| {
        assert_eq!(std::thread::current().id(), thread);
        assert!(count > 0);
        captured
            .borrow_mut()
            .push((topic.to_owned(), key.map(<[u8]>::to_vec)));
        0
    });
    let topic = Topic::new(&producer, &topic_name, &topic_config)?;
    producer.produce_to_topic(
        &topic,
        ProduceRecord {
            topic: "ignored-name",
            partition: 0,
            key: None,
            payload: None,
            headers: None,
            timestamp: Some(1234567),
            token: 0,
        },
    )?;
    producer.flush(10000)?;
    let mut headers = Headers::new();
    headers.add("duplicate", None).add("duplicate", Some(b""));
    producer.produce((
        &topic,
        ProduceRecord {
            topic: "also-ignored",
            partition: PARTITION_UA,
            key: Some(b""),
            payload: Some(b"queue-payload"),
            headers: Some(&headers),
            timestamp: Some(1234568),
            token: 1,
        },
    ))?;
    producer.flush(10000)?;
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(calls.borrow()[0], (topic_name.clone(), Some(Vec::new())));
    let received = Rc::new(RefCell::new(Vec::new()));
    let output = Rc::clone(&received);
    let mut config = Conf::new();
    config.set("bootstrap.servers", &producer.mock_bootstraps())?;
    config.consume_cb(move |message| {
        assert_eq!(std::thread::current().id(), thread);
        output.borrow_mut().push(message.clone());
    });
    let consumer = SimpleConsumer::new(&config)?;
    let source = Topic::for_consumer(&consumer, &topic_name, &Conf::topic())?;
    let queue = Queue::new(&consumer)?;
    consumer.start_queue(&source, 0, Offset::Beginning, &queue)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    while received.borrow().len() < 2 {
        queue.poll(100)?;
        assert!(Instant::now() < deadline);
    }
    let records = received.borrow();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].topic, topic_name);
    assert_eq!(records[0].key, None);
    assert_eq!(records[0].payload, None);
    assert_eq!(records[0].timestamp, Some(1234567));
    assert_eq!(records[1].key, Some(Vec::new()));
    assert_eq!(records[1].payload.as_deref(), Some(&b"queue-payload"[..]));
    assert_eq!(records[1].timestamp, Some(1234568));
    let header_values = records[1]
        .headers
        .iter()
        .map(|h| (&h.name[..], h.value.as_deref()))
        .collect::<Vec<_>>();
    assert_eq!(
        header_values,
        vec![("duplicate", None), ("duplicate", Some(&b""[..]))]
    );
    assert_eq!(
        (records[0].partition, records[0].offset, records[1].offset),
        (0, 0, 1)
    );
    consumer.stop(&source, 0)?;
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
fn integration_lifecycle_registered_consumer_drop_uses_native_cleanup() -> Result<()> {
    let mut config = Conf::new();
    config.set("test.mock.num.brokers", "1")?;
    let producer = Producer::new(&config)?;
    let name = format!("drop-registered-{}", std::process::id());
    producer.produce(ProduceRecord {
        topic: &name,
        partition: 0,
        key: None,
        payload: Some(b"ready"),
        headers: None,
        timestamp: None,
        token: 0,
    })?;
    producer.flush(10000)?;
    let owner: Rc<RefCell<Option<std::rc::Weak<Consumer>>>> = Rc::default();
    let local = Rc::clone(&owner);
    let callbacks = Rc::new(std::cell::Cell::new(0));
    let observed = Rc::clone(&callbacks);
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", &producer.mock_bootstraps())?
        .set("group.id", &name)?
        .set("auto.offset.reset", "earliest")?;
    conf.rebalance_cb(move |code, parts| {
        observed.set(observed.get() + 1);
        let consumer = local.borrow().as_ref().unwrap().upgrade().unwrap();
        if code == -175 {
            consumer.assign(parts).unwrap();
        } else if code == -174 {
            consumer.unassign().unwrap();
        }
    });
    let consumer = Rc::new(Consumer::new(&conf)?);
    *owner.borrow_mut() = Some(Rc::downgrade(&consumer));
    consumer.subscribe(&[&name])?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if consumer.consume(100)?.is_some() {
            break;
        }
        assert!(Instant::now() < deadline);
    }
    let before = callbacks.get();
    assert!(before > 0);
    drop(consumer);
    assert!(owner.borrow().as_ref().unwrap().upgrade().is_none());
    assert_eq!(
        callbacks.get(),
        before,
        "destruction must not call Rust observers"
    );
    Ok(())
}

#[test]
fn integration_scenario_rebalance_panic_before_ack_cleanup() -> Result<()> {
    const CHILD: &str = "RDKAFKA_PANIC_DROP_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let mut host = Conf::new();
        host.set("test.mock.num.brokers", "1")?;
        let producer = Producer::new(&host)?;
        let topic = format!("panic-before-ack-{}", std::process::id());
        producer.produce(ProduceRecord {
            topic: &topic,
            partition: 0,
            key: None,
            payload: Some(b"pending"),
            headers: None,
            timestamp: None,
            token: 0,
        })?;
        producer.flush(10000)?;
        let mut conf = Conf::new();
        conf.set("bootstrap.servers", &producer.mock_bootstraps())?
            .set("group.id", &topic)?
            .set("partition.assignment.strategy", "cooperative-sticky")?;
        conf.rebalance_cb(|_, _| {
            panic!("caller callback failed before assignment acknowledgement")
        });
        let consumer = Consumer::new(&conf)?;
        consumer.subscribe(&[&topic])?;
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                consumer.consume(100).unwrap();
            }
        }));
        assert!(failed.is_err(), "the assignment callback must execute");
        drop(consumer);
        return Ok(());
    }
    let mut child = std::process::Command::new(
        std::env::current_exe().map_err(|e| Error::Config(e.to_string()))?,
    )
    .args([
        "--exact",
        "integration_scenario_rebalance_panic_before_ack_cleanup",
        "--nocapture",
    ])
    .env(CHILD, "1")
    .spawn()
    .map_err(|e| Error::Config(e.to_string()))?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().map_err(|e| Error::Config(e.to_string()))? {
            assert!(
                status.success(),
                "rebalance panic cleanup child failed: {status}"
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            child.kill().map_err(|e| Error::Config(e.to_string()))?;
            child.wait().map_err(|e| Error::Config(e.to_string()))?;
            panic!("rebalance panic cleanup exceeded the bounded subprocess deadline");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn integration_scenario_partitioner_panic_restores_callback_before_retry() -> Result<()> {
    let mut config = Conf::new();
    config.set("test.mock.num.brokers", "1")?;
    let calls = Rc::new(std::cell::Cell::new(0));
    let captured = Rc::clone(&calls);
    config.partitioner_cb(move |_, _, count| {
        captured.set(captured.get() + 1);
        if captured.get() == 1 {
            panic!("partitioner rejected first invocation");
        }
        count - 1
    });
    let p = Producer::new(&config)?;
    let topic = format!("partitioner-unwind-{}", std::process::id());
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        p.produce(ProduceRecord::new(&topic, Some(b"must-not-enqueue")))
    }));
    assert!(failed.is_err());
    assert_eq!(p.outq_len(), 0);
    p.produce(ProduceRecord::new(&topic, Some(b"retry")))?;
    p.flush(10000)?;
    assert_eq!(calls.get(), 2);
    Ok(())
}

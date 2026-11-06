//! Native callback delivery, reentrancy, panic recovery, and observer cleanup.
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

#[test]
fn integration_scenario_transaction_offsets_reject_callback_reentrancy() -> Result<()> {
    let owner: Rc<RefCell<Option<std::rc::Weak<Producer>>>> = Rc::default();
    let producer_owner = Rc::clone(&owner);
    let metadata: Rc<RefCell<Option<ConsumerGroupMetadata>>> = Rc::default();
    let metadata_ref = Rc::clone(&metadata);
    let calls = Rc::new(std::cell::Cell::new(0));
    let captured = Rc::clone(&calls);
    let mut config = Conf::new();
    config.set("test.mock.num.brokers", "1")?;
    config.delivery_report_cb(move |_| {
        let p = producer_owner.borrow().as_ref().unwrap().upgrade().unwrap();
        let metadata = metadata_ref.borrow();
        assert!(matches!(
            p.send_offsets_to_transaction(&[], metadata.as_ref().unwrap(), 100),
            Err(Error::ReentrantDrive)
        ));
        captured.set(captured.get() + 1);
    });
    let p = Rc::new(Producer::new(&config)?);
    *owner.borrow_mut() = Some(Rc::downgrade(&p));
    let mut consumer_config = Conf::new();
    consumer_config
        .set("bootstrap.servers", &p.mock_bootstraps())?
        .set("group.id", "offset-drive-guard")?;
    let consumer = Consumer::new(&consumer_config)?;
    *metadata.borrow_mut() = Some(consumer.consumer_group_metadata()?);
    let name = format!("offset-drive-guard-{}", std::process::id());
    let mut record = ProduceRecord::new(&name, Some(b"offset-guard"));
    record.partition = 0;
    p.produce(record)?;
    p.flush(10000)?;
    assert_eq!(calls.get(), 1);
    consumer.close()?;
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

#[path = "support/live_broker.rs"]
pub mod live_broker;

mod live {
    use std::{
        cell::RefCell,
        rc::Rc,
        time::{Duration, Instant},
    };

    use rdkafka::*;

    use super::live_broker::*;

    #[test]
    #[ignore = "requires an explicitly provisioned Redpanda broker"]
    fn e2e_lifecycle_live_partitioner_observes_topic_expansion_after_metadata_refresh() -> Result<()>
    {
        let brokers = brokers();
        let admin = admin(&brokers)?;
        let name = unique("partitioner-expansion");
        topic(&admin, &name, 1)?;
        let counts = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&counts);
        let mut config = config(&brokers)?;
        config.partitioner_cb(move |_, _, count| {
            captured.borrow_mut().push(count);
            count - 1
        });
        let p = Producer::new(&config)?;
        p.produce(ProduceRecord::new(&name, Some(b"before-expansion")))?;
        p.flush(TIMEOUT)?;
        assert_eq!(*counts.borrow(), [1]);
        success(admin.create_partitions(
            &[NewPartitions {
                topic: name.clone(),
                total_count: 3,
            }],
            TIMEOUT,
        )?);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let metadata = p.metadata(Some(&name), TIMEOUT)?;
            if metadata
                .topics
                .iter()
                .any(|topic| topic.name == name && topic.partitions.len() == 3)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "expanded metadata did not become visible"
            );
        }
        p.produce(ProduceRecord::new(&name, Some(b"after-expansion")))?;
        p.flush(TIMEOUT)?;
        assert_eq!(*counts.borrow(), [1, 3]);
        let c = consumer(&brokers, &unique("expanded-reader"))?;
        c.assign(&[TopicPartition::new(&name, 2, 0)])?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(message) = c.consume(100)? {
                assert_eq!(message.partition, 2);
                assert_eq!(message.payload.as_deref(), Some(&b"after-expansion"[..]));
                break;
            }
            assert!(
                Instant::now() < deadline,
                "expanded partition did not receive record"
            );
        }
        c.close()?;
        success(admin.delete_topics(&[&name], TIMEOUT)?);
        Ok(())
    }
}

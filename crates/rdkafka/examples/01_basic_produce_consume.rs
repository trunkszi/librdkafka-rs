//! Basic synchronous produce and consume workflow with caller-thread callbacks.
//!
//! Run with the in-process native mock broker:
//!   cargo run -p rdkafka --example 01_basic_produce_consume
//! Or against an external Kafka / Redpanda cluster:
//!   RDKAFKA_BROKERS=127.0.0.1:9092 cargo run -p rdkafka --example 01_basic_produce_consume

use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use rdkafka::{Conf, Consumer, Headers, ProduceRecord, Producer, Result, TopicPartition};

fn resolve_brokers() -> Result<(String, Option<Producer>)> {
    if let Ok(brokers) = std::env::var("RDKAFKA_BROKERS") {
        return Ok((brokers, None));
    }
    let mut mock_conf = Conf::new();
    mock_conf.set("test.mock.num.brokers", "3")?;
    let host = Producer::new(&mock_conf)?;
    let brokers = host.mock_bootstraps();
    Ok((brokers, Some(host)))
}

fn main() -> Result<()> {
    let (brokers, _mock_host) = resolve_brokers()?;
    let topic = format!("example-basic-{}", std::process::id());

    // Local delivery callbacks can capture !Send state such as Rc<RefCell<_>>
    // because callbacks run synchronously when the caller polls or flushes.
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let delivered_cb = Rc::clone(&delivered);

    let mut producer_conf = Conf::new();
    producer_conf
        .set("bootstrap.servers", &brokers)?
        .set("message.timeout.ms", "10000")?;
    producer_conf.delivery_report_cb(move |report| {
        delivered_cb
            .borrow_mut()
            .push((report.token, report.partition, report.offset));
    });

    let producer = Producer::new(&producer_conf)?;

    let mut headers = Headers::new();
    headers
        .add("trace-id", Some(b"req-001"))
        .add("nullable-tag", None)
        .add("empty-tag", Some(b""));

    producer.produce(ProduceRecord {
        topic: &topic,
        partition: 0,
        key: Some(b"account-42"),
        payload: Some(b"order-created"),
        headers: Some(&headers),
        timestamp: Some(1_700_000_000_000),
        token: 1001,
    })?;
    producer.flush(10_000)?;

    println!(
        "Delivered {} record(s): {:?}",
        delivered.borrow().len(),
        *delivered.borrow()
    );

    let mut consumer_conf = Conf::new();
    consumer_conf
        .set("bootstrap.servers", &brokers)?
        .set("group.id", &topic)?
        .set("enable.auto.commit", "false")?
        .set("auto.offset.reset", "earliest")?;

    let consumer = Consumer::new(&consumer_conf)?;
    consumer.assign(&[TopicPartition::new(&topic, 0, 0)])?;

    let deadline = Instant::now() + Duration::from_secs(10);
    let message = loop {
        if let Some(msg) = consumer.consume_native(250)? {
            break msg;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for consumed record"
        );
    };

    println!(
        "Consumed topic={} partition={} offset={} key={:?} payload={:?} headers={}",
        message.topic(),
        message.partition(),
        message.offset(),
        message.key().map(String::from_utf8_lossy),
        message.payload().map(String::from_utf8_lossy),
        message.headers().len(),
    );

    assert_eq!(message.key(), Some(&b"account-42"[..]));
    assert_eq!(message.payload(), Some(&b"order-created"[..]));
    assert_eq!(message.headers().len(), 3);

    consumer.close()?;
    Ok(())
}

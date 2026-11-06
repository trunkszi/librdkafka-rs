//! Public client integration and lifecycle scenarios using real native brokers.
use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use rdkafka::*;
static_assertions::assert_not_impl_any!(Conf:Send,Sync);
static_assertions::assert_not_impl_any!(Producer:Send,Sync);
static_assertions::assert_not_impl_any!(Consumer:Send,Sync);
static_assertions::assert_not_impl_any!(SimpleConsumer:Send,Sync);
static_assertions::assert_not_impl_any!(Queue:Send,Sync);
static_assertions::assert_not_impl_any!(Topic:Send,Sync);
static_assertions::assert_not_impl_any!(Headers:Send,Sync);
static_assertions::assert_not_impl_any!(TopicPartition:Send,Sync);
static_assertions::assert_not_impl_any!(Metadata:Send,Sync);
static_assertions::assert_not_impl_any!(ReceivedMessage:Send,Sync);
#[test]
fn integration_scenario_config_and_null_headers() -> Result<()> {
    let mut c = Conf::new();
    c.set("client.id", "config-test")?;
    assert_eq!(c.get("client.id")?, "config-test");
    assert!(
        c.dump()
            .iter()
            .any(|(k, v)| k == "client.id" && v == "config-test")
    );
    assert!(c.set("missing.property", "x").is_err());
    assert!(c.set("client.id", "truncated\0suffix").is_err());
    let mut t = Conf::topic();
    t.set("auto.offset.reset", "earliest")?;
    assert_eq!(t.get("auto.offset.reset")?, "smallest");
    let mut h = Headers::new();
    h.add("same", None).add("same", Some(&[]));
    assert_eq!(
        h.iter().map(|h| h.value.as_deref()).collect::<Vec<_>>(),
        vec![None, Some(&[][..])]
    );
    Ok(())
}
fn roundtrip(brokers: &str, codec: &str) -> Result<()> {
    let reports = Rc::new(RefCell::new(Vec::new()));
    let reports_cb = Rc::clone(&reports);
    let thread = std::thread::current().id();
    let mut c = Conf::new();
    c.set("bootstrap.servers", brokers)?
        .set("compression.codec", codec)?
        .set("message.timeout.ms", "10000")?;
    c.delivery_report_cb(move |r| {
        assert_eq!(thread, std::thread::current().id());
        reports_cb.borrow_mut().push(r.clone());
    });
    let p = Producer::new(&c)?;
    let topic = format!("core-{codec}-{}", std::process::id());
    let mut h = Headers::new();
    h.add("duplicate", None).add("duplicate", Some(b""));
    p.produce(ProduceRecord {
        topic: &topic,
        partition: 0,
        key: None,
        payload: Some(b"payload"),
        headers: Some(&h),
        timestamp: Some(1700000000000),
        token: 42,
    })?;
    p.flush(10000)?;
    assert_eq!(reports.borrow().len(), 1);
    assert_eq!(reports.borrow()[0].token, 42);
    assert_eq!(reports.borrow()[0].error, None);
    let mut c = Conf::new();
    c.set("bootstrap.servers", brokers)?
        .set("group.id", &format!("core-{codec}-{}", std::process::id()))?
        .set("enable.auto.commit", "false")?
        .set("auto.offset.reset", "earliest")?;
    let consumer = Consumer::new(&c)?;
    consumer.assign(&[TopicPartition::new(&topic, 0, 0)])?;
    let start = Instant::now();
    let m = loop {
        if let Some(m) = consumer.consume_native(500)? {
            break m;
        }
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "message deadline"
        );
    };
    assert_eq!(m.payload(), Some(&b"payload"[..]));
    assert_eq!(m.key(), None);
    assert_eq!(m.timestamp(), Some(1700000000000));
    assert_eq!(
        m.headers()
            .iter()
            .map(|h| h.value.as_deref())
            .collect::<Vec<_>>(),
        vec![None, Some(&[][..])]
    );
    consumer.close()?;
    drop(consumer);
    assert_eq!(m.payload(), Some(&b"payload"[..]));
    Ok(())
}
#[test]
#[ignore = "requires an explicitly provisioned Redpanda broker"]
fn integration_lifecycle_live_all_compressions() -> Result<()> {
    let brokers =
        std::env::var("RDKAFKA_TEST_BROKERS").map_err(|e| Error::Config(e.to_string()))?;
    for codec in ["none", "lz4", "snappy", "zstd", "gzip"] {
        roundtrip(&brokers, codec)?;
    }
    Ok(())
}

#[test]
fn integration_lifecycle_mock_all_compressions() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "3")?;
    let host = Producer::new(&conf)?;
    let brokers = host.mock_bootstraps();
    assert!(!brokers.is_empty());
    for codec in ["none", "lz4", "snappy", "zstd", "gzip"] {
        roundtrip(&brokers, codec)?;
    }
    Ok(())
}

#[test]
fn unit_scenario_lossless_errors_and_callback_conversion() {
    for n in [-199, -184, -150, 3, 100, 9999] {
        assert_eq!(ErrorCode(n).value(), n);
    }
    assert!(ErrorCode::QUEUE_FULL.is_queue_full());
    assert!(ErrorCode::QUEUE_FULL.is_retriable());
    assert!(ErrorCode::FATAL.is_fatal());
    let e = Error::from(anyhow::anyhow!("user observer failed"));
    assert!(matches!(e, Error::Callback(_)));
}

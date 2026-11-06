//! Partition EOF must keep the native payload error string.
use std::time::{Duration, Instant};

use rdkafka::{
    Conf, Consumer, Error, ErrorCode, Offset, ProduceRecord, Producer, Result, SimpleConsumer,
    Topic, TopicPartition,
};

fn host() -> Result<(Producer, String)> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "3")?;
    let producer = Producer::new(&conf)?;
    let brokers = producer.mock_bootstraps();
    Ok((producer, brokers))
}

fn publish(brokers: &str, topic: &str) -> Result<()> {
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", brokers)?;
    let producer = Producer::new(&conf)?;
    producer.produce(ProduceRecord {
        topic,
        partition: 0,
        key: None,
        payload: Some(b"only"),
        headers: None,
        timestamp: None,
        token: 1,
    })?;
    producer.flush(10_000)
}

fn assert_eof(error: &Error) {
    let text = error.to_string();
    assert_eq!(error.code(), Some(ErrorCode::PARTITION_EOF), "{text}");
    assert!(
        text.contains("reached end of partition"),
        "rkmessage payload errstr was dropped: {text}"
    );
}

#[test]
fn partition_eof_keeps_payload_errstr() -> Result<()> {
    let (_host, brokers) = host()?;
    let topic = format!("eof-simple-{}", std::process::id());
    publish(&brokers, &topic)?;
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", &brokers)?
        .set("enable.partition.eof", "true")?;
    let consumer = SimpleConsumer::new(&conf)?;
    let native = Topic::for_consumer(&consumer, &topic, &Conf::topic())?;
    consumer.start(&native, 0, Offset::Beginning)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut saw_record = false;
    loop {
        assert!(Instant::now() < deadline, "timed out before eof");
        match consumer.consume(&native, 0, 200) {
            Ok(Some(message)) => {
                assert_eq!(message.payload.as_deref(), Some(b"only".as_slice()));
                saw_record = true;
            }
            Ok(None) => {}
            Err(error) => {
                assert!(saw_record, "error before the record: {error}");
                assert_eof(&error);
                return Ok(());
            }
        }
    }
}

#[test]
fn group_eof_keeps_payload_errstr() -> Result<()> {
    let (_host, brokers) = host()?;
    let topic = format!("eof-group-{}", std::process::id());
    publish(&brokers, &topic)?;
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", &brokers)?
        .set("group.id", &topic)?
        .set("enable.auto.commit", "false")?
        .set("auto.offset.reset", "earliest")?
        .set("enable.partition.eof", "true")?;
    let consumer = Consumer::new(&conf)?;
    consumer.assign(&[TopicPartition::new(&topic, 0, 0)])?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut saw_record = false;
    loop {
        assert!(Instant::now() < deadline, "timed out before eof");
        match consumer.consume(200) {
            Ok(Some(message)) => {
                assert_eq!(message.payload.as_deref(), Some(b"only".as_slice()));
                saw_record = true;
            }
            Ok(None) => {}
            Err(error) => {
                assert!(saw_record, "error before the record: {error}");
                assert_eof(&error);
                return Ok(());
            }
        }
    }
}

#[test]
fn native_eof_keeps_payload_errstr() -> Result<()> {
    let (_host, brokers) = host()?;
    let topic = format!("eof-native-{}", std::process::id());
    publish(&brokers, &topic)?;
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", &brokers)?
        .set("group.id", &topic)?
        .set("enable.auto.commit", "false")?
        .set("auto.offset.reset", "earliest")?
        .set("enable.partition.eof", "true")?;
    let consumer = Consumer::new(&conf)?;
    consumer.assign(&[TopicPartition::new(&topic, 0, 0)])?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut saw_record = false;
    loop {
        assert!(Instant::now() < deadline, "timed out before eof");
        match consumer.consume_native(200) {
            Ok(Some(message)) => {
                assert_eq!(message.payload(), Some(b"only".as_slice()));
                saw_record = true;
            }
            Ok(None) => {}
            Err(error) => {
                assert!(saw_record, "error before the record: {error}");
                assert_eof(&error);
                return Ok(());
            }
        }
    }
}

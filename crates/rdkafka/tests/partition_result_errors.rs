use std::time::Duration;

use rdkafka::{Conf, Consumer, ProduceRecord, Producer, Result, TopicPartition};

#[test]
fn mixed_partition_pause_keeps_errors() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?;
    let host = Producer::new(&conf)?;
    let mut prime = ProduceRecord::new("partition-probe", Some(b"prime"));
    prime.partition = 0;
    host.produce(prime)?;
    host.flush(10000)?;
    let metadata = host.metadata(Some("partition-probe"), 10000)?;
    assert_eq!(metadata.brokers.len(), 1);
    assert!(
        metadata
            .topics
            .iter()
            .any(|topic| topic.name == "partition-probe"
                && topic.error == 0
                && topic.partitions.iter().any(|p| p.id == 0
                    && p.error == 0
                    && p.leader >= 0
                    && p.replicas.contains(&p.leader)))
    );
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", &host.mock_bootstraps())?
        .set("group.id", "partition-probe")?
        .set("enable.auto.commit", "false")?;
    let consumer = Consumer::new(&conf)?;
    consumer.assign(&[TopicPartition::new("partition-probe", 0, 0)])?;
    let selection = [
        TopicPartition::new("partition-probe", 0, 0),
        TopicPartition::new("unregistered-topic", 99, 0),
    ];
    let paused = consumer.pause(&selection)?;
    assert_eq!(paused[0].error, 0);
    assert_ne!(paused[1].error, 0);
    let resumed = consumer.resume(&selection)?;
    assert_eq!(resumed[0].error, 0);
    assert_ne!(resumed[1].error, 0);
    let sought = consumer.seek(&selection, Duration::from_millis(100))?;
    assert_eq!(sought[0].error, 0);
    assert_ne!(sought[1].error, 0);
    consumer.close()?;
    Ok(())
}

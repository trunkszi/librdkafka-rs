//! Administrative client validation, cooperative incremental rebalancing,
//! and transactional producer state machine lifecycle.
//!
//! Run with:
//!   cargo run -p rdkafka --example 04_admin_rebalance_transactions

use rdkafka::{
    AdminClient, Conf, Consumer, NewTopic, Offset, ProduceRecord, Producer, PurgeFlags, Result,
    TopicPartition,
};

fn main() -> Result<()> {
    // 1. Start a 3-broker native mock cluster and transactional producer.
    let mut tx_conf = Conf::new();
    tx_conf
        .set("test.mock.num.brokers", "3")?
        .set("transactional.id", "example-tx-producer")?
        .set("message.timeout.ms", "10000")?;
    let producer = Producer::new(&tx_conf)?;
    let brokers = producer.mock_bootstraps();
    let topic = format!("example-tx-topic-{}", std::process::id());

    // 2. Initialize transactions, commit one transaction, and abort a second one.
    producer.init_transactions(10_000)?;

    producer.begin_transaction()?;
    producer.produce(ProduceRecord {
        topic: &topic,
        partition: 0,
        key: None,
        payload: Some(b"committed-event"),
        headers: None,
        timestamp: None,
        token: 0,
    })?;
    producer.commit_transaction(10_000)?;

    producer.begin_transaction()?;
    producer.produce(ProduceRecord {
        topic: &topic,
        partition: 0,
        key: None,
        payload: Some(b"rolled-back-event"),
        headers: None,
        timestamp: None,
        token: 0,
    })?;
    producer.abort_transaction(10_000)?;

    producer.purge(PurgeFlags::ALL | PurgeFlags::NON_BLOCKING)?;
    producer.poll(0)?;
    println!("Transactional commit and abort completed cleanly.");

    // 3. Cooperative-sticky incremental partition assignment and unassignment.
    let mut consumer_conf = Conf::new();
    consumer_conf
        .set("bootstrap.servers", &brokers)?
        .set("group.id", "example-cooperative-group")?
        .set("partition.assignment.strategy", "cooperative-sticky")?;
    let consumer = Consumer::new(&consumer_conf)?;

    let p0 = TopicPartition::new(&topic, 0, Offset::Beginning.value());
    let p1 = TopicPartition::new(&topic, 1, Offset::Beginning.value());
    consumer.incremental_assign(&[p0, p1])?;
    println!(
        "After incremental_assign(p0, p1): {} partition(s) assigned",
        consumer.assignment()?.len()
    );
    assert_eq!(consumer.assignment()?.len(), 2);

    let p0_only = TopicPartition::new(&topic, 0, Offset::Beginning.value());
    consumer.incremental_unassign(&[p0_only])?;
    let remaining = consumer.assignment()?;
    println!(
        "After incremental_unassign(p0): remaining partition={}",
        remaining[0].partition
    );
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].partition, 1);

    let group_meta = consumer.consumer_group_metadata()?;
    drop(group_meta);
    consumer.close()?;

    // 4. AdminClient construction and synchronous validation against the cluster.
    let mut admin_conf = Conf::new();
    admin_conf.set("bootstrap.servers", &brokers)?;
    let admin = AdminClient::new(&admin_conf)?;
    assert!(
        admin
            .create_topics(&[NewTopic::new("invalid\0topic", 1, 1)], 100)
            .is_err()
    );
    admin.poll(0)?;
    println!("AdminClient validation check succeeded.");

    Ok(())
}

//! Native mock-broker transaction and cooperative assignment scenarios.
use rdkafka::{
    Conf, Consumer, ConsumerGroupMetadata, Offset, ProduceRecord, Producer, PurgeFlags, Result,
    TopicPartition,
};

static_assertions::assert_not_impl_any!(ConsumerGroupMetadata: Send, Sync);

#[test]
fn integration_lifecycle_transaction_states_and_owned_metadata() -> Result<()> {
    let mut config = Conf::new();
    config
        .set("test.mock.num.brokers", "3")?
        .set("transactional.id", "transaction-state-contract")?;
    let producer = Producer::new(&config)?;
    let error = producer
        .begin_transaction()
        .expect_err("initialization is required");
    assert!(error.code().is_some());
    assert!(!error.is_fatal());
    producer.init_transactions(10000)?;
    producer.begin_transaction()?;
    let state_error = producer
        .begin_transaction()
        .expect_err("a transaction is already open");
    assert!(!state_error.is_retriable());
    assert!(!state_error.is_fatal());
    assert!(!state_error.txn_requires_abort());
    let topic = format!("transaction-contract-{}", std::process::id());
    let send = || {
        producer.produce(ProduceRecord {
            topic: &topic,
            partition: 0,
            key: None,
            payload: Some(b"transactional"),
            headers: None,
            timestamp: None,
            token: 0,
        })
    };
    send()?;
    producer.commit_transaction(10000)?;
    producer.begin_transaction()?;
    send()?;
    producer.abort_transaction(10000)?;
    producer.begin_transaction()?;
    producer.commit_transaction(10000)?;
    producer.purge(PurgeFlags::ALL | PurgeFlags::NON_BLOCKING)?;
    producer.poll(0)?;
    assert_eq!(producer.outq_len(), 0);
    let mut config = Conf::new();
    config
        .set("bootstrap.servers", &producer.mock_bootstraps())?
        .set("group.id", "owned-generation-contract")?;
    let consumer = Consumer::new(&config)?;
    let metadata = consumer.consumer_group_metadata()?;
    consumer.close()?;
    drop(consumer);
    drop(metadata);
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
    Ok(())
}

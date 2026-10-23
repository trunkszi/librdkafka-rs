//! Transaction states, owned group metadata, and atomic record/offset commits.
use rdkafka::{Conf, Consumer, ConsumerGroupMetadata, ProduceRecord, Producer, PurgeFlags, Result};

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

#[path = "support/live_broker.rs"]
pub mod live_broker;

mod live {
    use std::time::{Duration, Instant};

    use rdkafka::*;

    use super::live_broker::*;

    #[test]
    #[ignore = "requires an explicitly provisioned transactional Redpanda broker"]
    fn e2e_lifecycle_live_atomic_transform_and_offsets() -> Result<()> {
        let brokers = brokers();
        let admin = admin(&brokers)?;
        let input = unique("atomic-input");
        let output = unique("atomic-output");
        let group = unique("atomic-group");
        topic(&admin, &input, 1)?;
        topic(&admin, &output, 1)?;
        let seed = producer(&brokers)?;
        send(&seed, &input, b"one")?;
        send(&seed, &input, b"two")?;
        seed.flush(TIMEOUT)?;
        let source = consumer(&brokers, &group)?;
        source.subscribe(&[&input])?;
        let first = message(&source)?;
        assert_eq!(first.payload.as_deref(), Some(&b"one"[..]));
        let mut conf = config(&brokers)?;
        conf.set("transactional.id", &unique("atomic-writer"))?;
        let tx = Producer::new(&conf)?;
        tx.init_transactions(TIMEOUT)?;
        let offset = TopicPartition::new(&input, 0, first.offset + 1);
        tx.begin_transaction()?;
        send(&tx, &output, b"aborted-one")?;
        tx.flush(TIMEOUT)?;
        tx.send_offsets_to_transaction(
            std::slice::from_ref(&offset),
            &source.consumer_group_metadata()?,
            TIMEOUT,
        )?;
        tx.abort_transaction(TIMEOUT)?;
        let committed = source.committed(std::slice::from_ref(&offset), Duration::from_secs(10))?;
        assert!(
            committed[0].offset < offset.offset,
            "aborted input offset became committed"
        );
        tx.begin_transaction()?;
        send(&tx, &output, b"ONE")?;
        tx.send_offsets_to_transaction(
            std::slice::from_ref(&offset),
            &source.consumer_group_metadata()?,
            TIMEOUT,
        )?;
        tx.commit_transaction(TIMEOUT)?;
        assert_eq!(
            source.committed(std::slice::from_ref(&offset), Duration::from_secs(10))?[0].offset,
            offset.offset
        );
        let second = message(&source)?;
        assert_eq!(second.payload.as_deref(), Some(&b"two"[..]));
        tx.begin_transaction()?;
        send(&tx, &output, b"TWO")?;
        let end = TopicPartition::new(&input, 0, second.offset + 1);
        tx.send_offsets_to_transaction(
            std::slice::from_ref(&end),
            &source.consumer_group_metadata()?,
            TIMEOUT,
        )?;
        tx.commit_transaction(TIMEOUT)?;
        assert_eq!(
            source.committed(std::slice::from_ref(&end), Duration::from_secs(10))?[0].offset,
            end.offset
        );
        source.close()?;
        let sink = consumer(&brokers, &unique("atomic-reader"))?;
        sink.assign(&[TopicPartition::new(&output, 0, 0)])?;
        assert_eq!(message(&sink)?.payload.as_deref(), Some(&b"ONE"[..]));
        assert_eq!(message(&sink)?.payload.as_deref(), Some(&b"TWO"[..]));
        let until = Instant::now() + Duration::from_secs(2);
        while Instant::now() < until {
            assert!(
                sink.consume(200)?.is_none(),
                "aborted transaction leaked or output duplicated"
            );
        }
        sink.close()?;
        success(admin.delete_topics(&[&input, &output], TIMEOUT)?);
        Ok(())
    }
}

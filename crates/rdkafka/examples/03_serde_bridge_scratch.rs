//! Macro-free `SerdeBridge` positional encoding, borrowed zero-copy decoding,
//! `DecodeLimits`, and caller-owned scratch buffer reuse across batch calls.
//!
//! Run with:
//!   cargo run -p rdkafka --example 03_serde_bridge_scratch

use std::time::{Duration, Instant};

use rdkafka::{
    BufferPolicy, Conf, Consumer, Producer, Result, SerdeBridge, TopicPartition, WireBatch,
    serde_bridge::DecodeLimits,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct OrderCommand<'a> {
    order_id: u64,
    #[serde(borrow)]
    symbol: &'a str,
    #[serde(with = "serde_bytes")]
    signature: &'a [u8],
    quantity: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct OwnedOrderCommand {
    order_id: u64,
    symbol: String,
    #[serde(with = "serde_bytes")]
    signature: Vec<u8>,
    quantity: u32,
}

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
    let topic = format!("example-serde-bridge-{}", std::process::id());

    // 1. Single-record borrowed round-trip with explicit `DecodeLimits`.
    let mut payload_buf = Vec::with_capacity(128);
    let command = OrderCommand {
        order_id: 9001,
        symbol: "ETH-PERP",
        signature: b"\xaa\xbb\xcc\xdd",
        quantity: 25,
    };
    SerdeBridge::encode_into(&command, &mut payload_buf)?;

    let limits = DecodeLimits {
        max_collection_elements: 1024,
        max_depth: 16,
    };
    let borrowed: OrderCommand<'_> =
        SerdeBridge::decode_borrowed_with_limits(&payload_buf, limits)?;
    assert_eq!(borrowed, command);
    // Verify that `symbol` and `signature` borrow directly from `payload_buf`.
    assert!(
        borrowed.symbol.as_ptr() >= payload_buf.as_ptr()
            && borrowed.symbol.as_ptr() < payload_buf.as_ptr().wrapping_add(payload_buf.len())
    );

    // 2. Batch production reusing caller-owned `WireBatch` and `Vec<u8>` scratch.
    let mut producer_conf = Conf::new();
    producer_conf
        .set("bootstrap.servers", &brokers)?
        .set("message.timeout.ms", "10000")?;
    let producer = Producer::new(&producer_conf)?;

    let commands = [
        OrderCommand {
            order_id: 1,
            symbol: "SOL-PERP",
            signature: b"sig-1",
            quantity: 10,
        },
        OrderCommand {
            order_id: 2,
            symbol: "BTC-PERP",
            signature: b"sig-2",
            quantity: 20,
        },
    ];

    let mut wire_scratch = WireBatch::with_capacity(8);
    let mut byte_scratch = Vec::with_capacity(256);
    let policy = BufferPolicy {
        max_records: 512,
        max_bytes: 32 * 1024,
    };

    let batch_result = producer.produce_batch_using_with_policy(
        &topic,
        &commands,
        &mut wire_scratch,
        &mut byte_scratch,
        policy,
        |item, scratch| SerdeBridge::encode_into(item, scratch).map(|_| ()),
    )?;
    assert_eq!(batch_result.accepted(), 2);
    producer.flush(10_000)?;

    // 3. Stream decoded serde records from the consumer without intermediate Vecs.
    let mut consumer_conf = Conf::new();
    consumer_conf
        .set("bootstrap.servers", &brokers)?
        .set("group.id", &topic)?
        .set("enable.auto.commit", "false")?
        .set("auto.offset.reset", "earliest")?;
    let consumer = Consumer::new(&consumer_conf)?;
    consumer.assign(&[TopicPartition::new(&topic, 0, 0)])?;

    let mut received_orders = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while received_orders.len() < 2 {
        consumer.consume_serde_with::<OwnedOrderCommand>(16, 250, |order| {
            received_orders.push(order);
            Ok(())
        })?;
        assert!(Instant::now() < deadline, "timed out consuming serde batch");
    }

    println!("Consumed serde batch: {received_orders:?}");
    assert_eq!(received_orders[0].order_id, 1);
    assert_eq!(received_orders[1].order_id, 2);

    consumer.close()?;
    Ok(())
}

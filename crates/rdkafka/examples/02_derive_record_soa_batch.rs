//! Typed `#[derive(Record)]`, Structure-of-Arrays (SoA) batching, `zerocopy`,
//! `BufferPolicy` arena retention, and vectorized native batch produce/consume.
//!
//! Run with:
//!   cargo run -p rdkafka --example 02_derive_record_soa_batch

use std::time::{Duration, Instant};

use rdkafka::{
    BufferPolicy, Conf, Consumer, Error, Producer, Record, Result, TopicPartition, WireBatch,
    WireRecord,
    record::{zerocopy_bytes, zerocopy_ref},
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[derive(Debug, PartialEq, Record)]
struct TradeEvent {
    #[record(key)]
    symbol: String,
    #[record(partition)]
    partition: i32,
    #[record(timestamp)]
    timestamp_ms: i64,
    #[record(header = "venue")]
    venue: Option<String>,
    price_ticks: i64,
    quantity: u32,
}

#[derive(Debug, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct QuoteTick {
    bid_le: [u8; 8],
    ask_le: [u8; 8],
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
    let topic = format!("example-soa-batch-{}", std::process::id());

    // 1. Direct zero-copy fixed-layout projection without serialization overhead.
    let tick = QuoteTick {
        bid_le: 10_125_u64.to_le_bytes(),
        ask_le: 10_130_u64.to_le_bytes(),
    };
    let raw_bytes = zerocopy_bytes(&tick);
    let projected = zerocopy_ref::<QuoteTick>(raw_bytes)?;
    assert_eq!(projected, &tick);

    // 2. Populate a generated Structure-of-Arrays (SoA) batch (`TradeEventBatch`).
    let mut outbound = TradeEventBatch::with_capacity(4);
    for idx in 0..3_i64 {
        outbound.push(&TradeEvent {
            symbol: format!("BTC-PERP-{idx}"),
            partition: 0,
            timestamp_ms: 1_700_000_000_000 + idx,
            venue: Some("CORE".to_owned()),
            price_ticks: 65_000 + idx * 10,
            quantity: (idx as u32 + 1) * 5,
        });
    }

    let mut producer_conf = Conf::new();
    producer_conf
        .set("bootstrap.servers", &brokers)?
        .set("message.timeout.ms", "10000")?;
    let producer = Producer::new(&producer_conf)?;

    let batch_result = producer.produce_batch(&topic, &outbound)?;
    assert!(batch_result.all_accepted());
    producer.flush(10_000)?;

    // 3. Consume native batches and decode into SoA columns while borrowing slices.
    let mut consumer_conf = Conf::new();
    consumer_conf
        .set("bootstrap.servers", &brokers)?
        .set("group.id", &topic)?
        .set("enable.auto.commit", "false")?
        .set("auto.offset.reset", "earliest")?;
    let consumer = Consumer::new(&consumer_conf)?;
    consumer.assign(&[TopicPartition::new(&topic, 0, 0)])?;

    let mut decoded = TradeEventBatch::with_capacity(4);
    let deadline = Instant::now() + Duration::from_secs(10);
    while decoded.len() < 3 {
        let received = consumer.consume_batch(16, 250)?;
        if !received.is_empty() {
            // Demonstrate `visit_from` retry capability across a simulated failure.
            let mut stop_at = None;
            let visit_outcome = received.visit_from(0, |index, _record| {
                if index == 1 {
                    stop_at = Some(index);
                    return Err(Error::Codec("simulated transient visitor pause".into()));
                }
                Ok(())
            });
            if let Some(failed_index) = stop_at {
                assert!(visit_outcome.is_err());
                received.visit_from(failed_index, |_index, _record| Ok(()))?;
            }
            received.decode_into(&mut decoded)?;
        }
        assert!(Instant::now() < deadline, "timed out receiving SoA batch");
    }

    println!(
        "Decoded SoA columns: symbols={:?} prices={:?} quantities={:?}",
        decoded.column_symbol(),
        decoded.column_price_ticks(),
        decoded.column_quantity(),
    );
    assert_eq!(decoded.column_price_ticks(), &[65_000, 65_010, 65_020]);

    // 4. Reuse arena buffers under an explicit `BufferPolicy`.
    let policy = BufferPolicy {
        max_records: 1024,
        max_bytes: 64 * 1024,
    };
    decoded.clear_with_policy(policy);
    let mut wire = WireBatch::with_capacity(8);
    wire.push(WireRecord::new(&topic, Some(raw_bytes)).with_partition(0))?;
    wire.clear_with_policy(policy);

    consumer.close()?;
    Ok(())
}

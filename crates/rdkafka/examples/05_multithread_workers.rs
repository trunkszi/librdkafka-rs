//! Multi-core scaling across OS threads using `ThreadLocalProducer` and
//! `WorkerProducer` (`WorkerBalance::KeyHash`, `RoundRobin`, and `WorkerSlotLease`).
//!
//! Run with:
//!   cargo run -p rdkafka --example 05_multithread_workers

use std::{sync::Arc, thread};

use rdkafka::{
    Conf, ProduceRecord, Producer, Result, ThreadLocalProducer, WorkerBalance, WorkerProducer,
};

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
    let topic = format!("example-workers-{}", std::process::id());

    // 1. `ThreadLocalProducer` is `Send + Sync` and lazily provisions one thread-affine `!Send +
    //    !Sync` `Producer` per OS thread.
    let brokers_tl = brokers.clone();
    let tl_producer = Arc::new(ThreadLocalProducer::new(move || {
        let mut conf = Conf::new();
        conf.set("bootstrap.servers", &brokers_tl)?
            .set("message.timeout.ms", "10000")?;
        Producer::new(&conf)
    })?);

    // 2. `WorkerProducer` provisions a local lock-free slot pool `0..N` per thread and routes calls
    //    via `RoundRobin`, `LeastInFlight`, or `KeyHash`.
    let worker_pool = Arc::new(WorkerProducer::new(
        2,
        WorkerBalance::KeyHash,
        move |slot| {
            let mut conf = Conf::new();
            conf.set("bootstrap.servers", &brokers)?
                .set("client.id", &format!("worker-slot-{slot}"))?
                .set("message.timeout.ms", "10000")?;
            Producer::new(&conf)
        },
    )?);

    let mut handles = Vec::new();
    for _ in 0..2_usize {
        let tl = Arc::clone(&tl_producer);
        let pool = Arc::clone(&worker_pool);
        let topic = topic.clone();

        handles.push(thread::spawn(move || -> Result<u64> {
            // Use the per-thread producer directly inside a closure.
            tl.with(|producer| {
                producer.produce(ProduceRecord {
                    topic: &topic,
                    partition: 0,
                    key: None,
                    payload: Some(b"from-thread-local"),
                    headers: None,
                    timestamp: None,
                    token: 0,
                })?;
                producer.flush(10_000)
            })?;

            // Acquire an explicit `!Send + !Sync` RAII slot lease routed by key hash.
            for key in [b"acct-alpha".as_slice(), b"acct-beta".as_slice()] {
                let lease = pool.lease_keyed(key)?;
                lease.produce(ProduceRecord {
                    topic: &topic,
                    partition: 0,
                    key: Some(key),
                    payload: Some(b"from-worker-slot"),
                    headers: None,
                    timestamp: None,
                    token: 0,
                })?;
                lease.flush(10_000)?;
            }

            let metrics = pool.metrics()?;
            let dispatched: u64 = metrics.iter().map(|m| m.dispatched).sum();
            pool.reset()?;
            tl.reset()?;
            Ok(dispatched)
        }));
    }

    for (idx, handle) in handles.into_iter().enumerate() {
        let dispatched = handle.join().expect("worker thread panicked")?;
        println!("OS thread {idx} completed {dispatched} routed slot lease dispatches");
        assert_eq!(dispatched, 2);
    }

    Ok(())
}

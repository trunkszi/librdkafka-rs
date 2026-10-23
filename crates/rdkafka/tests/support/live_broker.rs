//! Configuration, resource naming, and bounded operations for live Kafka fixtures.
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::*;

pub const TIMEOUT: i32 = 20000;
/// Read the explicitly provisioned broker endpoint.
/// # Panics
/// Panics when RDKAFKA_TEST_BROKERS is unset.
pub fn brokers() -> String {
    std::env::var("RDKAFKA_TEST_BROKERS")
        .expect("RDKAFKA_TEST_BROKERS must name an explicitly provisioned broker")
}
/// Create a process-unique resource name.
/// # Panics
/// Panics if the system clock precedes the Unix epoch.
pub fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
/// Build client configuration using the supplied bootstrap servers.
/// # Errors
/// Returns configuration or native broker operation errors.
pub fn config(brokers: &str) -> Result<Conf> {
    let mut c = Conf::new();
    c.set("bootstrap.servers", brokers)?;
    Ok(c)
}
/// Create an administrative client for provisioning test resources.
/// # Errors
/// Returns configuration or native broker operation errors.
pub fn admin(brokers: &str) -> Result<AdminClient> {
    AdminClient::new(&config(brokers)?)
}
/// Require every administrative resource result to succeed.
/// # Panics
/// Panics if any resource reports an error.
pub fn success(result: AdminResult) -> AdminResult {
    assert!(
        !result.has_errors(),
        "administrative resource failure: {result:?}"
    );
    result
}
/// Create a one-replica topic and require its resource result to succeed.
/// # Errors
/// Returns configuration or native broker operation errors.
/// # Panics
/// Panics if the topic resource reports an error.
pub fn topic(admin: &AdminClient, name: &str, count: i32) -> Result<()> {
    success(admin.create_topics(&[NewTopic::new(name, count, 1)], TIMEOUT)?);
    Ok(())
}
/// Create a producer using the supplied bootstrap servers.
/// # Errors
/// Returns configuration or native broker operation errors.
pub fn producer(brokers: &str) -> Result<Producer> {
    Producer::new(&config(brokers)?)
}
/// Create a read-committed consumer with explicit offset commits.
/// # Errors
/// Returns configuration or native broker operation errors.
pub fn consumer(brokers: &str, group: &str) -> Result<Consumer> {
    let mut c = config(brokers)?;
    c.set("group.id", group)?
        .set("enable.auto.commit", "false")?
        .set("auto.offset.reset", "earliest")?
        .set("isolation.level", "read_committed")?;
    Consumer::new(&c)
}
/// Enqueue a payload on partition zero.
/// # Errors
/// Returns configuration or native broker operation errors.
pub fn send(p: &Producer, topic: &str, payload: &[u8]) -> Result<()> {
    let mut record = ProduceRecord::new(topic, Some(payload));
    record.partition = 0;
    p.produce(record)
}
/// Receive the next record within a bounded deadline.
/// # Errors
/// Returns native consumer errors.
/// # Panics
/// Panics if no record arrives before the deadline.
pub fn message(c: &Consumer) -> Result<Message> {
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(message) = c.consume(250)? {
            return Ok(message);
        }
        assert!(Instant::now() < end, "record delivery deadline");
    }
}

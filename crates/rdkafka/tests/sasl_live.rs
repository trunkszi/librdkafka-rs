//! E2E security boundary: authenticated record delivery and readback through Redpanda.
#![cfg(feature = "tls")]
#[path = "fixtures/broker_security/server.rs"]
#[allow(dead_code)]
mod server;
use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rdkafka::{
    AdminClient, Conf, Consumer, Event, MsgStatus, NewTopic, ProduceRecord, Producer, TlsSettings,
};

#[test]
#[ignore = "requires test-broker-security.sh TLS/mTLS and SASL Redpanda listeners"]
fn e2e_lifecycle_scenario_security_delivery_and_readback() -> server::TestResult<()> {
    let mode = std::env::var("RDKAFKA_SECURITY_MODE")?;
    let endpoint = std::env::var("RDKAFKA_TEST_BROKERS")?;
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", &endpoint)?
        .set("message.timeout.ms", "20000")?;
    conf.set("security.protocol", &mode)?;
    let debug = std::env::var("RDKAFKA_SECURITY_DEBUG").ok();
    if let Some(debug) = &debug {
        conf.set("debug", debug)?;
    }
    conf.event_cb(move |event| {
        if matches!(event, Event::Error { .. }) || debug.is_some() {
            eprintln!("broker event: {event:?}");
        }
    });
    if mode.contains("SASL") {
        conf.set("sasl.mechanisms", &std::env::var("RDKAFKA_SASL_MECHANISM")?)?
            .set("sasl.username", "security-user")?
            .set("sasl.password", "security-password")?;
    }
    if mode.contains("SSL") {
        let directory = std::path::PathBuf::from(std::env::var("RDKAFKA_SECURITY_CERTS")?);
        let mut tls = TlsSettings::new()
            .with_native_roots(false)
            .with_ca_pem(std::fs::read(directory.join("ca.pem"))?)
            .with_server_name("localhost");
        if std::env::var_os("RDKAFKA_SECURITY_MTLS").is_some() {
            tls = tls.with_client_identity(
                std::fs::read(directory.join("client.pem"))?,
                std::fs::read(directory.join("client.key"))?,
            );
        }
        conf.tls_settings(tls)?;
    }
    let unique = format!(
        "security-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let admin = AdminClient::new(&conf)?;
    let result = admin.create_topics(&[NewTopic::new(&unique, 1, 1)], 20000)?;
    assert!(!result.has_errors(), "topic creation: {result:?}");
    let reports = Rc::new(RefCell::new(Vec::new()));
    let captured = Rc::clone(&reports);
    conf.delivery_report_cb(move |report| captured.borrow_mut().push(report.clone()));
    let producer = Producer::new(&conf)?;
    let payload = format!("verified-{unique}");
    let mut record = ProduceRecord::new(&unique, Some(payload.as_bytes()));
    record.partition = 0;
    producer.produce(record)?;
    producer.flush(20000)?;
    assert_eq!(reports.borrow().len(), 1);
    assert_eq!(reports.borrow()[0].error, None);
    assert_eq!(reports.borrow()[0].status, MsgStatus::Persisted);
    conf.set("group.id", &unique)?
        .set("auto.offset.reset", "earliest")?
        .set("enable.auto.commit", "false")?;
    let consumer = Consumer::new(&conf)?;
    consumer.subscribe(&[&unique])?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(message) = consumer.consume(250)? {
            assert_eq!(message.payload.as_deref(), Some(payload.as_bytes()));
            assert_eq!(message.offset, reports.borrow()[0].offset);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "authenticated record readback deadline"
        );
    }
    consumer.unsubscribe()?;
    drop(consumer);
    drop(producer);
    let deleted = admin.delete_topics(&[&unique], 20000)?;
    assert!(!deleted.has_errors(), "topic deletion: {deleted:?}");
    Ok(())
}

#[test]
#[ignore = "invoked by security harness to generate ephemeral rcgen identities"]
fn integration_scenario_security_generate_certificates() -> server::TestResult<()> {
    let path = std::path::PathBuf::from(std::env::var("RDKAFKA_SECURITY_CERTS")?);
    let identity = server::Identity::new()?;
    for (name, contents) in [
        ("ca.pem", identity.ca),
        ("server.pem", identity.cert),
        ("server.key", identity.key),
        ("client.pem", identity.client_cert),
        ("client.key", identity.client_key),
    ] {
        std::fs::write(path.join(name), contents)?;
    }
    Ok(())
}

//! Integration boundary: native Kafka metadata over a controlled rustls broker.
#![cfg(feature = "tls")]
#[path = "fixtures/broker_security/server.rs"]
mod server;
use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use rdkafka::{Conf, Error, Event, Producer, TlsSettings};
use server::{Server, TestResult};

fn config(endpoint: &str, tls: TlsSettings) -> rdkafka::Result<Conf> {
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", endpoint)?
        .set("security.protocol", "SSL")?
        .set("api.version.request", "false")?
        .set("broker.version.fallback", "0.8.2.2")?
        .set("socket.timeout.ms", "1000")?;
    conf.tls_settings(tls)?;
    Ok(conf)
}
fn trust(server: &Server) -> TlsSettings {
    TlsSettings::new()
        .with_native_roots(false)
        .with_ca_pem(server.identity.ca.clone())
        .with_server_name("fixture.test")
}
fn metadata(endpoint: &str, settings: TlsSettings) -> TestResult<()> {
    let producer = Producer::new(&config(endpoint, settings)?)?;
    let metadata = producer.metadata(None, 5000)?;
    assert_eq!(metadata.brokers.len(), 1);
    assert_eq!(metadata.brokers[0].id, 1);
    drop(producer);
    Ok(())
}
#[test]
fn integration_lifecycle_scenario_broker_tls12_tls13_ca_sni_and_mtls() -> TestResult<()> {
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        for mutual in [false, true] {
            let mut server = Server::new(version, mutual)?;
            let mut settings = trust(&server);
            if mutual {
                settings = settings.with_client_identity(
                    server.identity.client_cert.clone(),
                    server.identity.client_key.clone(),
                );
            }
            metadata(&server.address.to_string(), settings)?;
            let observed = server.finish()?;
            assert_eq!(observed.version, version.version);
            assert_eq!(observed.sni.as_deref(), Some("fixture.test"));
            assert_eq!(observed.client_authenticated, mutual);
            assert!(observed.metadata_requests > 0);
        }
    }
    Ok(())
}
#[test]
fn integration_scenario_broker_tls_rejects_bad_ca_hostname_and_missing_identity() -> TestResult<()>
{
    for failure in [
        "untrusted_ca",
        "hostname_mismatch",
        "missing_client_identity",
    ] {
        let mut server = Server::new(
            &rustls::version::TLS13,
            failure == "missing_client_identity",
        )?;
        let settings = match failure {
            "untrusted_ca" => TlsSettings::new()
                .with_native_roots(false)
                .with_ca_pem(server::Identity::new()?.ca)
                .with_server_name("fixture.test"),
            "hostname_mismatch" => trust(&server).with_server_name("wrong.test"),
            _ => trust(&server),
        };
        let events = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&events);
        let owner = std::thread::current().id();
        let mut conf = config(&server.address.to_string(), settings)?;
        conf.event_cb(move |event| {
            assert_eq!(owner, std::thread::current().id());
            if let Event::Error { code, message, .. } = event {
                captured.borrow_mut().push((*code, message.clone()));
            }
        });
        let producer = Producer::new(&conf)?;
        let error = producer
            .metadata(None, 1500)
            .expect_err("invalid TLS reached metadata");
        assert!(
            matches!(error, Error::Tls(_)),
            "{failure}: expected Error::Tls, got {error:?}"
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while !events
            .borrow()
            .iter()
            .any(|(_, message)| message.contains("TLS"))
            && Instant::now() < deadline
        {
            match producer.poll(20) {
                Err(Error::Tls(_)) => {}
                Err(error) => return Err(error.into()),
                Ok(_) => {}
            }
        }
        assert!(
            events
                .borrow()
                .iter()
                .any(|(_, message)| message.contains("TLS")),
            "{failure}: missing named TLS error: {:?}",
            events.borrow()
        );
        drop(producer);
        assert!(server.finish().is_err(), "{failure} reached Kafka metadata");
    }
    Ok(())
}
#[test]
fn integration_scenario_broker_tls_invalid_identity_is_structured_error() -> TestResult<()> {
    let server = Server::new(&rustls::version::TLS13, true)?;
    let other = server::Identity::new()?;
    let settings =
        trust(&server).with_client_identity(server.identity.client_cert.clone(), other.client_key);
    let mut conf = Conf::new();
    assert!(matches!(conf.tls_settings(settings), Err(Error::Tls(_))));
    Ok(())
}
#[test]
fn integration_lifecycle_scenario_broker_tls_native_roots() -> TestResult<()> {
    let mut server = Server::new(&rustls::version::TLS13, false)?;
    let path = std::env::temp_dir().join(format!(
        "rdkafka-native-roots-{}-{}",
        std::process::id(),
        server.address.port()
    ));
    std::fs::create_dir(&path)?;
    let ca = path.join("ca.pem");
    std::fs::write(&ca, &server.identity.ca)?;
    let result = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "integration_scenario_broker_tls_native_roots_child",
            "--ignored",
            "--nocapture",
        ])
        .env("SSL_CERT_FILE", &ca)
        .env("SSL_CERT_DIR", &path)
        .env("RDKAFKA_NATIVE_TLS_ENDPOINT", server.address.to_string())
        .status();
    std::fs::remove_dir_all(&path)?;
    assert!(result?.success(), "native root subprocess failed");
    assert!(server.finish()?.metadata_requests > 0);
    Ok(())
}
#[test]
#[ignore = "invoked by native-root parent with a process-local platform trust store"]
fn integration_scenario_broker_tls_native_roots_child() -> TestResult<()> {
    metadata(
        &std::env::var("RDKAFKA_NATIVE_TLS_ENDPOINT")?,
        TlsSettings::new().with_server_name("fixture.test"),
    )
}

#[test]
fn integration_lifecycle_scenario_broker_tls_accepts_sequential_clients() -> TestResult<()> {
    let mut server = Server::new(&rustls::version::TLS13, false)?;
    metadata(&server.address.to_string(), trust(&server))?;
    metadata(&server.address.to_string(), trust(&server))?;
    assert!(server.finish()?.metadata_requests >= 2);
    Ok(())
}

fn rejects_certificate(operation: &str) -> server::TestResult<()> {
    use rdkafka::{Consumer, Offset, Queue, SimpleConsumer, Topic, TopicPartition};
    let mut peer = server::Server::new(&rustls::version::TLS13, false)?;
    let unrelated = server::Identity::new()?;
    assert_ne!(peer.identity.ca, unrelated.ca);
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed = Rc::clone(&events);
    let caller = std::thread::current().id();
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", &peer.address.to_string())?
        .set("security.protocol", "SSL")?
        .set("api.version.request", "false")?
        .set("broker.version.fallback", "0.8.2.2")?;
    conf.tls_settings(
        TlsSettings::new()
            .with_native_roots(false)
            .with_ca_pem(unrelated.ca)
            .with_server_name("fixture.test"),
    )?;
    conf.event_cb(move |event| {
        assert_eq!(std::thread::current().id(), caller);
        if let Event::Error { message, .. } = event {
            observed.borrow_mut().push(message.clone());
        }
    });
    let result = match operation {
        "watermark" => Producer::new(&conf)?
            .query_watermark_offsets("events", 0, 1500)
            .map(|_| ()),
        "committed" => {
            conf.set("group.id", "tls-failure-propagation")?;
            Consumer::new(&conf)?
                .committed(
                    &[TopicPartition::new("events", 0, 0)],
                    Duration::from_millis(1500),
                )
                .map(|_| ())
        }
        "simple" => {
            let client = SimpleConsumer::new(&conf)?;
            let topic = Topic::for_consumer(&client, "events", &Conf::topic())?;
            client.start(&topic, 0, Offset::Beginning)?;
            let result = client.consume(&topic, 0, 1500).map(|_| ());
            client.stop(&topic, 0)?;
            result
        }
        "queue" => {
            let client = SimpleConsumer::new(&conf)?;
            let topic = Topic::for_consumer(&client, "events", &Conf::topic())?;
            let queue = Queue::new(&client)?;
            client.start_queue(&topic, 0, Offset::Beginning, &queue)?;
            let result = queue.consume(1500).map(|_| ());
            client.stop(&topic, 0)?;
            result
        }
        _ => unreachable!("controlled operation name"),
    };
    assert!(
        matches!(result, Err(Error::Tls(_))),
        "{operation}: {result:?}"
    );
    assert!(
        events
            .borrow()
            .iter()
            .any(|message| message.contains("TLS")),
        "{operation}: missing TLS event"
    );
    assert!(
        peer.finish().is_err(),
        "invalid certificate reached Kafka metadata"
    );
    Ok(())
}
#[test]
fn integration_scenario_watermark_preserves_tls_failure() -> server::TestResult<()> {
    rejects_certificate("watermark")
}
#[test]
fn integration_scenario_committed_preserves_tls_failure() -> server::TestResult<()> {
    rejects_certificate("committed")
}
#[test]
fn integration_scenario_simple_consume_preserves_tls_failure() -> server::TestResult<()> {
    rejects_certificate("simple")
}
#[test]
fn integration_scenario_queue_consume_preserves_tls_failure() -> server::TestResult<()> {
    rejects_certificate("queue")
}

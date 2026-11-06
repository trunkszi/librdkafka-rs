//! Real Confluent REST and framing round trips against an isolated Docker Redpanda.
#![cfg(feature = "http")]
use std::{
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rdkafka::{
    HttpTransport, TlsSettings,
    http::HttpRequest,
    schema_registry::{
        Schema, SchemaRegistryClient, SchemaType, decode_confluent, encode_confluent,
    },
};

struct Registry {
    endpoint: String,
    container: Option<String>,
}
impl Registry {
    fn open() -> Self {
        if let Ok(endpoint) = std::env::var("RDKAFKA_SCHEMA_REGISTRY_URL") {
            return Self {
                endpoint,
                container: None,
            };
        }
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("librdkafka-schema-{}-{suffix}", std::process::id());
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &name,
                "-p",
                "127.0.0.1::8081",
                "docker.redpanda.com/redpandadata/redpanda:latest",
                "redpanda",
                "start",
                "--overprovisioned",
                "--smp",
                "1",
                "--memory",
                "512M",
                "--reserve-memory",
                "0M",
                "--node-id",
                "0",
                "--check=false",
                "--kafka-addr",
                "0.0.0.0:9092",
                "--advertise-kafka-addr",
                "127.0.0.1:9092",
                "--schema-registry-addr",
                "0.0.0.0:8081",
            ])
            .output()
            .expect("Docker must be available for live Schema Registry acceptance");
        assert!(
            output.status.success(),
            "Redpanda startup: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut registry = Self {
            endpoint: String::new(),
            container: Some(name.clone()),
        };
        let port = Command::new("docker")
            .args(["port", &name, "8081/tcp"])
            .output()
            .unwrap();
        assert!(port.status.success());
        registry.endpoint = format!("http://{}", String::from_utf8(port.stdout).unwrap().trim());
        registry
    }
    fn ready(&self) {
        let mut transport = HttpTransport::new(TlsSettings::new()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            let mut request = HttpRequest::new("GET", format!("{}/subjects", self.endpoint));
            request.timeout = Duration::from_secs(1);
            match transport.execute(request) {
                Ok(response) if response.status == 200 => return,
                result if Instant::now() >= deadline => {
                    panic!(
                        "Schema Registry not ready within deadline: {}",
                        match result {
                            Ok(response) => format!("HTTP {}", response.status),
                            Err(error) => error.to_string(),
                        }
                    );
                }
                _ => std::thread::sleep(Duration::from_millis(100)),
            }
        }
    }
}
impl Drop for Registry {
    fn drop(&mut self) {
        if let Some(name) = &self.container {
            match Command::new("docker").args(["rm", "-f", name]).output() {
                Ok(output) if output.status.success() => {}
                Ok(_) => eprintln!("failed to remove owned Redpanda fixture {name}"),
                Err(error) => eprintln!("Redpanda cleanup command failed: {error}"),
            }
        }
    }
}

#[test]
fn e2e_lifecycle_schema_registry_register_lookup_fetch_and_frame() {
    let registry = Registry::open();
    registry.ready();
    let transport = HttpTransport::new(TlsSettings::new()).unwrap();
    let mut client = SchemaRegistryClient::new(transport, &registry.endpoint);
    let subject = format!(
        "round trip/{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = Schema::new(
        r#"{"type":"record","name":"Envelope","fields":[{"name":"value","type":"string"}]}"#,
        SchemaType::Avro,
    );
    let id = client.register_schema(&subject, &schema).unwrap();
    client.clear_cache();
    let fetched = client.get_schema_by_id(id).unwrap();
    assert_eq!(
        sonic_rs::from_str::<sonic_rs::Value>(&fetched.schema).unwrap(),
        sonic_rs::from_str::<sonic_rs::Value>(&schema.schema).unwrap()
    );
    let latest = client.get_latest_subject_schema(&subject).unwrap();
    assert_eq!(
        (latest.id, latest.subject.as_str(), latest.version),
        (id, subject.as_str(), 1)
    );
    client.clear_cache();
    assert_eq!(client.lookup_schema_id(&subject, &schema).unwrap(), id);
    let mut frame = Vec::with_capacity(64);
    encode_confluent(id, b"encoded-avro-payload", &mut frame).unwrap();
    assert_eq!(
        &frame[..5],
        &[
            0,
            id.to_be_bytes()[0],
            id.to_be_bytes()[1],
            id.to_be_bytes()[2],
            id.to_be_bytes()[3]
        ]
    );
    let (decoded_id, payload) = decode_confluent(&frame).unwrap();
    assert_eq!(decoded_id, id);
    assert_eq!(payload, b"encoded-avro-payload");
    assert_eq!(payload.as_ptr(), frame[5..].as_ptr());
    assert!(client.lookup_schema_id("missing subject", &schema).is_err());
}

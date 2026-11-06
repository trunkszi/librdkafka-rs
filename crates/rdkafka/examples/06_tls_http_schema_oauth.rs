//! Verified `TlsSettings`, single-threaded `HttpTransport`, Confluent Schema Registry
//! wire framing and caching (`SchemaRegistryClient`), and OAuth2 (`OAuthBearerHttpClient`).
//!
//! Run with:
//!   cargo run -p rdkafka --features http --example 06_tls_http_schema_oauth

use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use rdkafka::{
    Result, TlsSettings,
    http::{HttpPreferences, HttpTransport, HttpVersionChoice},
    oauth::{OAuthBearerConfig, OAuthBearerHttpClient, OAuthBearerToken, OAuthBearerTokenSetter},
    schema_registry::{
        Schema, SchemaRegistryClient, SchemaType, decode_confluent, encode_confluent,
    },
};

#[derive(Default)]
struct ExampleTokenTarget {
    access_token: Option<String>,
}

impl OAuthBearerTokenSetter for ExampleTokenTarget {
    fn oauthbearer_set_token(&mut self, token: &OAuthBearerToken) -> Result<()> {
        self.access_token = Some(token.access_token.clone());
        Ok(())
    }

    fn oauthbearer_set_token_failure(&mut self, _error: &str) -> Result<()> {
        self.access_token = None;
        Ok(())
    }
}

fn spawn_local_http_stub(responses: Vec<&'static str>) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let handle = thread::spawn(move || {
        for body in responses {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 2048];
            let _bytes_read = stream.read(&mut request).expect("read request");
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
                 {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(reply.as_bytes()).expect("write response");
        }
    });
    (format!("http://{addr}"), handle)
}

fn main() -> Result<()> {
    // 1. Build a verified `rustls::ClientConfig` from `TlsSettings`.
    let tls = TlsSettings::new().with_server_name("kafka.example.internal");
    let _rustls_config = tls.build_client_config()?;

    // 2. Confluent 5-byte wire framing (`0x00` + big-endian schema ID + payload).
    let mut frame = Vec::new();
    encode_confluent(42, b"encoded-avro-payload", &mut frame)?;
    let (schema_id, borrowed_payload) = decode_confluent(&frame)?;
    assert_eq!(schema_id, 42);
    assert_eq!(borrowed_payload, b"encoded-avro-payload");
    println!(
        "Confluent frame encoded: schema_id={schema_id}, payload_len={}",
        borrowed_payload.len()
    );

    // 3. SchemaRegistryClient registration + cache hit over `HttpTransport`.
    let (sr_url, sr_server) = spawn_local_http_stub(vec![r#"{"id":101}"#]);
    let prefs = HttpPreferences {
        version: HttpVersionChoice::Http1,
        ..HttpPreferences::default()
    };
    let sr_transport = HttpTransport::with_preferences(TlsSettings::default(), prefs)?;
    let mut registry = SchemaRegistryClient::new(sr_transport, sr_url);

    let schema = Schema::new(r#"{"type":"string"}"#, SchemaType::Avro);
    let first_id = registry.register_schema("orders-value", &schema)?;
    let cached_id = registry.register_schema("orders-value", &schema)?;
    let cached_schema = registry.get_schema_by_id(101)?;
    sr_server.join().expect("schema server join");
    assert_eq!(first_id, 101);
    assert_eq!(cached_id, 101);
    assert_eq!(cached_schema, schema);
    println!("Schema Registry registered and cached schema_id={first_id}");

    // 4. Synchronous OAuth2 client-credentials token refresh.
    let (oauth_url, oauth_server) = spawn_local_http_stub(vec![
        r#"{"access_token":"eyJ.example.token","expires_in":3600,"scope":"kafka.produce"}"#,
    ]);
    let mut oauth_cfg =
        OAuthBearerConfig::new(format!("{oauth_url}/token"), "service-client", "secret");
    oauth_cfg.allow_insecure_loopback_http = true;
    let oauth_transport = HttpTransport::with_preferences(TlsSettings::default(), prefs)?;
    let mut oauth_client = OAuthBearerHttpClient::new(oauth_transport, oauth_cfg);

    let mut target = ExampleTokenTarget::default();
    oauth_client.refresh(&mut target)?;
    oauth_server.join().expect("oauth server join");
    assert_eq!(target.access_token.as_deref(), Some("eyJ.example.token"));
    println!("OAuthBearer token refreshed synchronously on calling thread.");

    Ok(())
}

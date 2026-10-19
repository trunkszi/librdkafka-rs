#![cfg(feature = "http")]

use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};

use rdkafka::http::{HttpPreferences, HttpRequest, HttpTransport, HttpVersionChoice, TlsSettings};

static_assertions::assert_not_impl_any!(HttpTransport: Send, Sync);

fn server(response: &'static [u8]) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut bytes = [0; 4096];
        let mut request = Vec::new();
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = socket.read(&mut bytes).unwrap();
            if count == 0 {
                return;
            }
            request.extend_from_slice(&bytes[..count]);
        }
        // An over-limit client may close its socket before the complete reply.
        if let Err(error) = socket.write_all(response) {
            assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                ),
                "unexpected fixture write failure: {error}"
            );
        }
    });
    (format!("http://{address}/"), handle)
}

fn transport() -> HttpTransport {
    HttpTransport::with_preferences(
        TlsSettings::default(),
        HttpPreferences {
            version: HttpVersionChoice::Http1,
            ..HttpPreferences::default()
        },
    )
    .unwrap()
}

#[test]
fn integration_response_status_headers_and_bounded_body() {
    let (url, server) = server(b"HTTP/1.1 409 Conflict\r\nContent-Length: 4\r\nX-Test: actual\r\nConnection: close\r\n\r\nbody");
    let mut request = HttpRequest::new("GET", url);
    request.max_response_bytes = 4;
    let response = transport().execute(request).unwrap();
    assert_eq!(response.status, 409);
    assert_eq!(response.body, b"body");
    assert!(
        response
            .headers
            .iter()
            .any(|(key, value)| key.eq_ignore_ascii_case("x-test") && value == "actual")
    );
    assert_eq!(response.version, Some(HttpVersionChoice::Http1));
    server.join().unwrap();
}

#[test]
fn integration_scenario_body_overflow_is_rejected() {
    let (url, server) =
        server(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n12345");
    let mut request = HttpRequest::new("GET", url);
    request.max_response_bytes = 4;
    assert!(
        transport()
            .execute(request)
            .unwrap_err()
            .to_string()
            .contains("size limit")
    );
    server.join().unwrap();
}

#[test]
fn unit_scenario_request_injection_and_unbounded_timeout_are_rejected() {
    let mut client = transport();
    let mut request = HttpRequest::new("GET", "http://127.0.0.1:1/");
    request.timeout = Duration::ZERO;
    assert!(
        client
            .execute(request)
            .unwrap_err()
            .to_string()
            .contains("timeout")
    );
    let request = HttpRequest::new("GET\r\nX-Injected: yes", "http://127.0.0.1:1/");
    assert!(
        client
            .execute(request)
            .unwrap_err()
            .to_string()
            .contains("method")
    );
    let request = HttpRequest::new("GET", "http://user:secret@127.0.0.1:1/");
    let error = client.execute(request).unwrap_err().to_string();
    assert!(!error.contains("secret"));
    assert!(error.contains("credentials"));
}

#[test]
fn unit_scenario_tls_requires_trust_and_paired_credentials() {
    let tls = TlsSettings::default().with_native_roots(false);
    assert!(tls.build_client_config().is_err());
    let mut tls = TlsSettings {
        client_key_pem: b"private-secret".to_vec(),
        ..TlsSettings::default()
    };
    assert!(!format!("{tls:?}").contains("private-secret"));
    assert!(tls.build_client_config().is_err());
    tls.client_key_pem.clear();
    tls.danger_accept_invalid_certs = true;
    assert!(tls.build_client_config().is_err());
}

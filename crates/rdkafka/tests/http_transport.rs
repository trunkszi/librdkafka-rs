#![cfg(feature = "http")]

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
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

#[test]
fn unit_scenario_oversized_timeouts_fail_before_url_parsing_or_resolution() {
    let mut client = transport();
    for timeout in [
        Duration::from_millis(i32::MAX as u64) + Duration::from_nanos(1),
        Duration::MAX,
    ] {
        let mut request = HttpRequest::new("GET", "invalid URL");
        request.timeout = timeout;
        let error = client.execute(request).unwrap_err();
        assert!(matches!(error, rdkafka::Error::Http(_)));
        assert!(error.to_string().contains("timeout exceeds maximum"));
    }
}

#[test]
fn integration_scenario_tls_override_preserves_two_original_origins_on_the_same_port() {
    let mut fixture = TwoOrigins::new();
    assert_eq!(fixture.addresses[0].port(), fixture.addresses[1].port());
    let tls = TlsSettings::default()
        .with_native_roots(false)
        .with_ca_pem(fixture.ca_pem.clone())
        .with_server_name("fixture.test");
    let mut client = HttpTransport::with_preferences(
        tls,
        HttpPreferences {
            version: HttpVersionChoice::Http1,
            ..HttpPreferences::default()
        },
    )
    .unwrap();
    for (index, address) in fixture.addresses.iter().enumerate() {
        let response = client
            .execute(HttpRequest::new("GET", format!("https://{address}/")))
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, format!("origin-{index}").as_bytes());
    }
    drop(client);
    let observed = fixture.finish();
    for (index, requests) in observed.iter().enumerate() {
        assert_eq!(requests.len(), 1);
        assert!(requests[0].lines().any(|line| {
            line.eq_ignore_ascii_case(&format!("Host: {}", fixture.addresses[index]))
        }));
    }
}

struct TwoOrigins {
    addresses: [SocketAddr; 2],
    ca_pem: Vec<u8>,
    stop: Arc<AtomicBool>,
    threads: Vec<thread::JoinHandle<Vec<String>>>,
}

impl TwoOrigins {
    fn new() -> Self {
        Self::with_alpn(vec![b"http/1.1".to_vec()])
    }

    fn with_alpn(alpn: Vec<Vec<u8>>) -> Self {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};

        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let key = KeyPair::generate().unwrap();
        let certificate = CertificateParams::new(vec!["fixture.test".into()])
            .unwrap()
            .signed_by(&key, &ca, &ca_key)
            .unwrap();
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
        .unwrap();
        config.alpn_protocols = alpn;
        let config = Arc::new(config);
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = first.local_addr().unwrap().port();
        let second = TcpListener::bind(("127.0.0.2", port)).unwrap();
        let addresses = [first.local_addr().unwrap(), second.local_addr().unwrap()];
        let stop = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();
        for (index, listener) in [first, second].into_iter().enumerate() {
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::clone(&stop);
            let config = Arc::clone(&config);
            threads.push(thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(10);
                let mut requests = Vec::new();
                while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                    let socket = match listener.accept() {
                        Ok((socket, _)) => socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(error) => panic!("fixture accept failed: {error}"),
                    };
                    socket
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    socket
                        .set_write_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    let mut stream = rustls::StreamOwned::new(
                        rustls::ServerConnection::new(Arc::clone(&config)).unwrap(),
                        socket,
                    );
                    // Serve repeated requests so accidental cache reuse reaches the wrong origin.
                    while let Some(request) = read_tls_request(&mut stream) {
                        requests.push(request);
                        let body = format!("origin-{index}");
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: \
                             keep-alive\r\n\r\n{body}",
                            body.len()
                        )
                        .unwrap();
                        stream.flush().unwrap();
                    }
                }
                requests
            }));
        }
        Self {
            addresses,
            ca_pem: ca.pem().into_bytes(),
            stop,
            threads,
        }
    }

    fn finish(&mut self) -> Vec<Vec<String>> {
        self.stop.store(true, Ordering::Relaxed);
        self.threads
            .drain(..)
            .map(|thread| thread.join().unwrap())
            .collect()
    }
}

impl Drop for TwoOrigins {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads.drain(..) {
            if thread.join().is_err() && !thread::panicking() {
                panic!("TLS origin fixture panicked");
            }
        }
    }
}

fn read_tls_request(stream: &mut impl Read) -> Option<String> {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        match stream.read(&mut byte) {
            Ok(0) => {
                assert!(request.is_empty(), "incomplete fixture request");
                return None;
            }
            Ok(_) => request.push(byte[0]),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                assert!(request.is_empty(), "incomplete fixture request");
                return None;
            }
            Err(error) => panic!("fixture request read failed: {error}"),
        }
        assert!(
            request.len() <= 4096,
            "fixture request exceeds header limit"
        );
    }
    Some(String::from_utf8(request).unwrap())
}

#[test]
fn integration_scenario_tls_override_preserves_explicit_host_header() {
    let mut fixture = TwoOrigins::new();
    let tls = TlsSettings::default()
        .with_native_roots(false)
        .with_ca_pem(fixture.ca_pem.clone())
        .with_server_name("fixture.test");
    let mut client = HttpTransport::with_preferences(
        tls,
        HttpPreferences {
            version: HttpVersionChoice::Http1,
            ..HttpPreferences::default()
        },
    )
    .unwrap();
    let mut request = HttpRequest::new("GET", format!("https://{}/", fixture.addresses[0]));
    request
        .headers
        .push(("hOsT".into(), "application.test".into()));
    assert_eq!(client.execute(request).unwrap().body, b"origin-0");
    drop(client);
    let observed = fixture.finish();
    assert_eq!(observed[0].len(), 1);
    let hosts = observed[0][0]
        .lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("host"))
        .map(|(_, value)| value.trim())
        .collect::<Vec<_>>();
    assert_eq!(hosts, ["application.test"]);
    assert!(observed[1].is_empty());
}

#[test]
fn integration_scenario_cleartext_http2_reports_prior_knowledge() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut preface = [0; 24];
        socket.read_exact(&mut preface).unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        write_h2_frame(&mut socket, 4, 0, 0, &[]);
        loop {
            let mut header = [0; 9];
            socket.read_exact(&mut header).unwrap();
            let length = (usize::from(header[0]) << 16)
                | (usize::from(header[1]) << 8)
                | usize::from(header[2]);
            assert!(length <= 16 * 1024);
            let mut payload = vec![0; length];
            socket.read_exact(&mut payload).unwrap();
            let stream_id = u32::from_be_bytes(header[5..9].try_into().unwrap());
            if header[3] == 4 && header[4] & 1 == 0 {
                write_h2_frame(&mut socket, 4, 1, 0, &[]);
            }
            if header[3] == 1 && header[4] & 1 != 0 {
                write_h2_frame(&mut socket, 1, 4, stream_id, &[0x88]);
                write_h2_frame(&mut socket, 0, 1, stream_id, b"h2c");
                break;
            }
        }
        let mut drain = [0; 1024];
        loop {
            match socket.read(&mut drain) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) => panic!("HTTP/2 fixture drain failed: {error}"),
            }
        }
    });
    let mut client = HttpTransport::with_preferences(
        TlsSettings::default(),
        HttpPreferences {
            version: HttpVersionChoice::Http2PriorKnowledge,
            ..HttpPreferences::default()
        },
    )
    .unwrap();
    let response = client
        .execute(HttpRequest::new("GET", format!("http://{address}/")))
        .unwrap();
    assert_eq!(response.body, b"h2c");
    assert_eq!(
        response.version,
        Some(HttpVersionChoice::Http2PriorKnowledge)
    );
    drop(client);
    server.join().unwrap();
}

fn write_h2_frame(socket: &mut impl Write, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    let length = (payload.len() as u32).to_be_bytes();
    socket
        .write_all(&[length[1], length[2], length[3], kind, flags])
        .unwrap();
    socket.write_all(&stream.to_be_bytes()).unwrap();
    socket.write_all(payload).unwrap();
    socket.flush().unwrap();
}

#[test]
fn integration_lifecycle_keepalive_policy_controls_connection_reuse() {
    for keep_alive in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let peer = thread::spawn(move || {
            let mut connections = 0;
            let mut requests = 0;
            while requests < 2 {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                connections += 1;
                while let Some(request) = read_tls_request(&mut stream) {
                    assert!(request.starts_with("GET / HTTP/1.1\r\n"));
                    stream
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx")
                        .unwrap();
                    requests += 1;
                    if !keep_alive {
                        let mut byte = [0];
                        assert_eq!(
                            stream.read(&mut byte).unwrap(),
                            0,
                            "nonpersistent socket stayed open"
                        );
                        break;
                    }
                }
            }
            connections
        });
        let mut transport = HttpTransport::with_preferences(
            TlsSettings::default(),
            HttpPreferences {
                version: HttpVersionChoice::Http1Only,
                keep_alive,
                ..HttpPreferences::default()
            },
        )
        .unwrap();
        for _ in 0..2 {
            assert_eq!(
                transport
                    .execute(HttpRequest::new("GET", format!("http://{address}/")))
                    .unwrap()
                    .body,
                b"x"
            );
        }
        drop(transport);
        assert_eq!(peer.join().unwrap(), if keep_alive { 1 } else { 2 });
    }
}

#[test]
fn integration_scenario_response_header_budget_is_independent_of_body_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        assert!(read_tls_request(&mut stream).is_some());
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-Padding: {}\r\n\r\n",
            "x".repeat(65_536)
        );
        if let Err(error) = stream.write_all(response.as_bytes()) {
            assert!(matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ));
        }
    });
    let mut request = HttpRequest::new("GET", format!("http://{address}/"));
    request.max_response_bytes = 0;
    assert!(
        transport()
            .execute(request)
            .unwrap_err()
            .to_string()
            .contains("HTTP response headers exceed configured size limit")
    );
    peer.join().unwrap();
}

#[test]
fn integration_lifecycle_scenario_timeout_retires_the_pending_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        assert!(read_tls_request(&mut stream).is_some());
        let mut byte = [0];
        assert_eq!(
            stream.read(&mut byte).unwrap(),
            0,
            "timed-out request retained its socket"
        );
    });
    let mut request = HttpRequest::new("GET", format!("http://{address}/"));
    request.timeout = Duration::from_millis(100);
    let start = Instant::now();
    let error = transport().execute(request).unwrap_err();
    assert!(error.to_string().contains("timed out"));
    assert!(start.elapsed() < Duration::from_secs(3));
    peer.join().unwrap();
}

#[test]
fn integration_scenario_required_http2_rejects_missing_alpn_while_negotiation_accepts_http1() {
    for version in [
        HttpVersionChoice::Http2Tls,
        HttpVersionChoice::Http2PriorKnowledge,
        HttpVersionChoice::Negotiate,
    ] {
        let mut fixture = TwoOrigins::with_alpn(Vec::new());
        let settings = TlsSettings::new()
            .with_native_roots(false)
            .with_ca_pem(fixture.ca_pem.clone())
            .with_server_name("fixture.test");
        let mut transport = HttpTransport::with_preferences(
            settings,
            HttpPreferences {
                version,
                ..HttpPreferences::default()
            },
        )
        .unwrap();
        let response = transport.execute(HttpRequest::new(
            "GET",
            format!("https://{}/", fixture.addresses[0]),
        ));
        if version != HttpVersionChoice::Negotiate {
            assert!(
                matches!(response, Err(rdkafka::Error::Http(ref message)) if message.contains("HTTP/2 was required"))
            );
        } else {
            let response = response.unwrap();
            assert_eq!(response.version, Some(HttpVersionChoice::Http1));
            assert_eq!(response.body, b"origin-0");
        }
        drop(transport);
        assert_eq!(
            fixture.finish()[0].len(),
            usize::from(version == HttpVersionChoice::Negotiate)
        );
    }
}

#[test]
fn integration_scenario_hostname_sni_override_preserves_original_http1_host() {
    let mut fixture = TwoOrigins::new();
    let settings = TlsSettings::new()
        .with_native_roots(false)
        .with_ca_pem(fixture.ca_pem.clone())
        .with_server_name("fixture.test");
    let mut client = HttpTransport::with_preferences(
        settings,
        HttpPreferences {
            version: HttpVersionChoice::Http1,
            ..HttpPreferences::default()
        },
    )
    .unwrap();
    let authority = format!("localhost:{}", fixture.addresses[0].port());
    let mut request = HttpRequest::new("GET", format!("https://{authority}/"));
    request.timeout = Duration::from_secs(3);
    assert_eq!(client.execute(request).unwrap().body, b"origin-0");
    drop(client);
    let observed = fixture.finish();
    assert_eq!(observed[0].len(), 1);
    assert!(
        observed[0][0]
            .lines()
            .any(|line| line.eq_ignore_ascii_case(&format!("host: {authority}")))
    );
}

#[test]
fn integration_scenario_negotiated_http1_preserves_original_host_on_its_first_request() {
    let mut fixture = TwoOrigins::new();
    let settings = TlsSettings::new()
        .with_native_roots(false)
        .with_ca_pem(fixture.ca_pem.clone())
        .with_server_name("fixture.test");
    let mut client = HttpTransport::new(settings).unwrap();
    let authority = fixture.addresses[0].to_string();
    let response = client
        .execute(HttpRequest::new("GET", format!("https://{authority}/")))
        .unwrap();
    assert_eq!(response.version, Some(HttpVersionChoice::Http1));
    assert_eq!(response.body, b"origin-0");
    drop(client);
    let observed = fixture.finish();
    assert_eq!(observed[0].len(), 1);
    assert!(
        observed[0][0]
            .lines()
            .any(|line| line.eq_ignore_ascii_case(&format!("host: {authority}")))
    );
}

#[test]
fn integration_scenario_upload_and_receiving_response_use_independent_budgets() {
    for (budget, body) in [
        (0, Some(b"".as_slice())),
        (2, Some(b"ok".as_slice())),
        (1, None),
    ] {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let peer = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut stream = rustls::StreamOwned::new(
                rustls::ServerConnection::new(Arc::new(config)).unwrap(),
                socket,
            );
            let Some(headers) = read_tls_request(&mut stream) else {
                return Vec::new();
            };
            assert!(headers.starts_with("POST / HTTP/1.1\r\n"));
            let mut upload = vec![0; 5];
            stream.read_exact(&mut upload).unwrap();
            let size = body.map_or(2, <[u8]>::len);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            if let Some(body) = body {
                stream.write_all(body).unwrap();
            }
            stream.flush().unwrap();
            if body.is_none() {
                // No body is sent: a receive limit must fail at the oversized
                // declared length, without waiting for the request deadline.
                let mut byte = [0];
                match stream.read(&mut byte) {
                    Ok(0) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {}
                    other => panic!("oversized response was not rejected: {other:?}"),
                }
            }
            upload
        });
        let mut client = HttpTransport::with_preferences(
            TlsSettings::new()
                .with_native_roots(false)
                .with_ca_pem(cert.cert.pem())
                .with_server_name("localhost"),
            HttpPreferences {
                version: HttpVersionChoice::Http1Only,
                ..HttpPreferences::default()
            },
        )
        .unwrap();
        let mut request = HttpRequest::new("POST", format!("https://{address}/"));
        request.body = b"12345".to_vec();
        request.max_response_bytes = budget;
        request.timeout = Duration::from_secs(1);
        let response = client.execute(request);
        drop(client);
        let upload = peer.join().unwrap();
        assert_eq!(
            upload, b"12345",
            "response budget incorrectly limited upload"
        );
        if let Some(body) = body {
            assert_eq!(response.unwrap().body, body);
        } else {
            assert!(
                response
                    .unwrap_err()
                    .to_string()
                    .contains("HTTP response body exceeds configured size limit")
            );
        }
    }
}

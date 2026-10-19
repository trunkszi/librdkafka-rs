use std::{
    io::{Read, Write},
    net::TcpListener,
    os::{
        fd::{FromRawFd, IntoRawFd},
        unix::net::UnixStream,
    },
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use rustls::{ServerConfig, ServerConnection, StreamOwned};

use super::new;
use crate::TlsSettings;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

struct Fixture {
    settings: TlsSettings,
    port: u16,
    server: JoinHandle<TestResult>,
    release: std::sync::mpsc::Sender<()>,
}

impl Fixture {
    fn start(version: &'static rustls::SupportedProtocolVersion, mutual: bool) -> Self {
        Self::start_mode(version, mutual, false)
    }

    fn start_mode(
        version: &'static rustls::SupportedProtocolVersion,
        mutual: bool,
        stalled: bool,
    ) -> Self {
        let ca_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = params.self_signed(&ca_key).unwrap();
        let server_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["broker.test".into()]).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_cert = params.signed_by(&server_key, &ca, &ca_key).unwrap();
        let client_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["client.test".into()]).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_cert = params.signed_by(&client_key, &ca, &ca_key).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[version])
            .unwrap();
        let builder = if mutual {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(ca.der().clone()).unwrap();
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .unwrap();
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        };
        let config = builder
            .with_single_cert(
                vec![server_cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (release, resume) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;
            socket.set_write_timeout(Some(Duration::from_secs(5)))?;
            let mut stream = StreamOwned::new(ServerConnection::new(Arc::new(config))?, socket);
            if stalled {
                while stream.conn.is_handshaking() {
                    stream.conn.complete_io(&mut stream.sock)?;
                }
                resume.recv_timeout(Duration::from_secs(5))?;
                return Ok(());
            }
            let mut payload = vec![0; 180_000];
            stream.read_exact(&mut payload)?;
            if mutual
                && stream
                    .conn
                    .peer_certificates()
                    .is_none_or(|chain| chain.is_empty())
            {
                return Err("missing authenticated client identity".into());
            }
            if stream.conn.protocol_version() != Some(version.version) {
                return Err("unexpected negotiated TLS version".into());
            }
            stream.write_all(&payload)?;
            stream.conn.send_close_notify();
            stream.flush()?;
            Ok(())
        });
        Self {
            settings: TlsSettings::new()
                .with_native_roots(false)
                .with_ca_pem(ca.pem())
                .with_server_name("broker.test")
                .with_client_identity(client_cert.pem(), client_key.serialize_pem()),
            port,
            server,
            release,
        }
    }
}

#[test]
fn integration_lifecycle_scenario_verified_tls12_tls13_sni_mutual_and_backpressure() {
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let fixture = Fixture::start(version, true);
        let bridge = new(&fixture.settings).unwrap();
        let (placeholder, unused_peer) = UnixStream::pair().unwrap();
        let fd = placeholder.into_raw_fd();
        assert_eq!(
            bridge.connect(fd, "127.0.0.1", fixture.port, "wrong-name:9092"),
            0
        );
        drop(unused_peer);
        // SAFETY: ownership of the placeholder descriptor was transferred above.
        let mut socket = unsafe { UnixStream::from_raw_fd(fd) };
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let payload: Vec<_> = (0..180_000).map(|index| (index % 251) as u8).collect();
        socket.write_all(&payload).unwrap();
        let mut echoed = vec![0; payload.len()];
        socket.read_exact(&mut echoed).unwrap();
        assert_eq!(echoed, payload);
        let mut after_close = [0];
        assert_eq!(
            socket.read(&mut after_close).unwrap(),
            0,
            "clean TLS close must not replay plaintext"
        );
        assert_eq!(bridge.close(socket.into_raw_fd()), 0);
        fixture.server.join().unwrap().unwrap();
        assert!(bridge.errors().is_empty());
    }
}

#[test]
fn integration_scenario_untrusted_ca_and_hostname_reject_before_native_replacement() {
    for wrong_name in [false, true] {
        let mut fixture = Fixture::start(&rustls::version::TLS13, false);
        if wrong_name {
            fixture.settings.server_name = Some("wrong.test".into());
        } else {
            let other_ca = rcgen::generate_simple_self_signed(vec!["other.test".into()]).unwrap();
            fixture.settings.ca_pem = other_ca.cert.pem().into_bytes();
        }
        let bridge = new(&fixture.settings).unwrap();
        let (mut original, mut peer) = UnixStream::pair().unwrap();
        let fd = original.into_raw_fd();
        assert_ne!(
            bridge.connect(fd, "127.0.0.1", fixture.port, "broker.test:9092"),
            0
        );
        // SAFETY: ownership of the original descriptor was transferred above.
        original = unsafe { UnixStream::from_raw_fd(fd) };
        peer.write_all(b"unchanged").unwrap();
        let mut data = [0; 9];
        original.read_exact(&mut data).unwrap();
        assert_eq!(&data, b"unchanged");
        assert_eq!(bridge.errors().len(), 1);
        assert_eq!(bridge.close(original.into_raw_fd()), 0);
        assert!(fixture.server.join().unwrap().is_err());
    }
}

#[test]
fn integration_lifecycle_scenario_fork_isolates_failures_and_drop_stops_live_session() {
    let fixture = Fixture::start(&rustls::version::TLS13, false);
    let bridge = new(&fixture.settings).unwrap();
    let isolated = bridge.fork();
    assert_ne!(
        bridge.connect(-1, "invalid", fixture.port, "broker.test:9092"),
        0
    );
    assert_eq!(bridge.errors().len(), 1);
    assert!(isolated.errors().is_empty());
    let (placeholder, peer) = UnixStream::pair().unwrap();
    let fd = placeholder.into_raw_fd();
    assert_eq!(
        isolated.connect(fd, "127.0.0.1", fixture.port, "broker.test:9092"),
        0
    );
    drop(isolated);
    // SAFETY: the native endpoint remains caller-owned after the bridge is dropped.
    let socket = unsafe { UnixStream::from_raw_fd(fd) };
    drop(socket);
    drop(peer);
    assert!(fixture.server.join().unwrap().is_err());
}

#[test]
fn integration_scenario_missing_mutual_identity_closes_bridge_and_reports_failure() {
    let mut fixture = Fixture::start(&rustls::version::TLS13, true);
    fixture.settings.client_cert_pem.clear();
    fixture.settings.client_key_pem.clear();
    let bridge = new(&fixture.settings).unwrap();
    let (placeholder, peer) = UnixStream::pair().unwrap();
    let fd = placeholder.into_raw_fd();
    let result = bridge.connect(fd, "127.0.0.1", fixture.port, "broker.test:9092");
    // SAFETY: ownership of the placeholder descriptor was transferred above.
    let mut socket = unsafe { UnixStream::from_raw_fd(fd) };
    if result == 0 {
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).unwrap(), 0);
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let failure = loop {
        if let Some(failure) = bridge.errors().into_iter().next() {
            break failure;
        }
        assert!(std::time::Instant::now() < deadline, "missing TLS failure");
        std::thread::yield_now();
    };
    assert!(
        failure.contains("certificate") || failure.contains("Certificate"),
        "{failure}"
    );
    assert_eq!(bridge.close(socket.into_raw_fd()), 0);
    drop(peer);
    assert!(fixture.server.join().unwrap().is_err());
}

#[test]
fn integration_lifecycle_scenario_close_joins_pump_under_remote_backpressure() {
    let fixture = Fixture::start_mode(&rustls::version::TLS13, false, true);
    let bridge = new(&fixture.settings).unwrap();
    let (placeholder, peer) = UnixStream::pair().unwrap();
    let fd = placeholder.into_raw_fd();
    assert_eq!(
        bridge.connect(fd, "127.0.0.1", fixture.port, "broker.test:9092"),
        0
    );
    // SAFETY: ownership of the placeholder descriptor was transferred above.
    let mut socket = unsafe { UnixStream::from_raw_fd(fd) };
    let chunk = [42; 64 * 1024];
    loop {
        match socket.write(&chunk) {
            Ok(0) => panic!("bridge unexpectedly closed"),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("bridge write: {error}"),
        }
    }
    let started = std::time::Instant::now();
    assert_eq!(bridge.close(socket.into_raw_fd()), 0);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "pump close waited for remote reader"
    );
    fixture.release.send(()).unwrap();
    fixture.server.join().unwrap().unwrap();
    drop(peer);
    assert!(bridge.errors().is_empty());
}

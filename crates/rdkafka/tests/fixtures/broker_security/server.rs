//! Controlled Kafka metadata peer using real rustls records and ephemeral certificates.
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};

pub type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub struct Identity {
    pub ca: Vec<u8>,
    pub cert: Vec<u8>,
    pub key: Vec<u8>,
    pub client_cert: Vec<u8>,
    pub client_key: Vec<u8>,
}
impl Identity {
    pub fn new() -> TestResult<Self> {
        let ca_key = KeyPair::generate()?;
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "Security fixture CA");
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca = params.self_signed(&ca_key)?;
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::new(vec!["fixture.test".into(), "localhost".into()])?;
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params
            .distinguished_name
            .push(DnType::CommonName, "fixture.test");
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let cert = params.signed_by(&key, &ca, &ca_key)?;
        let client_key = KeyPair::generate()?;
        let mut params = CertificateParams::new(vec!["client.test".into()])?;
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params
            .distinguished_name
            .push(DnType::CommonName, "security-client");
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let client = params.signed_by(&client_key, &ca, &ca_key)?;
        Ok(Self {
            ca: ca.pem().into_bytes(),
            cert: cert.pem().into_bytes(),
            key: key.serialize_pem().into_bytes(),
            client_cert: client.pem().into_bytes(),
            client_key: client_key.serialize_pem().into_bytes(),
        })
    }
}
pub struct Observed {
    pub version: rustls::ProtocolVersion,
    pub sni: Option<String>,
    pub client_authenticated: bool,
    pub metadata_requests: usize,
}
pub struct Server {
    pub address: SocketAddr,
    pub identity: Identity,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<TestResult<Observed>>>,
}
impl Server {
    pub fn new(
        version: &'static rustls::SupportedProtocolVersion,
        mutual: bool,
    ) -> TestResult<Self> {
        let identity = Identity::new()?;
        let builder =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[version])?;
        let builder = if mutual {
            let mut roots = RootCertStore::empty();
            for cert in rustls_pemfile::certs(&mut identity.ca.as_slice()) {
                roots.add(cert?)?;
            }
            builder.with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    Arc::new(rustls::crypto::ring::default_provider()),
                )
                .build()?,
            )
        } else {
            builder.with_no_client_auth()
        };
        let certs =
            rustls_pemfile::certs(&mut identity.cert.as_slice()).collect::<Result<Vec<_>, _>>()?;
        let key = rustls_pemfile::private_key(&mut identity.key.as_slice())?
            .ok_or("missing fixture key")?;
        let mut config = builder.with_single_cert(certs, key)?;
        // Resumption is outside this metadata fixture. Avoid post-handshake ticket
        // writes racing replacement of Kafka's bootstrap connection.
        config.send_tls13_tickets = 0;
        let config = Arc::new(config);
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut workers = Vec::new();
            let mut failure = None;
            // Kafka bootstrap discovery may replace its connection with the advertised broker.
            // Keep accepting peers until the client owner explicitly finishes the fixture.
            while !stopped.load(Ordering::Relaxed) && Instant::now() < deadline {
                match listener.accept() {
                    Ok((socket, _)) => {
                        let config = Arc::clone(&config);
                        workers.push(thread::spawn(move || serve(socket, config, address)));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => {
                        failure = Some(error.to_string());
                        break;
                    }
                }
            }
            let mut observed: Option<Observed> = None;
            for worker in workers {
                match worker
                    .join()
                    .map_err(|_| "TLS connection fixture panicked")?
                {
                    Ok(Some(connection)) => {
                        if let Some(total) = &mut observed {
                            total.metadata_requests += connection.metadata_requests;
                        } else {
                            observed = Some(connection);
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if failure.is_none() {
                            failure = Some(error.to_string());
                        }
                    }
                }
            }
            if let Some(failure) = failure {
                return Err(failure.into());
            }
            observed.ok_or_else(|| "fixture received no Kafka metadata request".into())
        });
        Ok(Self {
            address,
            identity,
            stop,
            thread: Some(thread),
        })
    }
    pub fn finish(&mut self) -> TestResult<Observed> {
        self.stop.store(true, Ordering::Relaxed);
        self.thread
            .take()
            .ok_or("fixture joined twice")?
            .join()
            .map_err(|_| "fixture panicked")?
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            eprintln!("broker TLS fixture panicked");
        }
    }
}

fn serve(
    socket: TcpStream,
    config: Arc<ServerConfig>,
    address: SocketAddr,
) -> TestResult<Option<Observed>> {
    socket.set_read_timeout(Some(Duration::from_secs(3)))?;
    socket.set_write_timeout(Some(Duration::from_secs(3)))?;
    let mut stream = StreamOwned::new(ServerConnection::new(config)?, socket);
    let mut count = 0;
    loop {
        let mut size = [0; 4];
        if let Err(error) = stream.read_exact(&mut size) {
            if matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) {
                break;
            }
            return Err(error.into());
        }
        let size = i32::from_be_bytes(size);
        if !(8..=1024 * 1024).contains(&size) {
            return Err("invalid Kafka frame length".into());
        }
        let mut request = vec![0; size as usize];
        stream.read_exact(&mut request)?;
        if i16::from_be_bytes(request[..2].try_into()?) != 3
            || i16::from_be_bytes(request[2..4].try_into()?) != 0
        {
            return Err("expected Metadata v0 request".into());
        }
        let mut response = request[4..8].to_vec();
        response.extend_from_slice(&1i32.to_be_bytes());
        response.extend_from_slice(&1i32.to_be_bytes());
        response.extend_from_slice(&9i16.to_be_bytes());
        response.extend_from_slice(b"127.0.0.1");
        response.extend_from_slice(&i32::from(address.port()).to_be_bytes());
        response.extend_from_slice(&0i32.to_be_bytes());
        let mut frame = Vec::with_capacity(response.len() + 4);
        frame.extend_from_slice(&(response.len() as i32).to_be_bytes());
        frame.extend_from_slice(&response);
        match stream.write_all(&frame).and_then(|()| stream.flush()) {
            Ok(()) => count += 1,
            // Bootstrap replacement can cancel a valid request while its reply
            // is being written. Only completed replies count toward acceptance.
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                ) =>
            {
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    if count == 0 {
        return Ok(None);
    }
    Ok(Some(Observed {
        version: stream
            .conn
            .protocol_version()
            .ok_or("missing TLS version")?,
        sni: stream.conn.server_name().map(str::to_owned),
        client_authenticated: stream
            .conn
            .peer_certificates()
            .is_some_and(|c| !c.is_empty()),
        metadata_requests: count,
    }))
}

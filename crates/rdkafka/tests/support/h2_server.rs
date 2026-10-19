//! A synchronous TLS/H2 fixture handling one known response without an async runtime.
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct Server {
    pub address: SocketAddr,
    pub ca_pem: Vec<u8>,
    pub client_cert: Vec<u8>,
    pub client_key: Vec<u8>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<TestResult<Observed>>>,
}

pub struct Observed {
    pub body: Vec<u8>,
    pub client_authenticated: bool,
}

impl Server {
    pub fn new(body: &[u8], mutual: bool) -> TestResult<Self> {
        let ca_key = KeyPair::generate()?;
        let mut ca_params = CertificateParams::new(vec![])?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key)?;
        let server_key = KeyPair::generate()?;
        let mut server_params = CertificateParams::new(vec!["fixture.test".into()])?;
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_cert = server_params.signed_by(&server_key, &ca, &ca_key)?;
        let client_key = KeyPair::generate()?;
        let mut client_params = CertificateParams::new(vec!["client.test".into()])?;
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_cert = client_params.signed_by(&client_key, &ca, &ca_key)?;
        let builder =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()?;
        let builder = if mutual {
            let mut roots = RootCertStore::empty();
            roots.add(ca.der().clone())?;
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
        let mut config = builder.with_single_cert(
            vec![server_cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
        )?;
        config.alpn_protocols = vec![b"h2".to_vec()];
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&stop);
        let body = body.to_vec();
        let thread = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            let stream = loop {
                if stopped.load(Ordering::Relaxed) || Instant::now() >= deadline {
                    return Err("TLS fixture stopped before connection".into());
                }
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;
            stream.set_write_timeout(Some(Duration::from_secs(5)))?;
            let mut stream = StreamOwned::new(ServerConnection::new(Arc::new(config))?, stream);
            let mut preface = [0; 24];
            stream.read_exact(&mut preface)?;
            if &preface != b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
                || stream.conn.alpn_protocol() != Some(b"h2")
            {
                return Err("client did not negotiate HTTP/2".into());
            }
            send_frame(&mut stream, 4, 0, 0, &[])?;
            let mut request_body = Vec::new();
            loop {
                let mut header = [0; 9];
                stream.read_exact(&mut header)?;
                let length = (usize::from(header[0]) << 16)
                    | (usize::from(header[1]) << 8)
                    | usize::from(header[2]);
                if length > 1024 * 1024 {
                    return Err("fixture frame too large".into());
                }
                let mut payload = vec![0; length];
                stream.read_exact(&mut payload)?;
                let stream_id = u32::from_be_bytes(header[5..9].try_into()?);
                match header[3] {
                    4 if header[4] & 1 == 0 => send_frame(&mut stream, 4, 1, 0, &[])?,
                    6 if header[4] & 1 == 0 => send_frame(&mut stream, 6, 1, 0, &payload)?,
                    0 | 1 => {
                        if header[3] == 0 {
                            request_body.extend_from_slice(&payload);
                        }
                        if header[4] & 1 != 0 {
                            // HPACK static-table index 8 is the independent literal :status=200.
                            send_frame(&mut stream, 1, 4, stream_id, &[0x88])?;
                            send_frame(&mut stream, 0, 1, stream_id, &body)?;
                            let observed = Observed {
                                body: request_body,
                                client_authenticated: stream
                                    .conn
                                    .peer_certificates()
                                    .is_some_and(|c| !c.is_empty()),
                            };
                            // Keep the connection alive until the client releases its connection
                            // cache. Closing before the client's
                            // SETTINGS ACK races curl's pending writes.
                            let mut drain = [0; 4096];
                            loop {
                                match stream.read(&mut drain) {
                                    Ok(0) => break,
                                    Ok(_) => {}
                                    Err(error)
                                        if error.kind() == std::io::ErrorKind::UnexpectedEof =>
                                    {
                                        break;
                                    }
                                    Err(error) => return Err(error.into()),
                                }
                            }
                            return Ok(observed);
                        }
                    }
                    _ => {}
                }
            }
        });
        Ok(Self {
            address,
            ca_pem: ca.pem().into_bytes(),
            client_cert: client_cert.pem().into_bytes(),
            client_key: client_key.serialize_pem().into_bytes(),
            stop,
            thread: Some(thread),
        })
    }
    pub fn url(&self) -> String {
        format!("https://{}/fixture", self.address)
    }
    pub fn finish(&mut self) -> TestResult<Observed> {
        self.thread
            .take()
            .ok_or("fixture already joined")?
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
            eprintln!("TLS fixture thread panicked");
        }
    }
}
fn send_frame(
    writer: &mut impl Write,
    kind: u8,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> std::io::Result<()> {
    let length = (payload.len() as u32).to_be_bytes();
    writer.write_all(&[length[1], length[2], length[3], kind, flags])?;
    writer.write_all(&stream_id.to_be_bytes())?;
    writer.write_all(payload)?;
    writer.flush()
}

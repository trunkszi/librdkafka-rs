use std::{
    collections::HashMap,
    net::{IpAddr, Shutdown, SocketAddr, TcpStream},
    os::{fd::AsRawFd, unix::net::UnixStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use rdkafka_sys::tls::BrokerTlsHooks;

use super::pump;
use crate::{Error, Result, TlsSettings};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const FAILURE_CAPACITY: usize = 64;

// Successful pump iterations never access this queue. Its mutex coordinates
// only connection failures, teardown failures, and caller-thread diagnostics.
#[derive(Default)]
struct Failures {
    messages: Vec<String>,
    omitted: u64,
}

impl Failures {
    fn push(&mut self, message: String) {
        // Reserve one output entry for an explicit overflow diagnostic.
        if self.messages.len() < FAILURE_CAPACITY - 1 {
            self.messages.push(message);
        } else {
            self.omitted = self.omitted.saturating_add(1);
        }
    }

    fn drain(&mut self) -> Vec<String> {
        let mut messages = std::mem::take(&mut self.messages);
        let omitted = std::mem::take(&mut self.omitted);
        if omitted != 0 {
            messages.push(format!(
                "BROKER_TLS_DIAGNOSTIC_OVERFLOW: at least {omitted} additional failures"
            ));
        }
        messages
    }
}

struct Session {
    wake: UnixStream,
    worker: JoinHandle<()>,
    stopping: Arc<AtomicBool>,
    failures: Arc<Mutex<Failures>>,
}

impl Session {
    fn stop(self) {
        self.stopping.store(true, Ordering::Release);
        // Shutdown wakes poll even when native shutdown has no unread data.
        match self.wake.shutdown(Shutdown::Both) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotConnected => {}
            Err(error) => self
                .failures
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(format!("broker TLS shutdown: {error}")),
        }
        // A panic is contained by the transport worker and never reaches C++.
        if self.worker.join().is_err() {
            self.failures
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push("broker TLS worker join failed".into());
        }
    }
}

pub(super) struct BrokerTls {
    config: Arc<rustls::ClientConfig>,
    server_name: Option<String>,
    sessions: Mutex<HashMap<i32, Session>>,
    failures: Arc<Mutex<Failures>>,
}

impl BrokerTls {
    pub(super) fn new(settings: &TlsSettings) -> Result<Self> {
        Ok(Self {
            config: Arc::new(settings.build_client_config()?),
            server_name: settings.server_name.clone(),
            sessions: Mutex::new(HashMap::new()),
            failures: Arc::default(),
        })
    }

    fn connect_verified(&self, fd: i32, address: &str, port: u16, name: &str) -> Result<()> {
        let address = address
            .parse::<IpAddr>()
            .map_err(|_| Error::Tls("native broker address is not an IP address".into()))?;
        let stream = TcpStream::connect_timeout(&SocketAddr::new(address, port), CONNECT_TIMEOUT)
            .map_err(|error| Error::Tls(format!("broker TCP connect: {error}")))?;
        stream.set_nonblocking(true).map_err(io_error)?;
        stream.set_nodelay(true).map_err(io_error)?;
        let hostname = self
            .server_name
            .as_deref()
            .unwrap_or_else(|| peer_name(name));
        let name = rustls::pki_types::ServerName::try_from(hostname.to_owned())
            .map_err(|_| Error::Tls("invalid broker TLS server name".into()))?;
        let mut connection = rustls::ClientConnection::new(self.config.clone(), name)
            .map_err(|error| Error::Tls(format!("broker TLS initialization: {error}")))?;
        connection.set_buffer_limit(Some(pump::BUFFER_SIZE));
        pump::handshake(&mut connection, &stream, CONNECT_TIMEOUT).map_err(io_error)?;
        let (native, local) = UnixStream::pair().map_err(io_error)?;
        local.set_nonblocking(true).map_err(io_error)?;
        native.set_nonblocking(true).map_err(io_error)?;
        let wake = local.try_clone().map_err(io_error)?;
        let failures = self.failures.clone();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = stopping.clone();
        let worker = std::thread::Builder::new()
            .name("kafka-rustls".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    pump::run(connection, stream, &local, &worker_stopping)
                }));
                let message = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(format!("broker TLS transport: {error}")),
                    Err(_) => Some("broker TLS transport panicked".into()),
                };
                if !worker_stopping.load(Ordering::Acquire)
                    && let Some(message) = message
                {
                    failures
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push(message);
                }
                // The stop handle duplicates this endpoint, so dropping local
                // alone cannot signal EOF to the native endpoint.
                if let Err(error) = local.shutdown(Shutdown::Both)
                    && error.kind() != std::io::ErrorKind::NotConnected
                {
                    failures
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push(format!("broker TLS endpoint shutdown: {error}"));
                }
            })
            .map_err(io_error)?;
        // Native transport has already registered fd with its poller. Replacing
        // its open file description preserves that descriptor identity.
        // SAFETY: both descriptors remain open throughout dup2; fd is native-owned.
        let replaced = unsafe { libc::dup2(native.as_raw_fd(), fd) };
        // SAFETY: fd remains native-owned; fcntl sets descriptor flags only.
        let protected =
            replaced >= 0 && unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } >= 0;
        if !protected {
            let error = std::io::Error::last_os_error();
            Session {
                wake,
                worker,
                stopping,
                failures: self.failures.clone(),
            }
            .stop();
            return Err(io_error(error));
        }
        let old = self
            .sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(
                fd,
                Session {
                    wake,
                    worker,
                    stopping,
                    failures: self.failures.clone(),
                },
            );
        if let Some(old) = old {
            old.stop();
        }
        Ok(())
    }
}

impl BrokerTlsHooks for BrokerTls {
    fn fork(&self) -> Arc<dyn BrokerTlsHooks> {
        Arc::new(Self {
            config: self.config.clone(),
            server_name: self.server_name.clone(),
            sessions: Mutex::default(),
            failures: Arc::default(),
        })
    }

    fn connect(&self, fd: i32, address: &str, port: u16, name: &str) -> i32 {
        match self.connect_verified(fd, address, port, name) {
            Ok(()) => 0,
            Err(error) => {
                self.failures
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .push(error.to_string());
                libc::EPROTO
            }
        }
    }

    fn close(&self, fd: i32) -> i32 {
        let session = self
            .sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&fd);
        // SAFETY: the native closesocket callback transfers closure of this fd.
        let result = unsafe { libc::close(fd) };
        if let Some(session) = session {
            session.stop();
        }
        result
    }

    fn errors(&self) -> Vec<String> {
        self.failures
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .drain()
    }
}

impl Drop for BrokerTls {
    fn drop(&mut self) {
        let sessions = self
            .sessions
            .get_mut()
            .unwrap_or_else(|poison| poison.into_inner());
        for (_, session) in sessions.drain() {
            session.stop();
        }
    }
}

fn peer_name(name: &str) -> &str {
    if name.parse::<IpAddr>().is_ok() {
        return name;
    }
    if let Some(name) = name.strip_prefix('[') {
        return name.split(']').next().unwrap_or(name);
    }
    name.rsplit_once(':').map_or(name, |(host, _)| host)
}

fn io_error(error: std::io::Error) -> Error {
    Error::Tls(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{FAILURE_CAPACITY, Failures};

    #[test]
    fn unit_scenario_diagnostics_preserve_first_failures_bound_storage_and_report_overflow() {
        let mut failures = Failures::default();
        for index in 0..1000 {
            failures.push(format!("failure {index}"));
        }
        assert_eq!(failures.messages.len(), FAILURE_CAPACITY - 1);
        let messages = failures.drain();
        assert_eq!(messages.len(), FAILURE_CAPACITY);
        assert_eq!(messages[0], "failure 0");
        assert_eq!(messages[62], "failure 62");
        assert_eq!(
            messages[63],
            "BROKER_TLS_DIAGNOSTIC_OVERFLOW: at least 937 additional failures"
        );
        assert!(failures.drain().is_empty());
        failures.push("new failure".into());
        assert_eq!(failures.drain(), ["new failure"]);
    }
}

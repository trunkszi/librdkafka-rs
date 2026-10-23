//! Synchronous caller-thread HTTP driven by a private io_uring proactor.

use std::{
    cell::Cell,
    marker::PhantomData,
    mem::ManuallyDrop,
    net::IpAddr,
    rc::Rc,
    time::{Duration, Instant},
};

use nexora::{
    AlpnProtocol, ClientConfig, HttpClient, Method, Proactor, ProtoError, Request, Scheme,
    TlsClientConfig, Uri, Version,
};

pub use crate::tls::TlsSettings;
use crate::{Error, Result};
mod dns;

const MAX_RESPONSE_HEADER_BYTES: usize = 65_536;

/// HTTP protocol requested for subsequent transfers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HttpVersionChoice {
    /// Negotiate HTTP/2, accepting HTTP/1.1 when the endpoint requires it.
    #[default]
    Negotiate,
    /// Require HTTP/2 over TLS through ALPN.
    Http2Tls,
    /// Require an HTTP/2 preface, including for cleartext endpoints.
    Http2PriorKnowledge,
    /// Request HTTP/1.1.
    Http1,
    /// Request HTTP/1.1 without protocol negotiation.
    Http1Only,
}

/// Connection and protocol preferences applied to every request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpPreferences {
    pub version: HttpVersionChoice,
    pub tcp_nodelay: bool,
    pub keep_alive: bool,
    pub multiplex: bool,
}

impl Default for HttpPreferences {
    fn default() -> Self {
        Self {
            version: HttpVersionChoice::Negotiate,
            tcp_nodelay: true,
            keep_alive: true,
            multiplex: true,
        }
    }
}

/// Owned request data; authorization headers and request bodies are never logged.
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Positive overall transfer deadline, rounded up to milliseconds.
    /// Values above `i32::MAX` milliseconds are rejected before DNS resolution.
    pub timeout: Duration,
    /// Maximum response body size, independently of the 64 KiB header limit.
    pub max_response_bytes: usize,
}

impl HttpRequest {
    /// Create a request with a 30 second transfer deadline and 8 MiB body limit.
    #[must_use]
    pub fn new(method: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            url: url.into(),
            headers: Vec::new(),
            body: Vec::new(),
            timeout: Duration::from_secs(30),
            max_response_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Response status, final headers, and bounded body.
#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub version: Option<HttpVersionChoice>,
}

/// Owns a persistent connection pool and completion driver on its creating thread.
///
/// Ordinary DNS uses c-ares through Nexora. A TLS name override binds that name
/// to the original endpoint addresses and isolates pools by original authority.
/// Certificates and client keys remain in memory throughout the connection.
pub struct HttpTransport {
    tls: TlsSettings,
    tls_config: TlsClientConfig,
    preferences: HttpPreferences,
    engine: Option<Engine>,
    owner: PhantomData<Rc<()>>,
}

struct Engine {
    client: Option<HttpClient>,
    proactor: Proactor,
    timeout: Duration,
    max_body: usize,
    original_authority: Option<String>,
    pinned_addresses: Option<Vec<IpAddr>>,
}

struct Prepared {
    request: Request,
    timeout: Duration,
    original_authority: Option<String>,
    addresses: Option<Vec<IpAddr>>,
}

impl HttpTransport {
    /// Configure HTTP negotiation and verified TLS without opening a socket.
    ///
    /// # Errors
    /// Returns an error for invalid trust, peer names, or client credentials.
    pub fn new(tls: TlsSettings) -> Result<Self> {
        Self::with_preferences(tls, HttpPreferences::default())
    }

    /// Configure connection preferences and load in-memory TLS credentials.
    ///
    /// # Errors
    /// Returns an error for invalid trust, peer names, or client credentials.
    pub fn with_preferences(tls: TlsSettings, preferences: HttpPreferences) -> Result<Self> {
        let alpn: &[AlpnProtocol] = match preferences.version {
            HttpVersionChoice::Http1 | HttpVersionChoice::Http1Only => &[AlpnProtocol::Http11],
            HttpVersionChoice::Negotiate => &[AlpnProtocol::Http2, AlpnProtocol::Http11],
            _ => &[AlpnProtocol::Http2],
        };
        let tls_config = build_tls_config(&tls, alpn)?;
        Ok(Self {
            tls,
            tls_config,
            preferences,
            engine: None,
            owner: PhantomData,
        })
    }

    /// Return the configured protocol and connection preferences.
    #[must_use]
    pub fn preferences(&self) -> HttpPreferences {
        self.preferences
    }

    /// Execute a bounded transfer and return every HTTP status to the caller.
    ///
    /// # Errors
    /// Returns an error for invalid input, DNS or transport failures, timeout,
    /// TLS verification failure, or response headers/body exceeding their limits.
    ///
    /// # Panics
    /// Panics if the proactor cannot retire pending operations during cleanup.
    pub fn execute(&mut self, request: HttpRequest) -> Result<HttpResponse> {
        let started = Instant::now();
        let prepared = self.prepare(&request, started)?;
        let deadline = started + prepared.timeout;
        if Instant::now() >= deadline {
            return Err(Error::Http("HTTP request timed out".into()));
        }
        let secure = prepared.request.uri.scheme() == Scheme::Https;
        let replace = self.engine.as_ref().is_none_or(|engine| {
            engine.timeout != prepared.timeout
                || engine.max_body != request.max_response_bytes
                || engine.original_authority != prepared.original_authority
                || engine.pinned_addresses != prepared.addresses
        });
        if replace {
            self.engine = None;
            let mut proactor = Proactor::builder()
                .build()
                .map_err(|_| Error::Http("event-loop initialization failed".into()))?;
            let mut config = ClientConfig {
                request_timeout: prepared.timeout,
                connect_timeout: prepared.timeout.min(Duration::from_secs(10)),
                max_header_list_size: MAX_RESPONSE_HEADER_BYTES,
                max_body_size: request.max_response_bytes,
                max_request_body_size: usize::MAX,
                follow_redirects: false,
                prefer_http2: !matches!(
                    self.preferences.version,
                    HttpVersionChoice::Http1 | HttpVersionChoice::Http1Only
                ),
                http2_prior_knowledge: self.preferences.version
                    == HttpVersionChoice::Http2PriorKnowledge,
                tls_config: Some(self.tls_config.clone()),
                tls_server_name: self.tls.server_name.clone(),
                ..ClientConfig::default()
            };
            if !self.preferences.keep_alive {
                config.pool.max_idle_per_origin = 0;
            }
            let client = HttpClient::new(&mut proactor, config).map_err(transport_error)?;
            self.engine = Some(Engine {
                client: Some(client),
                proactor,
                timeout: prepared.timeout,
                max_body: request.max_response_bytes,
                original_authority: prepared.original_authority,
                pinned_addresses: prepared.addresses.clone(),
            });
        }
        let engine = self
            .engine
            .as_mut()
            .ok_or_else(|| Error::Http("HTTP driver unavailable".into()))?;
        let client = engine
            .client
            .as_mut()
            .ok_or_else(|| Error::Http("HTTP driver unavailable".into()))?;
        if let Some(addresses) = prepared.addresses {
            client.resolve_host(
                prepared.request.uri.host(),
                prepared.request.uri.port(),
                addresses,
            );
        }
        let result = Rc::new(Cell::new(None));
        let completed = Rc::clone(&result);
        client
            .submit(
                &mut engine.proactor,
                prepared.request,
                move |_, _, response| {
                    completed.set(Some(response));
                },
            )
            .map_err(transport_error)?;
        let response = loop {
            if Instant::now() >= deadline {
                break Err(Error::Http("HTTP request timed out".into()));
            }
            if let Some(response) = result.take() {
                break response.map_err(transport_error);
            }
            if let Err(error) = client.drive(&mut engine.proactor) {
                break Err(transport_error(error));
            }
        };
        if response.is_err() || !self.preferences.keep_alive {
            self.engine = None;
        }
        let response = response?;
        if (self.preferences.version == HttpVersionChoice::Http2PriorKnowledge
            || secure && self.preferences.version == HttpVersionChoice::Http2Tls)
            && response.version != Version::Http2
        {
            self.engine = None;
            return Err(Error::Http(
                "HTTP/2 was required but was not negotiated".into(),
            ));
        }
        if response.body.len() > request.max_response_bytes {
            return Err(Error::Http(
                "HTTP response body exceeds configured size limit".into(),
            ));
        }
        if response.headers.byte_size() > MAX_RESPONSE_HEADER_BYTES {
            return Err(Error::Http(
                "HTTP response headers exceed configured size limit".into(),
            ));
        }
        Ok(HttpResponse {
            status: response.status.as_u16(),
            headers: response
                .headers
                .iter()
                .map(|field| (field.name.clone(), field.value.clone()))
                .collect(),
            body: response.body.as_ref().to_vec(),
            version: Some(if response.version == Version::Http2 {
                if secure {
                    HttpVersionChoice::Http2Tls
                } else {
                    HttpVersionChoice::Http2PriorKnowledge
                }
            } else {
                HttpVersionChoice::Http1
            }),
        })
    }

    fn prepare(&self, input: &HttpRequest, started: Instant) -> Result<Prepared> {
        let timeout = transfer_timeout(input.timeout)?;
        let method =
            Method::parse(&input.method).map_err(|_| Error::Http("invalid HTTP method".into()))?;
        let raw_authority = input
            .url
            .split_once("://")
            .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or(""));
        if input.url.contains('#') || raw_authority.is_some_and(|value| value.contains('@')) {
            return Err(Error::Http(
                "HTTP URL must use http/https without credentials or fragment".into(),
            ));
        }
        let uri = Uri::parse(&input.url).map_err(|_| Error::Http("invalid HTTP URL".into()))?;
        if !matches!(uri.scheme(), Scheme::Http | Scheme::Https) {
            return Err(Error::Http(
                "HTTP URL must use http/https without credentials or fragment".into(),
            ));
        }
        let original_host = uri.host().to_owned();
        let original_authority = uri.authority();
        let override_name = self
            .tls
            .server_name
            .as_ref()
            .filter(|_| uri.scheme() == Scheme::Https);
        let mut request = Request::new(method, uri);
        request.body = nexora::Bytes::copy_from_slice(&input.body);
        for (name, value) in &input.headers {
            if name.is_empty() || !name.bytes().all(is_token) || value.contains(['\r', '\n', '\0'])
            {
                return Err(Error::Http("invalid HTTP header".into()));
            }
            request.headers.append(name, value);
        }
        if !self.preferences.keep_alive
            && matches!(
                self.preferences.version,
                HttpVersionChoice::Http1 | HttpVersionChoice::Http1Only
            )
        {
            request.headers.insert("connection", "close");
        }
        let addresses = if override_name.is_some() || dns::is_localhost(&original_host) {
            Some(dns::resolve(
                &original_host,
                request.uri.port(),
                started + timeout,
            )?)
        } else {
            None
        };
        Ok(Prepared {
            request,
            timeout,
            original_authority: override_name.map(|_| original_authority),
            addresses,
        })
    }
}

// The ring must retire kernel references before the client's pinned storage drops.
impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let mut retained = ManuallyDrop::new(client);
            retained.shutdown(&mut self.proactor);
            match self.proactor.shutdown() {
                Ok(()) => drop(ManuallyDrop::into_inner(retained)),
                Err(error) => eprintln!("HTTP driver shutdown failed: {error}"),
            }
        }
    }
}

fn transport_error(error: nexora::Error) -> Error {
    match error {
        nexora::Error::BodyTooLarge { .. }
        | nexora::Error::Proto(ProtoError::BodyTooLarge { .. }) => {
            Error::Http("HTTP response body exceeds configured size limit".into())
        }
        nexora::Error::Proto(ProtoError::HeaderListTooLarge { .. }) => {
            Error::Http("HTTP response headers exceed configured size limit".into())
        }
        nexora::Error::Proto(ProtoError::Http2StreamError {
            code: nexora::Http2ErrorCode::EnhanceYourCalm,
            reason,
            ..
        }) if reason == "header list too large" => {
            Error::Http("HTTP response headers exceed configured size limit".into())
        }
        nexora::Error::Proto(ProtoError::Http2ConnectionError {
            code: nexora::Http2ErrorCode::EnhanceYourCalm,
            reason,
        }) if reason == "compressed header block too large" => {
            Error::Http("HTTP response headers exceed configured size limit".into())
        }
        nexora::Error::Proto(ProtoError::Tls(reason))
            if reason == "peer did not negotiate HTTP/2 ALPN" =>
        {
            Error::Http("HTTP/2 was required but was not negotiated".into())
        }
        nexora::Error::Proto(ProtoError::Tls(_)) => {
            Error::Tls("HTTP TLS verification or handshake failed".into())
        }
        nexora::Error::Timeout => Error::Http("HTTP request timed out".into()),
        nexora::Error::Dns(_) | nexora::Error::DnsResolutionFailed { .. } => {
            Error::Http("HTTP endpoint DNS lookup failed".into())
        }
        _ => Error::Http("HTTP transport operation failed".into()),
    }
}

fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}
fn transfer_timeout(timeout: Duration) -> Result<Duration> {
    if timeout.is_zero() {
        return Err(Error::Http("HTTP timeout must be positive".into()));
    }
    if timeout > Duration::from_millis(i32::MAX as u64) {
        return Err(Error::Http(
            "HTTP timeout exceeds maximum supported milliseconds".into(),
        ));
    }
    let millis = timeout.as_nanos().div_ceil(1_000_000) as u64;
    Ok(Duration::from_millis(millis))
}

fn build_tls_config(settings: &TlsSettings, alpn: &[AlpnProtocol]) -> Result<TlsClientConfig> {
    settings.validate()?;
    let mut roots = Vec::new();
    if settings.use_native_roots {
        roots.extend(
            rustls_native_certs::load_native_certs()
                .certs
                .into_iter()
                .map(|cert| cert.to_vec()),
        );
    }
    if !settings.ca_pem.is_empty() {
        let custom = certificates(&settings.ca_pem, "invalid CA PEM")?;
        if custom.is_empty() {
            return Err(Error::Tls("CA PEM contains no certificates".into()));
        }
        roots.extend(custom);
    }
    if roots.is_empty() {
        return Err(Error::Tls("no trusted certificate authorities".into()));
    }
    let config = TlsClientConfig::with_root_certificates(&roots, alpn)
        .map_err(|_| Error::Tls("invalid CA certificate".into()))?;
    if settings.client_cert_pem.is_empty() {
        return Ok(config);
    }
    let certs = certificates(&settings.client_cert_pem, "invalid client certificate PEM")?;
    let key = rustls_pemfile::private_key(&mut settings.client_key_pem.as_slice())
        .map_err(|_| Error::Tls("invalid client private key PEM".into()))?
        .ok_or_else(|| Error::Tls("missing client private key".into()))?;
    config
        .with_client_auth(certs, key.secret_der().to_vec())
        .map_err(|_| Error::Tls("invalid client identity".into()))
}

fn certificates(pem: &[u8], message: &str) -> Result<Vec<Vec<u8>>> {
    rustls_pemfile::certs(&mut &*pem)
        .map(|cert| cert.map(|cert| cert.to_vec()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|_| Error::Tls(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_scenario_tls_identity_override_preserves_http_authority() {
        let transport = HttpTransport::new(TlsSettings::default().with_server_name("::1")).unwrap();
        let prepared = transport
            .prepare(
                &HttpRequest::new("GET", "https://127.0.0.1:1/"),
                Instant::now(),
            )
            .unwrap();
        assert_eq!(prepared.request.uri.as_str(), "https://127.0.0.1:1/");
    }

    #[test]
    fn unit_scenario_positive_submillisecond_timeouts_remain_bounded() {
        for micros in [1, 500, 999] {
            assert_eq!(
                transfer_timeout(Duration::from_micros(micros)).unwrap(),
                Duration::from_millis(1)
            );
        }
        assert_eq!(
            transfer_timeout(Duration::from_nanos(1)).unwrap(),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn unit_scenario_timeout_bounds_are_checked_without_overflow() {
        let maximum = Duration::from_millis(i32::MAX as u64);
        assert_eq!(transfer_timeout(maximum).unwrap(), maximum);
        for timeout in [
            Duration::ZERO,
            maximum + Duration::from_nanos(1),
            Duration::MAX,
        ] {
            assert!(matches!(transfer_timeout(timeout), Err(Error::Http(_))));
        }
    }
}

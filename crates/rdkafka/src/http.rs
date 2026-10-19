//! Synchronous, caller-thread HTTP on a persistent curl event loop.

use std::{
    cell::{Cell, RefCell},
    marker::PhantomData,
    net::{IpAddr, ToSocketAddrs},
    rc::Rc,
    time::Duration,
};

use curl::{
    EventLoop,
    easy::{Easy, HttpVersion, List, SslOpt},
};

pub use crate::tls::TlsSettings;
use crate::{Error, Result};

mod identity;

/// HTTP protocol requested for subsequent transfers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HttpVersionChoice {
    /// Negotiate HTTP/2, accepting HTTP/1.1 when the endpoint requires it.
    #[default]
    Negotiate,
    /// Request HTTP/2 over TLS through ALPN.
    Http2Tls,
    /// Require an HTTP/2 preface, including for cleartext endpoints.
    Http2PriorKnowledge,
    /// Request HTTP/1.1.
    Http1,
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
    /// Positive overall curl transfer deadline, rounded up to milliseconds.
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

/// Owns the connection cache and all callbacks on its creating thread.
///
/// DNS resolution uses the calling thread's platform resolver before the curl
/// transfer deadline starts. TLS peer and hostname verification are mandatory.
/// HTTP mutual TLS uses sealed anonymous RAM files on Linux; other platforms
/// reject client identities without writing credentials to disk.
pub struct HttpTransport {
    loop_: EventLoop,
    tls: TlsSettings,
    preferences: HttpPreferences,
    owner: PhantomData<Rc<()>>,
    identity: identity::Identity,
}

impl HttpTransport {
    /// Open an event loop using HTTP/2 negotiation and verified TLS.
    ///
    /// # Errors
    /// Returns an error for missing HTTP/2 or rustls support, invalid TLS
    /// settings, or an event-loop initialization failure.
    pub fn new(tls: TlsSettings) -> Result<Self> {
        Self::with_preferences(tls, HttpPreferences::default())
    }

    /// Open an event loop with explicit connection preferences.
    ///
    /// # Errors
    /// Returns an error when the runtime lacks HTTP/2 or rustls, the TLS
    /// settings are invalid, or curl rejects the requested options.
    pub fn with_preferences(tls: TlsSettings, preferences: HttpPreferences) -> Result<Self> {
        let backend = curl::Version::get();
        if !backend.feature_http2() {
            return Err(Error::Http("curl lacks HTTP/2 support".into()));
        }
        if !backend
            .ssl_version()
            .is_some_and(|name| name.to_ascii_lowercase().contains("rustls"))
        {
            return Err(Error::Tls("curl must use the rustls backend".into()));
        }
        tls.build_client_config()?;
        let identity = identity::Identity::new(&tls)?;
        let loop_ =
            EventLoop::new().map_err(|_| Error::Http("event-loop initialization failed".into()))?;
        loop_
            .pipelining(false, preferences.multiplex)
            .map_err(|_| Error::Http("HTTP multiplex configuration failed".into()))?;
        Ok(Self {
            loop_,
            tls,
            preferences,
            owner: PhantomData,
            identity,
        })
    }

    /// Return the configured protocol and connection preferences.
    #[must_use]
    pub fn preferences(&self) -> HttpPreferences {
        self.preferences
    }

    /// Execute a bounded transfer and return all HTTP status codes to the caller.
    ///
    /// # Errors
    /// Returns an error for invalid input, DNS or transport failures, timeout,
    /// TLS verification failure, or response headers/body exceeding their limits.
    pub fn execute(&mut self, request: HttpRequest) -> Result<HttpResponse> {
        let mut easy = self.configured_easy(&request)?;
        let response = Rc::new(RefCell::new(HttpResponse {
            status: 0,
            headers: Vec::new(),
            body: Vec::new(),
            version: None,
        }));
        let callback_failure = Rc::new(Cell::new(None));
        let header_bytes = Rc::new(Cell::new(0usize));
        let failure = Rc::new(RefCell::new(None));
        let done = Rc::new(Cell::new(false));
        {
            let response = Rc::clone(&response);
            let callback_failure = Rc::clone(&callback_failure);
            let limit = request.max_response_bytes;
            easy.write_function(move |bytes| {
                let Ok(mut response) = response.try_borrow_mut() else {
                    callback_failure.set(Some("HTTP body callback reentered"));
                    return Ok(0);
                };
                if bytes.len() > limit.saturating_sub(response.body.len()) {
                    callback_failure.set(Some("HTTP response body exceeds configured size limit"));
                    return Ok(0);
                }
                response.body.extend_from_slice(bytes);
                Ok(bytes.len())
            })
            .map_err(curl_error)?;
        }
        {
            let response = Rc::clone(&response);
            let callback_failure = Rc::clone(&callback_failure);
            easy.header_function(move |bytes| {
                let count = header_bytes.get().saturating_add(bytes.len());
                header_bytes.set(count);
                if count > 64 * 1024 {
                    callback_failure
                        .set(Some("HTTP response headers exceed configured size limit"));
                    return false;
                }
                let Ok(line) = std::str::from_utf8(bytes) else {
                    callback_failure.set(Some("HTTP response header is not UTF-8"));
                    return false;
                };
                let Ok(mut response) = response.try_borrow_mut() else {
                    callback_failure.set(Some("HTTP header callback reentered"));
                    return false;
                };
                if line.starts_with("HTTP/") {
                    response.headers.clear();
                    response.version = if line.starts_with("HTTP/2") {
                        Some(HttpVersionChoice::Http2Tls)
                    } else if line.starts_with("HTTP/1.") {
                        Some(HttpVersionChoice::Http1)
                    } else {
                        None
                    };
                } else if let Some((name, value)) = line.split_once(':') {
                    response
                        .headers
                        .push((name.trim().to_owned(), value.trim().to_owned()));
                }
                true
            })
            .map_err(curl_error)?;
        }
        let completed = Rc::clone(&done);
        let completed_response = Rc::clone(&response);
        let completed_failure = Rc::clone(&failure);
        let token = self
            .loop_
            .add(easy, move |easy, result| {
                match result {
                    Err(error) => *completed_failure.borrow_mut() = Some(event_error(error)),
                    Ok(()) => match easy.response_code() {
                        Ok(status) => match u16::try_from(status) {
                            Ok(status) => completed_response.borrow_mut().status = status,
                            Err(_) => {
                                *completed_failure.borrow_mut() =
                                    Some(Error::Http("invalid HTTP status".into()))
                            }
                        },
                        Err(error) => *completed_failure.borrow_mut() = Some(curl_error(error)),
                    },
                }
                completed.set(true);
            })
            .map_err(|_| Error::Http("could not schedule HTTP request".into()))?;
        if self.loop_.run().is_err() {
            if !done.get() {
                self.loop_.cancel(token).map_err(|_| {
                    Error::Http("event-loop failure and cancellation failed".into())
                })?;
            }
            return Err(Error::Http("HTTP event-loop failure".into()));
        }
        if let Some(reason) = callback_failure.get() {
            return Err(Error::Http(reason.into()));
        }
        if let Some(error) = failure.borrow_mut().take() {
            return Err(error);
        }
        if !done.get() {
            return Err(Error::Http("HTTP request did not complete".into()));
        }
        let mut borrowed = response.borrow_mut();
        Ok(HttpResponse {
            status: borrowed.status,
            headers: std::mem::take(&mut borrowed.headers),
            body: std::mem::take(&mut borrowed.body),
            version: borrowed.version,
        })
    }

    fn configured_easy(&self, request: &HttpRequest) -> Result<Easy> {
        if request.timeout.is_zero() {
            return Err(Error::Http("HTTP timeout must be positive".into()));
        }
        if request.method.is_empty() || !request.method.bytes().all(is_token) {
            return Err(Error::Http("invalid HTTP method".into()));
        }
        let mut url =
            url::Url::parse(&request.url).map_err(|_| Error::Http("invalid HTTP URL".into()))?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::Http(
                "HTTP URL must use http/https without credentials or fragment".into(),
            ));
        }
        let original_host = url
            .host_str()
            .ok_or_else(|| Error::Http("HTTP URL has no host".into()))?
            .trim_matches(['[', ']'])
            .to_owned();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| Error::Http("HTTP URL has no port".into()))?;
        let original_authority = &url[url::Position::BeforeHost..url::Position::AfterPort];
        let original_authority = original_authority.to_owned();
        if url.scheme() == "https"
            && let Some(name) = &self.tls.server_name
        {
            url.set_host(Some(name))
                .map_err(|_| Error::Tls("invalid TLS server name".into()))?;
        }
        let mut easy = Easy::new();
        easy.url(url.as_str()).map_err(curl_error)?;
        easy.proxy("").map_err(curl_error)?;
        let addresses = resolve(&original_host, port)?;
        easy.resolve_host(
            url.host_str()
                .unwrap_or(&original_host)
                .trim_matches(['[', ']']),
            port,
            &addresses,
        )
        .map_err(curl_error)?;
        easy.http_version(match self.preferences.version {
            HttpVersionChoice::Negotiate => HttpVersion::V2,
            HttpVersionChoice::Http2Tls => HttpVersion::V2TLS,
            HttpVersionChoice::Http2PriorKnowledge => HttpVersion::V2PriorKnowledge,
            HttpVersionChoice::Http1 => HttpVersion::V11,
        })
        .map_err(curl_error)?;
        easy.tcp_nodelay(self.preferences.tcp_nodelay)
            .map_err(curl_error)?;
        easy.tcp_keepalive(self.preferences.keep_alive)
            .map_err(curl_error)?;
        easy.forbid_reuse(!self.preferences.keep_alive)
            .map_err(curl_error)?;
        easy.fresh_connect(!self.preferences.keep_alive)
            .map_err(curl_error)?;
        easy.pipewait(self.preferences.multiplex)
            .map_err(curl_error)?;
        let millis = request.timeout.as_millis().max(1).min(u128::from(u64::MAX)) as u64;
        easy.timeout(Duration::from_millis(millis))
            .map_err(curl_error)?;
        easy.connect_timeout(Duration::from_millis(millis).min(Duration::from_secs(10)))
            .map_err(curl_error)?;
        easy.ssl_verify_peer(true).map_err(curl_error)?;
        easy.ssl_verify_host(true).map_err(curl_error)?;
        if !self.tls.ca_pem.is_empty() {
            easy.ssl_cainfo_blob(&self.tls.ca_pem).map_err(curl_error)?;
        }
        let mut ssl = SslOpt::new();
        ssl.native_ca(self.tls.use_native_roots);
        easy.ssl_options(&ssl).map_err(curl_error)?;
        self.identity.configure(&mut easy)?;
        let mut headers = List::new();
        headers.append("Expect:").map_err(curl_error)?;
        if self.tls.server_name.is_some() && url.scheme() == "https" {
            headers
                .append(&format!("Host: {original_authority}"))
                .map_err(curl_error)?;
        }
        for (name, value) in &request.headers {
            if name.is_empty() || !name.bytes().all(is_token) || value.contains(['\r', '\n', '\0'])
            {
                return Err(Error::Http("invalid HTTP header".into()));
            }
            headers
                .append(&format!("{name}: {value}"))
                .map_err(curl_error)?;
        }
        easy.http_headers(headers).map_err(curl_error)?;
        if request.method == "HEAD" {
            easy.nobody(true).map_err(curl_error)?;
        } else if !request.body.is_empty() || request.method == "POST" {
            easy.post_fields_copy(&request.body).map_err(curl_error)?;
        }
        easy.custom_request(&request.method).map_err(curl_error)?;
        Ok(easy)
    }
}

fn curl_error(error: curl::Error) -> Error {
    Error::Http(error.description().to_owned())
}
fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}
fn resolve(host: &str, port: u16) -> Result<Vec<IpAddr>> {
    if let Ok(address) = host.parse::<IpAddr>() {
        return Ok(vec![address]);
    }
    let mut addresses = Vec::new();
    for address in (host, port)
        .to_socket_addrs()
        .map_err(|_| Error::Http("HTTP endpoint DNS lookup failed".into()))?
    {
        if !addresses.contains(&address.ip()) {
            addresses.push(address.ip());
        }
    }
    if addresses.is_empty() {
        return Err(Error::Http(
            "HTTP endpoint has no resolved addresses".into(),
        ));
    }
    Ok(addresses)
}

fn event_error(error: curl::event_loop::Error) -> Error {
    match error {
        curl::event_loop::Error::Curl(error) => curl_error(error),
        curl::event_loop::Error::Multi(error) => Error::Http(error.description().to_owned()),
        curl::event_loop::Error::Io(error) => {
            Error::Http(format!("HTTP transport I/O failed: {:?}", error.kind()))
        }
        _ => Error::Http("HTTP event-loop operation failed".into()),
    }
}

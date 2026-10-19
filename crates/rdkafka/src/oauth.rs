//! Caller-thread OAuth client-credentials refresh and native token delivery.
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    Error, Result,
    http::{HttpRequest, HttpTransport},
};

/// Token installation boundary, implemented by a native client or callback adapter.
pub trait OAuthBearerTokenSetter {
    /// Install a validated token; `expires_at_ms` is Unix epoch milliseconds.
    ///
    /// # Errors
    /// Returns an error if the native client rejects the token.
    fn oauthbearer_set_token(&mut self, token: &OAuthBearerToken) -> Result<()>;
    /// Enter the native client's token failure state.
    ///
    /// # Errors
    /// Returns an error if the failure state cannot be installed.
    fn oauthbearer_set_token_failure(&mut self, reason: &str) -> Result<()>;
}

/// Validated access token. Debug output deliberately excludes credentials.
pub struct OAuthBearerToken {
    pub access_token: String,
    pub expires_at_ms: i64,
    pub principal: String,
    pub scope: Vec<String>,
    pub extensions: BTreeMap<String, String>,
}
impl std::fmt::Debug for OAuthBearerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthBearerToken")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

/// Client-credentials request configuration; secrets are excluded from Debug output.
pub struct OAuthBearerConfig {
    pub token_endpoint: String,
    /// Permit HTTP only for literal loopback addresses and localhost.
    pub allow_insecure_loopback_http: bool,
    pub client_id: String,
    pub client_secret: String,
    pub scope: Vec<String>,
    pub principal: String,
    pub extensions: BTreeMap<String, String>,
}
impl std::fmt::Debug for OAuthBearerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthBearerConfig").finish_non_exhaustive()
    }
}
impl OAuthBearerConfig {
    pub fn new(
        endpoint: impl Into<String>,
        id: impl Into<String>,
        secret: impl Into<String>,
    ) -> Self {
        Self {
            token_endpoint: endpoint.into(),
            allow_insecure_loopback_http: false,
            client_id: id.into(),
            client_secret: secret.into(),
            scope: Vec::new(),
            principal: String::new(),
            extensions: BTreeMap::new(),
        }
    }
}

/// Synchronous, thread-affine OIDC client using the shared HTTP transport.
pub struct OAuthBearerHttpClient {
    transport: HttpTransport,
    config: OAuthBearerConfig,
}
impl OAuthBearerHttpClient {
    pub fn new(transport: HttpTransport, config: OAuthBearerConfig) -> Self {
        Self { transport, config }
    }
    /// Obtain a token and validate its lifetime before returning it.
    ///
    /// # Errors
    /// Rejects invalid local token metadata before HTTP, transport failures,
    /// non-success responses, invalid JSON, and expired tokens.
    pub fn request_token(&mut self) -> Result<OAuthBearerToken> {
        validate_endpoint(&self.config)?;
        if self.config.principal.chars().any(char::is_control) {
            return Err(oauth_error("invalid principal"));
        }
        validate_extensions(&self.config.extensions)?;
        let started = unix_millis()?;
        let mut request = HttpRequest::new("POST", &self.config.token_endpoint);
        request.headers.push((
            "Content-Type".into(),
            "application/x-www-form-urlencoded".into(),
        ));
        let mut fields = vec![
            ("grant_type", "client_credentials".to_owned()),
            ("client_id", self.config.client_id.clone()),
            ("client_secret", self.config.client_secret.clone()),
        ];
        if !self.config.scope.is_empty() {
            fields.push(("scope", self.config.scope.join(" ")));
        }
        request.body = fields
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
            .collect::<Vec<_>>()
            .join("&")
            .into_bytes();
        let response = self
            .transport
            .execute(request)
            .map_err(|_| oauth_error("token request failed"))?;
        if !(200..300).contains(&response.status) {
            return Err(oauth_error("token endpoint rejected request"));
        }
        let value: serde_json::Value = serde_json::from_slice(&response.body)
            .map_err(|_| oauth_error("invalid token response JSON"))?;
        let access_token = value["access_token"]
            .as_str()
            .filter(|v| !v.is_empty() && !v.chars().any(char::is_control))
            .ok_or_else(|| oauth_error("missing or invalid access token"))?
            .to_owned();
        if value
            .get("token_type")
            .filter(|value| !value.is_null())
            .is_some_and(|v| !v.as_str().is_some_and(|s| s.eq_ignore_ascii_case("bearer")))
        {
            return Err(oauth_error("unsupported token type"));
        }
        let seconds = value["expires_in"]
            .as_u64()
            .filter(|n| *n > 0)
            .ok_or_else(|| oauth_error("invalid token lifetime"))?;
        let lifetime = seconds
            .checked_mul(1000)
            .and_then(|v| i64::try_from(v).ok())
            .ok_or_else(|| oauth_error("token lifetime overflow"))?;
        let expires_at_ms = started
            .checked_add(lifetime)
            .ok_or_else(|| oauth_error("token expiry overflow"))?;
        if expires_at_ms <= unix_millis()? {
            return Err(oauth_error("token expired during refresh"));
        }
        let scope = match value.get("scope").filter(|value| !value.is_null()) {
            None => self.config.scope.clone(),
            Some(v) => v
                .as_str()
                .ok_or_else(|| oauth_error("invalid token scope"))?
                .split_whitespace()
                .map(str::to_owned)
                .collect(),
        };
        let mut extensions = self.config.extensions.clone();
        if let Some(extra) = value.get("extensions").filter(|value| !value.is_null()) {
            for (key, value) in extra
                .as_object()
                .ok_or_else(|| oauth_error("invalid token extensions"))?
            {
                extensions.insert(
                    key.clone(),
                    value
                        .as_str()
                        .ok_or_else(|| oauth_error("invalid token extension value"))?
                        .to_owned(),
                );
            }
        }
        validate_extensions(&extensions)?;
        if expires_at_ms <= unix_millis()? {
            return Err(oauth_error("token expired during validation"));
        }
        Ok(OAuthBearerToken {
            access_token,
            expires_at_ms,
            principal: self.config.principal.clone(),
            scope,
            extensions,
        })
    }
    /// Refresh from a poll-delivered OAuth callback and install a token or failure.
    ///
    /// # Errors
    /// Returns refresh or installation failures after notifying the native client.
    /// If notification also fails, retains both sanitized failure summaries.
    pub fn refresh(&mut self, target: &mut impl OAuthBearerTokenSetter) -> Result<()> {
        let result = self.request_token().and_then(|token| {
            target.oauthbearer_set_token(&token).map_err(|error| {
                oauth_error(&format!(
                    "token installation failed ({})",
                    failure_category(&error)
                ))
            })
        });
        match result {
            Ok(()) => Ok(()),
            Err(primary) => {
                if let Err(fallback) =
                    target.oauthbearer_set_token_failure("OAuth bearer refresh failed")
                {
                    return Err(oauth_error(&format!(
                        "{primary}; token failure notification failed ({})",
                        failure_category(&fallback)
                    )));
                }
                Err(primary)
            }
        }
    }
}
fn validate_extensions(extensions: &BTreeMap<String, String>) -> Result<()> {
    if extensions.iter().any(|(key, value)| {
        key.is_empty()
            || key == "auth"
            || !key.bytes().all(|byte| byte.is_ascii_alphabetic())
            || value.chars().any(char::is_control)
    }) {
        return Err(oauth_error("invalid SASL extension"));
    }
    Ok(())
}
// Setter errors may contain credentials supplied by the token or native client.
fn failure_category(error: &Error) -> &'static str {
    match error {
        Error::Native { .. } => "native error",
        Error::Config(_) => "configuration error",
        Error::Http(_) => "HTTP error",
        Error::Tls(_) => "TLS error",
        Error::OAuth(_) => "OAuth error",
        Error::Callback(_) => "callback error",
        _ => "client error",
    }
}
fn oauth_error(message: &str) -> Error {
    Error::OAuth(message.into())
}
fn unix_millis() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| oauth_error("clock precedes Unix epoch"))?;
    i64::try_from(elapsed.as_millis()).map_err(|_| oauth_error("clock overflow"))
}
pub(crate) fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
    out
}

fn validate_endpoint(config: &OAuthBearerConfig) -> Result<()> {
    let url = url::Url::parse(&config.token_endpoint)
        .map_err(|_| oauth_error("invalid token endpoint"))?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(oauth_error(
            "token endpoint contains forbidden credentials or fragment",
        ));
    }
    let loopback = match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(host)) => host == "localhost",
        None => false,
    };
    if url.scheme() != "https"
        && !(config.allow_insecure_loopback_http && url.scheme() == "http" && loopback)
    {
        return Err(oauth_error("token endpoint requires HTTPS"));
    }
    Ok(())
}

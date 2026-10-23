//! Verified certificate trust shared by broker and HTTP transports.

use crate::{Error, Result};

/// Trust anchors, peer identity, and optional mutual TLS credentials.
#[derive(Clone)]
pub struct TlsSettings {
    /// Additional PEM-encoded certificate authorities.
    pub ca_pem: Vec<u8>,
    /// Include the platform certificate store.
    pub use_native_roots: bool,
    /// Verify and advertise this name while connecting to the configured endpoint.
    pub server_name: Option<String>,
    /// PEM-encoded client certificate chain.
    pub client_cert_pem: Vec<u8>,
    /// PEM-encoded client private key. Debug output never includes it.
    pub client_key_pem: Vec<u8>,
    /// Request unverified TLS; verified transports reject this setting.
    pub danger_accept_invalid_certs: bool,
}

impl std::fmt::Debug for TlsSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsSettings")
            .field("use_native_roots", &self.use_native_roots)
            .field("server_name", &self.server_name)
            .field("custom_ca", &!self.ca_pem.is_empty())
            .field("client_identity", &!self.client_cert_pem.is_empty())
            .field(
                "danger_accept_invalid_certs",
                &self.danger_accept_invalid_certs,
            )
            .finish_non_exhaustive()
    }
}

impl Default for TlsSettings {
    fn default() -> Self {
        Self {
            ca_pem: Vec::new(),
            use_native_roots: true,
            server_name: None,
            client_cert_pem: Vec::new(),
            client_key_pem: Vec::new(),
            danger_accept_invalid_certs: false,
        }
    }
}

impl TlsSettings {
    /// Use the platform trust store and verify the endpoint name.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Add PEM trust anchors.
    #[must_use]
    pub fn with_ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = pem.into();
        self
    }
    /// Select whether platform trust anchors are included.
    #[must_use]
    pub fn with_native_roots(mut self, enabled: bool) -> Self {
        self.use_native_roots = enabled;
        self
    }
    /// Set the verified TLS peer name independently of its socket address.
    #[must_use]
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }
    /// Provide a PEM certificate chain and matching private key for mutual TLS.
    #[must_use]
    pub fn with_client_identity(
        mut self,
        cert: impl Into<Vec<u8>>,
        key: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_cert_pem = cert.into();
        self.client_key_pem = key.into();
        self
    }

    /// Build verified rustls settings for a synchronous TLS connection.
    ///
    /// # Errors
    /// Returns an error for invalid certificates, mismatched credentials, empty trust,
    /// an invalid peer name, or an attempt to disable verification.
    pub fn build_client_config(&self) -> Result<rustls::ClientConfig> {
        self.validate()?;
        let mut roots = rustls::RootCertStore::empty();
        if self.use_native_roots {
            let native = rustls_native_certs::load_native_certs();
            for cert in native.certs {
                roots
                    .add(cert)
                    .map_err(|_| Error::Tls("invalid native trust anchor".into()))?;
            }
        }
        if !self.ca_pem.is_empty() {
            let certs = rustls_pemfile::certs(&mut self.ca_pem.as_slice())
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| Error::Tls("invalid CA PEM".into()))?;
            if certs.is_empty() {
                return Err(Error::Tls("CA PEM contains no certificates".into()));
            }
            for cert in certs {
                roots
                    .add(cert)
                    .map_err(|_| Error::Tls("invalid CA certificate".into()))?;
            }
        }
        if roots.is_empty() {
            return Err(Error::Tls("no trusted certificate authorities".into()));
        }
        let builder = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| Error::Tls("invalid TLS protocol configuration".into()))?
        .with_root_certificates(roots);
        if self.client_cert_pem.is_empty() {
            return Ok(builder.with_no_client_auth());
        }
        let certs = rustls_pemfile::certs(&mut self.client_cert_pem.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| Error::Tls("invalid client certificate PEM".into()))?;
        let key = rustls_pemfile::private_key(&mut self.client_key_pem.as_slice())
            .map_err(|_| Error::Tls("invalid client private key PEM".into()))?
            .ok_or_else(|| Error::Tls("missing client private key".into()))?;
        builder
            .with_client_auth_cert(certs, key)
            .map_err(|_| Error::Tls("invalid client identity".into()))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.danger_accept_invalid_certs {
            return Err(Error::Tls("unverified TLS is unsupported".into()));
        }
        if self.client_cert_pem.is_empty() != self.client_key_pem.is_empty() {
            return Err(Error::Tls(
                "client certificate and key must be configured together".into(),
            ));
        }
        if let Some(name) = &self.server_name {
            rustls::pki_types::ServerName::try_from(name.as_str())
                .map_err(|_| Error::Tls("invalid TLS server name".into()))?;
        }
        if !self.use_native_roots && self.ca_pem.is_empty() {
            return Err(Error::Tls("no trusted certificate authorities".into()));
        }
        Ok(())
    }
}

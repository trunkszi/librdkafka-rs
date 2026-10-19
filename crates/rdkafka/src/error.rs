//! Structured failures at typed-record and caller-buffer boundaries.

/// A rejected record layout, codec input, or batch shape.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// HTTP request setup, transport, or response validation failed.
    #[error("http: {0}")]
    Http(String),
    /// TLS trust, identity, or handshake validation failed.
    #[error("tls: {0}")]
    Tls(String),
    /// An OAuth token refresh failed closed.
    #[error("oauth: {0}")]
    OAuth(String),
    /// A schema registry request or wire frame was rejected.
    #[error("schema registry: {0}")]
    SchemaRegistry(String),
    /// Calling-thread slot construction, leasing, or lifecycle failed.
    #[error("worker: {0}")]
    Worker(String),
    /// A caller-provided codec or observer rejected the operation.
    #[error(transparent)]
    Callback(#[from] anyhow::Error),
    /// Malformed or unsupported positional data.
    #[error("codec: {0}")]
    Codec(String),
    /// A fixed layout cannot be projected safely.
    #[error("layout: {0}")]
    Layout(String),
    /// A caller-provided destination does not match the expected shape.
    #[error("batch length mismatch: expected {expected}, actual {actual}")]
    BatchLengthMismatch { expected: usize, actual: usize },
}

/// Result shared by typed records, batches, and codecs.
pub type Result<T> = std::result::Result<T, Error>;

impl serde::ser::Error for Error {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::Codec(message.to_string())
    }
}
impl serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::Codec(message.to_string())
    }
}

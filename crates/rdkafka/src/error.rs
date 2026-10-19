//! Native error identity and operation-specific failure flags.
use std::fmt;
/// Lossless numeric identity for every native broker and client error code.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ErrorCode(pub i32);
impl ErrorCode {
    pub const NO_ERROR: Self = Self(0);
    pub const QUEUE_FULL: Self = Self(-184);
    pub const TIMED_OUT: Self = Self(-185);
    pub const PARTITION_EOF: Self = Self(-191);
    pub const INVALID_ARGUMENT: Self = Self(-186);
    pub const UNKNOWN_TOPIC_OR_PARTITION: Self = Self(3);
    pub const FATAL: Self = Self(-150);
    pub fn name(self) -> String {
        rdkafka_sys::ffi::error_name(self.0)
    }
    pub fn message(self) -> String {
        rdkafka_sys::ffi::error_message(self.0)
    }
    /// Classifies code-level retry behavior; operation errors retain richer native flags.
    pub fn is_retriable(self) -> bool {
        rdkafka_sys::ffi::error_flags(self.0) & 1 != 0
    }
    pub fn is_fatal(self) -> bool {
        self == Self::FATAL
    }
    pub fn is_queue_full(self) -> bool {
        self == Self::QUEUE_FULL
    }
    /// Abort requirements belong to operation errors rather than standalone codes.
    pub fn txn_requires_abort(self) -> bool {
        false
    }
    pub fn value(self) -> i32 {
        self.0
    }
}
impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Kafka error {}", self.0)
    }
}
/// An operation failure, preserving transaction-specific native flags.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "REENTRANT_CALLBACK_DRIVE: polling, consumption, and flushing are unavailable inside \
         callbacks"
    )]
    ReentrantDrive,
    #[error("{message} ({code})")]
    Native {
        code: ErrorCode,
        message: String,
        retriable: bool,
        fatal: bool,
        txn_requires_abort: bool,
    },
    #[error("producer queue is full")]
    QueueFull,
    #[error("configuration: {0}")]
    Config(String),
    #[error("record layout: {0}")]
    Layout(String),
    #[error("batch column lengths differ: expected {expected}, actual {actual}")]
    BatchLengthMismatch { expected: usize, actual: usize },
    /// An OAuth token refresh failed closed.
    #[error("oauth: {0}")]
    OAuth(String),
    /// A schema registry request or wire frame was rejected.
    #[error("schema registry: {0}")]
    SchemaRegistry(String),
    /// Calling-thread slot construction, leasing, or lifecycle failed.
    #[error("worker: {0}")]
    Worker(String),
    #[error("codec: {0}")]
    Codec(String),
    #[error("TLS: {0}")]
    Tls(String),
    #[error("HTTP: {0}")]
    Http(String),
    #[error("callback: {0}")]
    Callback(#[from] anyhow::Error),
}
impl Error {
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            Self::Native { code, .. } => Some(*code),
            Self::QueueFull => Some(ErrorCode::QUEUE_FULL),
            _ => None,
        }
    }
    pub fn is_queue_full(&self) -> bool {
        self.code() == Some(ErrorCode::QUEUE_FULL)
    }
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::Native {
                retriable: true,
                ..
            } | Self::QueueFull
        )
    }
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Native { fatal: true, .. })
    }
    pub fn txn_requires_abort(&self) -> bool {
        matches!(
            self,
            Self::Native {
                txn_requires_abort: true,
                ..
            }
        )
    }
    pub(crate) fn native(code: i32, message: impl Into<String>) -> Self {
        let flags = rdkafka_sys::ffi::error_flags(code);
        Self::Native {
            code: ErrorCode(code),
            message: message.into(),
            retriable: flags & 1 != 0 || code == -184,
            fatal: flags & 2 != 0 || code == -150,
            txn_requires_abort: flags & 4 != 0,
        }
    }
}
impl From<cxx::Exception> for Error {
    fn from(value: cxx::Exception) -> Self {
        let text = value.what();
        let (code, message) = text
            .split_once(':')
            .and_then(|(c, m)| c.parse::<i32>().ok().map(|n| (n, m)))
            .unwrap_or((-1, text));
        if let Some(rest) = message.strip_prefix("flags=")
            && let Some((flags, message)) = rest.split_once(':')
            && let Ok(flags) = flags.parse::<u8>()
        {
            return Self::Native {
                code: ErrorCode(code),
                message: message.into(),
                retriable: flags & 1 != 0,
                fatal: flags & 2 != 0,
                txn_requires_abort: flags & 4 != 0,
            };
        }
        Self::native(code, message)
    }
}
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

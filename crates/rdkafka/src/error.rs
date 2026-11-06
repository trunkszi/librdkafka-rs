//! Native error identity and operation-specific failure flags.
use std::fmt;

use uarch::prelude::*;
#[path = "error_codes.rs"]
mod error_codes;
bitword! {
    struct NativeErrorFlags(u8) {
        [0] RETRIABLE,
        [1] FATAL,
        [2] ABORT,
    }
}
/// Lossless numeric identity for every native broker and client error code.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct ErrorCode(pub i32);
assert_layout!(ErrorCode, size = 4, align = align_of::<i32>());
// SAFETY: transparent i32 storage has no padding and every i32 is a valid error identity.
unsafe impl NoPadding for ErrorCode {}
// SAFETY: the sole transparent i32 field is one scalar ABI leaf.
unsafe impl RegisterLeaf for ErrorCode {
    const LEAVES: u8 = 1;
}
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
    #[hot_inline]
    pub fn is_retriable(self) -> bool {
        NativeErrorFlags::new(rdkafka_sys::ffi::error_flags(self.0) as u8)
            .matches(pattern!(NativeErrorFlags { RETRIABLE }))
    }
    #[hot_inline]
    pub fn is_fatal(self) -> bool {
        self == Self::FATAL
    }
    #[hot_inline]
    pub fn is_queue_full(self) -> bool {
        self == Self::QUEUE_FULL
    }
    /// Abort requirements belong to operation errors rather than standalone codes.
    #[hot_inline]
    pub fn txn_requires_abort(self) -> bool {
        false
    }
    #[hot_inline]
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
    #[cold_sink]
    pub(crate) fn codec(message: &str) -> Self {
        Self::Codec(message.into())
    }
    #[cold_sink]
    pub(crate) fn layout(message: &str) -> Self {
        Self::Layout(message.into())
    }
    #[cfg(feature = "http")]
    #[cold_sink]
    pub(crate) fn http(message: &str) -> Self {
        Self::Http(message.into())
    }
    #[hot_inline]
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            Self::Native { code, .. } => Some(*code),
            Self::QueueFull => Some(ErrorCode::QUEUE_FULL),
            _ => None,
        }
    }
    #[hot_inline]
    pub fn is_queue_full(&self) -> bool {
        self.code() == Some(ErrorCode::QUEUE_FULL)
    }
    #[hot_inline]
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::Native {
                retriable: true,
                ..
            } | Self::QueueFull
        )
    }
    #[hot_inline]
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Native { fatal: true, .. })
    }
    #[hot_inline]
    pub fn txn_requires_abort(&self) -> bool {
        matches!(
            self,
            Self::Native {
                txn_requires_abort: true,
                ..
            }
        )
    }
    #[cold_sink]
    pub(crate) fn native(code: i32, message: impl Into<String>) -> Self {
        if code == -184 {
            return Self::QueueFull;
        }
        let flags = NativeErrorFlags::new(rdkafka_sys::ffi::error_flags(code) as u8);
        Self::Native {
            code: ErrorCode(code),
            message: message.into(),
            retriable: flags.matches(pattern!(NativeErrorFlags { RETRIABLE })) || code == -184,
            fatal: flags.matches(pattern!(NativeErrorFlags { FATAL })) || code == -150,
            txn_requires_abort: flags.matches(pattern!(NativeErrorFlags { ABORT })),
        }
    }
}
impl From<cxx::Exception> for Error {
    #[cold_sink]
    fn from(value: cxx::Exception) -> Self {
        let text = value.what();
        let (code, message) = text
            .split_once(':')
            .and_then(|(c, m)| c.parse::<i32>().ok().map(|n| (n, m)))
            .unwrap_or((-1, text));
        if code == -184 {
            return Self::QueueFull;
        }
        if let Some(rest) = message.strip_prefix("flags=")
            && let Some((flags, message)) = rest.split_once(':')
            && let Ok(flags) = flags.parse::<u8>()
        {
            let flags = NativeErrorFlags::new(flags);
            return Self::Native {
                code: ErrorCode(code),
                message: message.into(),
                retriable: flags.matches(pattern!(NativeErrorFlags { RETRIABLE })),
                fatal: flags.matches(pattern!(NativeErrorFlags { FATAL })),
                txn_requires_abort: flags.matches(pattern!(NativeErrorFlags { ABORT })),
            };
        }
        Self::native(code, message)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

impl serde::ser::Error for Error {
    #[cold_sink]
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::Codec(message.to_string())
    }
}
impl serde::de::Error for Error {
    #[cold_sink]
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::Codec(message.to_string())
    }
}

//! Synchronous typed records, reusable column batches, and borrowed Kafka payload codecs.
extern crate self as rdkafka;

pub mod batch;
pub mod buffer_policy;
pub mod column;
pub mod error;
pub mod record;
pub mod serde_bridge;
pub mod wire;

pub use batch::Batch;
pub use buffer_policy::BufferPolicy;
pub use error::{Error, Result};
pub use rdkafka_macros::Record;
pub use record::{Record, RecordView};
pub use serde_bridge::SerdeBridge;
pub use wire::{WireBatch, WireHeader, WireHeaders, WireRecord};

/// Stable dependencies used by generated record code.
#[doc(hidden)]
pub mod __private {
    pub use serde;
    pub use serde_bytes;
    pub use zerocopy;
}

#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "http")]
pub mod oauth;
#[cfg(feature = "http")]
pub mod schema_registry;
pub mod threading;
#[cfg(feature = "tls")]
pub mod tls;
#[cfg(feature = "http")]
pub use http::{HttpPreferences, HttpTransport, HttpVersionChoice};
#[cfg(feature = "http")]
pub use oauth::{OAuthBearerHttpClient, OAuthBearerToken, OAuthBearerTokenSetter};
#[cfg(feature = "http")]
pub use schema_registry::SchemaRegistryClient;
pub use threading::{
    ThreadLocalClient, ThreadLocalConsumer, ThreadLocalFactory, ThreadLocalProducer, WorkerBalance,
    WorkerClient, WorkerConsumer, WorkerFactory, WorkerProducer,
};
#[cfg(feature = "tls")]
pub use tls::TlsSettings;

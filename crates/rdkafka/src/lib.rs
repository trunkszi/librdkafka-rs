//! Synchronous, thread-affine clients backed by statically linked librdkafka.
mod callbacks;
mod children;
mod client;
mod conf;
mod core;
pub mod error;
mod received;
mod types;
pub use callbacks::*;
pub use children::{Queue, Topic};
pub use client::{Consumer, KafkaConsumer, ProduceRequest, Producer, SimpleConsumer};
pub use conf::Conf;
pub use error::{Error, ErrorCode, Result};
pub use received::ReceivedMessage;
pub use types::*;

extern crate self as rdkafka;

pub mod batch;
pub mod buffer_policy;
pub mod column;
pub mod record;
pub mod serde_bridge;
pub mod wire;

pub use batch::Batch;
pub use buffer_policy::BufferPolicy;
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
    ThreadLocalClient, ThreadLocalConsumer, ThreadLocalProducer, WorkerBalance, WorkerClient,
    WorkerConsumer, WorkerProducer, WorkerSlotLease,
};
#[cfg(feature = "tls")]
pub use tls::TlsSettings;
mod transactions;
pub use transactions::{ConsumerGroupMetadata, PurgeFlags};
mod batch_clients;
pub use batch_clients::{BatchProduceResult, ReceivedBatch};
pub mod admin;
pub use admin::*;

//! Synchronous, thread-affine clients backed by statically linked librdkafka.
mod callbacks;
mod children;
mod client;
mod conf;
mod core;
mod error;
mod received;
mod types;
pub use callbacks::*;
pub use children::{Queue, Topic};
pub use client::{Consumer, KafkaConsumer, Producer, SimpleConsumer};
pub use conf::Conf;
pub use error::{Error, ErrorCode, Result};
pub use received::ReceivedMessage;
pub use types::*;

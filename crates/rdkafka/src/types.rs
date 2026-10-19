//! Owned record and metadata projections retaining Kafka null semantics.
use std::{marker::PhantomData, rc::Rc};
/// The native automatic partition selector.
pub const PARTITION_UA: i32 = -1;
#[derive(Clone, Debug, Default)]
pub struct Header {
    pub name: String,
    pub value: Option<Vec<u8>>,
}
#[derive(Clone, Debug, Default)]
pub struct Headers {
    entries: Vec<Header>,
    _thread: PhantomData<Rc<()>>,
}
impl Headers {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn add(&mut self, name: impl Into<String>, value: Option<&[u8]>) -> &mut Self {
        self.entries.push(Header {
            name: name.into(),
            value: value.map(<[u8]>::to_vec),
        });
        self
    }
    pub fn iter(&self) -> impl Iterator<Item = &Header> {
        self.entries.iter()
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
#[derive(Clone, Debug)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: i32,
    /// Leader epoch when known; absent preserves native unknown-epoch semantics.
    pub leader_epoch: Option<i32>,
    /// Opaque broker commit metadata, preserved as bytes.
    pub metadata: Vec<u8>,
    pub(crate) _thread: PhantomData<Rc<()>>,
}
impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32, offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            error: 0,
            leader_epoch: None,
            metadata: Vec::new(),
            _thread: PhantomData,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum Offset {
    Beginning,
    End,
    Stored,
    Invalid,
    Tail(u32),
    Absolute(i64),
}
impl Offset {
    pub fn value(self) -> i64 {
        match self {
            Self::Beginning => -2,
            Self::End => -1,
            Self::Stored => -1000,
            Self::Invalid => -1001,
            Self::Tail(n) => -2000 - i64::from(n),
            Self::Absolute(n) => n,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct Message {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub headers: Headers,
    pub token: u64,
}
#[derive(Clone, Debug, Default)]
pub struct Metadata {
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
    pub(crate) _thread: PhantomData<Rc<()>>,
}
#[derive(Clone, Debug)]
pub struct BrokerMetadata {
    pub id: i32,
    pub host: String,
    pub port: i32,
}
#[derive(Clone, Debug)]
pub struct TopicMetadata {
    pub name: String,
    pub error: i32,
    pub partitions: Vec<PartitionMetadata>,
}
#[derive(Clone, Debug)]
pub struct PartitionMetadata {
    pub id: i32,
    pub error: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isrs: Vec<i32>,
}
/// Borrowed enqueue input; null and empty payloads remain distinct.
#[derive(Debug)]
pub struct ProduceRecord<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub key: Option<&'a [u8]>,
    pub payload: Option<&'a [u8]>,
    pub headers: Option<&'a Headers>,
    pub timestamp: Option<i64>,
    pub token: u64,
}
impl<'a> ProduceRecord<'a> {
    pub fn new(topic: &'a str, payload: Option<&'a [u8]>) -> Self {
        Self {
            topic,
            payload,
            partition: PARTITION_UA,
            key: None,
            headers: None,
            timestamp: None,
            token: 0,
        }
    }
}

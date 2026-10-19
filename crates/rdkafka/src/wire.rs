//! Borrowed Kafka frames and reusable contiguous wire arenas.
use std::{marker::PhantomData, ops::Range, rc::Rc};

use crate::{BufferPolicy, Error, Result, buffer_policy::MemoryBuffer};

/// A Kafka header; duplicate names and null values are preserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireHeader<'a> {
    /// Header name.
    pub name: &'a str,
    /// Null differs from an empty byte string.
    pub value: Option<&'a [u8]>,
}
/// Borrowed headers from a caller slice or wire arena.
#[derive(Debug, Clone, Copy)]
pub struct WireHeaders<'a> {
    source: HeaderSource<'a>,
    affinity: PhantomData<Rc<()>>,
}
#[derive(Debug, Clone, Copy)]
enum HeaderSource<'a> {
    Slice(&'a [WireHeader<'a>]),
    Owned(&'a crate::Headers),
    Arena(&'a [HeaderIndex], &'a [u8]),
}
impl<'a> Default for WireHeaders<'a> {
    fn default() -> Self {
        Self::new(&[])
    }
}
impl<'a> WireHeaders<'a> {
    /// Borrow a caller-owned header list.
    pub const fn new(headers: &'a [WireHeader<'a>]) -> Self {
        Self {
            source: HeaderSource::Slice(headers),
            affinity: PhantomData,
        }
    }
    /// Borrow owned header metadata without copying header names or values.
    pub fn from_headers(headers: &'a crate::Headers) -> Self {
        Self {
            source: HeaderSource::Owned(headers),
            affinity: PhantomData,
        }
    }
    /// Number of headers, including duplicates.
    pub fn len(self) -> usize {
        match self.source {
            HeaderSource::Slice(v) => v.len(),
            HeaderSource::Owned(v) => v.len(),
            HeaderSource::Arena(v, _) => v.len(),
        }
    }
    /// Whether there are no headers.
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
    /// Project one header without allocation.
    pub fn get(self, index: usize) -> Option<WireHeader<'a>> {
        match self.source {
            HeaderSource::Slice(v) => v.get(index).copied(),
            HeaderSource::Owned(v) => {
                let h = v.iter().nth(index)?;
                Some(WireHeader {
                    name: &h.name,
                    value: h.value.as_deref(),
                })
            }
            HeaderSource::Arena(v, bytes) => {
                let entry = v.get(index)?;
                Some(WireHeader {
                    name: std::str::from_utf8(bytes.get(entry.name.clone())?).ok()?,
                    value: match &entry.value {
                        Some(range) => Some(bytes.get(range.clone())?),
                        None => None,
                    },
                })
            }
        }
    }
    /// Iterate in insertion order without allocating.
    pub fn iter(self) -> impl ExactSizeIterator<Item = WireHeader<'a>> + 'a {
        HeaderIter {
            headers: self,
            index: 0,
        }
    }
}
/// A borrowed raw message frame confined to the current thread.
#[derive(Debug, Clone, Copy)]
pub struct WireRecord<'a> {
    /// UTF-8 topic name.
    pub topic: &'a str,
    /// Nullable key bytes.
    pub key: Option<&'a [u8]>,
    /// Nullable payload bytes.
    pub payload: Option<&'a [u8]>,
    /// Ordered, nullable headers.
    pub headers: WireHeaders<'a>,
    /// Partition; -1 requests producer partition selection.
    pub partition: i32,
    /// Broker offset; -1 means unknown.
    pub offset: i64,
    /// Timestamp in Unix milliseconds when present.
    pub timestamp: Option<i64>,
    affinity: PhantomData<Rc<()>>,
}
impl<'a> WireRecord<'a> {
    /// Construct a message with no key, headers, or assigned partition.
    pub fn new(topic: &'a str, payload: Option<&'a [u8]>) -> Self {
        Self {
            topic,
            key: None,
            payload,
            headers: WireHeaders::default(),
            partition: -1,
            offset: -1,
            timestamp: None,
            affinity: PhantomData,
        }
    }
    /// Set nullable key bytes.
    pub fn with_key(mut self, key: Option<&'a [u8]>) -> Self {
        self.key = key;
        self
    }
    /// Set the partition.
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }
    /// Set the broker offset.
    pub fn with_offset(mut self, offset: i64) -> Self {
        self.offset = offset;
        self
    }
    /// Set the optional Unix millisecond timestamp.
    pub fn with_timestamp(mut self, timestamp: Option<i64>) -> Self {
        self.timestamp = timestamp;
        self
    }
    /// Borrow ordered caller-owned headers.
    pub fn with_headers(mut self, headers: &'a [WireHeader<'a>]) -> Self {
        self.headers = WireHeaders::new(headers);
        self
    }
}
#[derive(Debug, Clone)]
struct HeaderIndex {
    name: Range<usize>,
    value: Option<Range<usize>>,
}
#[derive(Debug, Clone)]
struct RecordIndex {
    topic: Range<usize>,
    key: Option<Range<usize>>,
    payload: Option<Range<usize>>,
    headers: Range<usize>,
    partition: i32,
    offset: i64,
    timestamp: Option<i64>,
}
/// Reusable SoA wire storage with four byte arenas and offset tables.
///
/// Clearing invalidates all projections through Rust's borrow rules. No record
/// allocates separately once the arenas and tables have sufficient capacity.
#[derive(Debug, Default)]
pub struct WireBatch {
    topics: Vec<u8>,
    keys: Vec<u8>,
    payloads: Vec<u8>,
    header_bytes: Vec<u8>,
    headers: Vec<HeaderIndex>,
    records: Vec<RecordIndex>,
    affinity: PhantomData<Rc<()>>,
}
impl WireBatch {
    /// Construct empty reusable arenas.
    pub fn new() -> Self {
        Self::default()
    }
    /// Reserve record descriptors; byte arenas grow with their actual contents.
    /// # Panics
    /// Panics if the requested inline allocation exceeds the vector size limit.
    pub fn with_capacity(records: usize) -> Self {
        Self {
            records: Vec::with_capacity(records),
            ..Self::default()
        }
    }
    /// Number of records.
    pub fn len(&self) -> usize {
        self.records.len()
    }
    /// Whether no records are stored.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    /// Retained descriptor capacity in records.
    pub fn capacity(&self) -> usize {
        self.records.capacity()
    }
    /// Total retained arena and descriptor vector capacity in bytes.
    pub fn allocated_bytes(&self) -> usize {
        self.topics
            .allocated_bytes()
            .saturating_add(self.keys.allocated_bytes())
            .saturating_add(self.payloads.allocated_bytes())
            .saturating_add(self.header_bytes.allocated_bytes())
            .saturating_add(self.headers.allocated_bytes())
            .saturating_add(self.records.allocated_bytes())
    }
    /// Append a frame, copying its bytes once into contiguous arenas.
    /// # Errors
    /// Returns an error on size overflow or allocation failure. Existing records
    /// remain valid and no partial record is appended.
    pub fn push(&mut self, record: WireRecord<'_>) -> Result<()> {
        let mut header_bytes = 0usize;
        for header in record.headers.iter() {
            header_bytes = header_bytes
                .checked_add(header.name.len())
                .and_then(|n| n.checked_add(header.value.map_or(0, <[u8]>::len)))
                .ok_or_else(|| Error::Layout("header arena size overflow".into()))?;
        }
        reserve(&mut self.records, 1)?;
        reserve(&mut self.topics, record.topic.len())?;
        reserve(&mut self.keys, record.key.map_or(0, <[u8]>::len))?;
        reserve(&mut self.payloads, record.payload.map_or(0, <[u8]>::len))?;
        reserve(&mut self.headers, record.headers.len())?;
        reserve(&mut self.header_bytes, header_bytes)?;
        let topic = append(&mut self.topics, record.topic.as_bytes());
        let key = record.key.map(|v| append(&mut self.keys, v));
        let payload = record.payload.map(|v| append(&mut self.payloads, v));
        let start = self.headers.len();
        for header in record.headers.iter() {
            let name = append(&mut self.header_bytes, header.name.as_bytes());
            let value = header.value.map(|v| append(&mut self.header_bytes, v));
            self.headers.push(HeaderIndex { name, value });
        }
        self.records.push(RecordIndex {
            topic,
            key,
            payload,
            headers: start..self.headers.len(),
            partition: record.partition,
            offset: record.offset,
            timestamp: record.timestamp,
        });
        Ok(())
    }
    /// Borrow one record without allocation.
    pub fn get(&self, index: usize) -> Option<WireRecord<'_>> {
        let record = self.records.get(index)?;
        Some(WireRecord {
            topic: std::str::from_utf8(self.topics.get(record.topic.clone())?).ok()?,
            key: match &record.key {
                Some(range) => Some(self.keys.get(range.clone())?),
                None => None,
            },
            payload: match &record.payload {
                Some(range) => Some(self.payloads.get(range.clone())?),
                None => None,
            },
            headers: WireHeaders {
                source: HeaderSource::Arena(
                    self.headers.get(record.headers.clone())?,
                    &self.header_bytes,
                ),
                affinity: PhantomData,
            },
            partition: record.partition,
            offset: record.offset,
            timestamp: record.timestamp,
            affinity: PhantomData,
        })
    }
    /// Borrow records in insertion order without allocation.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = WireRecord<'_>> {
        RecordIter {
            batch: self,
            index: 0,
        }
    }
    /// Clear using the default aggregate budget.
    pub fn clear(&mut self) {
        self.clear_with_policy(BufferPolicy::default());
    }
    /// Clear without releasing warmed capacity.
    pub fn clear_unbounded(&mut self) {
        self.clear_with_policy(BufferPolicy::unbounded());
    }
    /// Clear lengths and apply one aggregate record and byte retention budget.
    pub fn clear_with_policy(&mut self, policy: BufferPolicy) {
        let retention = policy.retention(
            self.capacity(),
            self.allocated_bytes(),
            self.records.allocated_bytes(),
        );
        self.records.clear_retained(retention);
        clear_arena(&mut self.headers, retention);
        clear_arena(&mut self.topics, retention);
        clear_arena(&mut self.keys, retention);
        clear_arena(&mut self.payloads, retention);
        clear_arena(&mut self.header_bytes, retention);
    }
    /// Release unused capacity while retaining records.
    pub fn shrink_to_fit(&mut self) {
        self.records.shrink_to_fit();
        self.headers.shrink_to_fit();
        self.topics.shrink_to_fit();
        self.keys.shrink_to_fit();
        self.payloads.shrink_to_fit();
        self.header_bytes.shrink_to_fit();
    }
}
fn reserve<T>(destination: &mut Vec<T>, additional: usize) -> Result<()> {
    destination
        .try_reserve(additional)
        .map_err(|error| Error::Layout(format!("wire arena reservation failed: {error}")))
}
fn append(destination: &mut Vec<u8>, bytes: &[u8]) -> Range<usize> {
    let start = destination.len();
    destination.extend_from_slice(bytes);
    start..destination.len()
}

struct HeaderIter<'a> {
    headers: WireHeaders<'a>,
    index: usize,
}
impl<'a> Iterator for HeaderIter<'a> {
    type Item = WireHeader<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let value = self.headers.get(self.index)?;
        self.index += 1;
        Some(value)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.headers.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}
impl ExactSizeIterator for HeaderIter<'_> {}
struct RecordIter<'a> {
    batch: &'a WireBatch,
    index: usize,
}
impl<'a> Iterator for RecordIter<'a> {
    type Item = WireRecord<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let value = self.batch.get(self.index)?;
        self.index += 1;
        Some(value)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batch.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}
impl ExactSizeIterator for RecordIter<'_> {}

/// A transactional writer appending directly to a batch's reusable arenas.
///
/// Instances are supplied only for the duration of `WireBatch::push_with`.
/// Codec closures must append to their byte buffer and must preserve its prefix.
pub struct WireRecordWriter<'a> {
    batch: &'a mut WireBatch,
    key: Option<Range<usize>>,
    payload: Option<Range<usize>>,
}
impl WireRecordWriter<'_> {
    /// Append nullable raw key bytes.
    /// # Errors
    /// Returns an error if arena capacity cannot be reserved.
    pub fn key(&mut self, value: Option<&[u8]>) -> Result<()> {
        self.key = match value {
            Some(value) => {
                reserve(&mut self.batch.keys, value.len())?;
                Some(append(&mut self.batch.keys, value))
            }
            None => None,
        };
        Ok(())
    }
    /// Append nullable raw payload bytes, including direct layout projections.
    /// # Errors
    /// Returns an error if arena capacity cannot be reserved.
    pub fn payload(&mut self, value: Option<&[u8]>) -> Result<()> {
        self.payload = match value {
            Some(value) => {
                reserve(&mut self.batch.payloads, value.len())?;
                Some(append(&mut self.batch.payloads, value))
            }
            None => None,
        };
        Ok(())
    }
    /// Encode a non-null key directly into its arena.
    /// # Errors
    /// Returns the codec error, or an error if the codec shortened the buffer.
    pub fn encode_key(&mut self, encode: impl FnOnce(&mut Vec<u8>) -> Result<()>) -> Result<()> {
        self.key = Some(encode_range(&mut self.batch.keys, encode)?);
        Ok(())
    }
    /// Encode a non-null payload directly into its arena.
    /// # Errors
    /// Returns the codec error, or an error if the codec shortened the buffer.
    pub fn encode_payload(
        &mut self,
        encode: impl FnOnce(&mut Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        self.payload = Some(encode_range(&mut self.batch.payloads, encode)?);
        Ok(())
    }
    /// Encode one non-null header value directly into the header arena.
    /// # Errors
    /// Returns allocation failures, codec errors, or prefix-shortening errors.
    pub fn encode_header(
        &mut self,
        name: &str,
        encode: impl FnOnce(&mut Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        reserve(&mut self.batch.headers, 1)?;
        reserve(&mut self.batch.header_bytes, name.len())?;
        let name = append(&mut self.batch.header_bytes, name.as_bytes());
        let value = Some(encode_range(&mut self.batch.header_bytes, encode)?);
        self.batch.headers.push(HeaderIndex { name, value });
        Ok(())
    }
    /// Append one nullable raw header value, preserving order and duplicates.
    /// # Errors
    /// Returns an error if arena or descriptor capacity cannot be reserved.
    pub fn header(&mut self, name: &str, value: Option<&[u8]>) -> Result<()> {
        let bytes = name
            .len()
            .checked_add(value.map_or(0, <[u8]>::len))
            .ok_or_else(|| Error::Layout("header size overflow".into()))?;
        reserve(&mut self.batch.headers, 1)?;
        reserve(&mut self.batch.header_bytes, bytes)?;
        let name = append(&mut self.batch.header_bytes, name.as_bytes());
        let value = value.map(|v| append(&mut self.batch.header_bytes, v));
        self.batch.headers.push(HeaderIndex { name, value });
        Ok(())
    }
}
impl WireBatch {
    /// Encode a frame directly into retained arenas without temporary buffers.
    ///
    /// On error all appended bytes and descriptors are rolled back; allocation
    /// capacity remains available. Encoder closures must only append bytes.
    /// # Errors
    /// Returns allocation failures and encoder errors unchanged. A closure that
    /// shortens its arena is rejected, but cannot restore overwritten prefixes.
    pub fn push_with(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        timestamp: Option<i64>,
        encode: impl FnOnce(&mut WireRecordWriter<'_>) -> Result<()>,
    ) -> Result<()> {
        reserve(&mut self.records, 1)?;
        reserve(&mut self.topics, topic.len())?;
        let checkpoint = (
            self.topics.len(),
            self.keys.len(),
            self.payloads.len(),
            self.header_bytes.len(),
            self.headers.len(),
        );
        let topic = append(&mut self.topics, topic.as_bytes());
        let mut writer = WireRecordWriter {
            batch: self,
            key: None,
            payload: None,
        };
        match encode(&mut writer) {
            Ok(()) => {
                writer.batch.records.push(RecordIndex {
                    topic,
                    key: writer.key,
                    payload: writer.payload,
                    headers: checkpoint.4..writer.batch.headers.len(),
                    partition,
                    offset,
                    timestamp,
                });
                Ok(())
            }
            Err(error) => {
                writer.batch.topics.truncate(checkpoint.0);
                writer.batch.keys.truncate(checkpoint.1);
                writer.batch.payloads.truncate(checkpoint.2);
                writer.batch.header_bytes.truncate(checkpoint.3);
                writer.batch.headers.truncate(checkpoint.4);
                Err(error)
            }
        }
    }
}
fn encode_range(
    destination: &mut Vec<u8>,
    encode: impl FnOnce(&mut Vec<u8>) -> Result<()>,
) -> Result<Range<usize>> {
    let start = destination.len();
    encode(destination)?;
    if destination.len() < start {
        return Err(Error::Codec("encoder shortened arena prefix".into()));
    }
    Ok(start..destination.len())
}

fn clear_arena<T>(arena: &mut Vec<T>, retention: crate::buffer_policy::Retention) {
    let target = retention.target(arena.capacity());
    arena.clear();
    arena.shrink_to(target);
}

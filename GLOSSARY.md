# librdkafka-rs

A general-purpose, synchronous, thread-affine Apache Kafka client SDK for Rust built on statically linked `librdkafka` (`cxx`), `rustls`, and `zerocopy`.

## Language

### Core Data Model

**Record**:
A strongly typed Kafka message schema derived via `#[derive(Record)]`, mapping struct fields to Kafka key, payload, headers, partition, and timestamp.
_Avoid_: Row, MessageStruct, Dto

**RecordView**:
A borrowed, zero-copy read projection (`RecordView<'a>` and derived `<T>RecordRef<'a>`) over an in-place broker or batch buffer without heap allocation.
_Avoid_: BorrowedMessage, MessageView, CppMessageView

**Batch**:
A reusable Structure-of-Arrays (SoA) column buffer (`Batch` trait and derived `<T>Batch`) that accumulates typed records in contiguous per-field vectors for amortized batch production and consumption.
_Avoid_: RecordVec, MessageList, Block

**BufferPolicy**:
A retention budget (`max_records` and `max_bytes`, defaulting to 65,536 records and 8 MiB) that governs how much allocated capacity `Batch` and `WireBatch` retain across `clear_with_policy` cycles.
_Avoid_: CapacityConfig, ShrinkPolicy

**SerdeBridge**:
A stateless adapter enabling types that only implement `serde::Serialize` and `serde::Deserialize<'de>` to encode into reusable caller buffers and decode with zero-copy slice borrowing without `#[derive(Record)]`.
_Avoid_: SerdeCodec, SerdeAdapter

### Client & Broker Handles

**Producer**:
A synchronous, thread-affine (`!Send + !Sync`) Kafka producer wrapping `RdKafka::Producer` with support for single-record, zero-copy, SoA batch, and transactional publishing.
_Avoid_: FutureProducer, BaseProducer, ThreadedProducer

**Consumer**:
A synchronous, thread-affine (`!Send + !Sync`) high-level consumer-group client wrapping `RdKafka::KafkaConsumer` with support for eager and cooperative-incremental rebalancing, batch consumption, and explicit offset management (`KafkaConsumer` is exported as a type alias).
_Avoid_: StreamConsumer, BaseConsumer

**SimpleConsumer**:
A synchronous, thread-affine (`!Send + !Sync`) low-level partition consumer wrapping `RdKafka::Consumer` for direct partition/offset consumption without consumer-group coordination.
_Avoid_: LegacyConsumer, RawConsumer

**AdminClient**:
A synchronous, thread-affine (`!Send + !Sync`) cluster administration client driven by `RdKafka::Queue` polling for topic, partition, configuration, consumer-group, offset, record-deletion, and ACL operations.
_Avoid_: AsyncAdminClient, ClusterManager

**Queue**:
A thread-affine handle to an `RdKafka::Queue` (`rd_kafka_queue_t`) supporting partition queue forwarding, direct polling, and OS file-descriptor wakeup (`io_event_enable`).
_Avoid_: EventChannel, PollStream

**TopicPartition**:
A `(topic, partition, offset, leader_epoch, metadata, error)` descriptor wrapping `RdKafka::TopicPartition` used for assignments, seeks, watermarks, and offset commits.
_Avoid_: PartitionOffset, Tp

### HTTP & Security Extensions

**TlsSettings**:
A `rustls`-backed TLS configuration (CA PEM, native roots, SNI override, and mTLS client certificate/key) shared across broker `SSL`/`SASL_SSL` connections and HTTP/2 transports.
_Avoid_: SslConfig, OpenSslSettings

**SchemaRegistryClient**:
A synchronous, thread-affine HTTP/2 + `rustls` client built on `curl::EventLoop` that caches schemas on the calling thread and frames Confluent wire-format payloads.
_Avoid_: AsyncSchemaRegistry, SrClient

### Wire & FFI Boundary

**WireRecord**:
A single raw Kafka message frame across the `cxx::bridge` boundary containing borrowed slices for topic, key, payload, headers, partition, offset, and timestamp.
_Avoid_: CppMessage, RawRkMessage

**WireBatch**:
A contiguous arena-backed collection of raw message frames (`key` arena, `payload` arena, `header` arena, and offset tables) passed across the FFI boundary in a single vectorized call.
_Avoid_: CppBatch, RawBatch

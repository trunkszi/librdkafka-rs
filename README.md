# librdkafka-rs

A synchronous Kafka client SDK with thread-affine Rust handles and a statically linked
`librdkafka` C++/C core. Producers, group consumers, partition consumers, topics, queues, headers,
and metadata stay on their creating thread. Delivery and local observer callbacks run when the
caller drives the client.

```rust,no_run
use rdkafka::{Conf, ProduceRecord, Producer};

fn main() -> rdkafka::Result<()> {
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", "127.0.0.1:9092")?;
    conf.delivery_report_cb(|report| eprintln!("delivery: {report:?}"));
    let producer = Producer::new(&conf)?;
    producer.produce(ProduceRecord::new("events", Some(b"hello")))?;
    producer.flush(5_000)
}
```

## Native build

The build needs Git, CMake, a C/C++17 toolchain, and the Rust toolchain declared in
`rust-toolchain.toml`. It builds static Kafka, zlib, and zstd archives; lz4, snappy, and crc32c use
Kafka's bundled implementations. OpenSSL, native curl, and asynchronous Rust runtimes are absent.

Kafka source resolution checks `LIBRDKAFKA_DIR`, then `vendor/librdkafka`, then a verified source
checkout in `target/vendor-cache/librdkafka`. Automatic downloads use pinned revisions. A source
override must contain `src-cpp/rdkafkacpp.h`.

The native core supports plaintext broker connections. OAuth refresh callbacks and token-setting
methods are exposed, but this OpenSSL-free build does not implement SCRAM/OAuth crypto or broker
TLS. Unsupported native capabilities return errors.

## Callback ownership

Local callbacks can capture `Rc<RefCell<_>>`. Partition selection runs on the calling thread before
an unassigned-partition record is enqueued. Native fallback partitioners do not access local Rust
callback state. Direct native socket/open hooks require `Send + Sync`; their local observers are
drained during caller polling. Observer queue overflow returns a named error.

Consumed owned messages preserve null and empty keys, payloads, and duplicate header names.
Native-backed receive methods additionally expose borrowed payload/key slices while retaining
native storage and its client owner. Topic and queue children retain their parent resources.

## Validation

```sh
bash .github/scripts/ci-incremental.sh origin/main
```

The gate checks source size, dependency syntax, resolved dependency bans, formatting, Clippy,
mock-broker tests, doctests, and a live Docker Redpanda round trip for all five compression modes.
Each live test container is scoped to that invocation and removed when it exits.

To reuse an existing plaintext broker:

```sh
RDKAFKA_TEST_BROKERS=127.0.0.1:9092 bash .github/scripts/test-redpanda.sh
```


Synchronous, thread-affine Kafka record schemas, reusable Structure-of-Arrays
batches, and caller-buffer binary codecs. Native broker clients are integrated
separately from the typed-record and codec layer.

## Typed records

```rust
use rdkafka::{Record, WireBatch};

#[derive(Record)]
struct Event {
    #[record(key)]
    account: String,
    #[record(timestamp)]
    timestamp_ms: i64,
    #[record(header = "trace")]
    trace: Option<String>,
    text: String,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

let event = Event {
    account: "a".into(), timestamp_ms: 1234, trace: None,
    text: "hello".into(), data: vec![1, 2, 3],
};
let mut wire = WireBatch::new();
event.encode_into(&mut wire)?;
let frame = wire.get(0).unwrap();
let view = Event::view(frame)?;
assert_eq!(view.text, "hello");
let mut columns = EventBatch::with_capacity(64);
columns.push_ref(view)?;
assert_eq!(columns.column_account(), &["a"]);
# Ok::<(), rdkafka::Error>(())
```

`#[kafka(...)]` overrides `#[record(...)]`, which overrides the supported serde
attributes. Payload fields are positional; metadata fields are carried in the
Kafka key, timestamp, partition, or headers. String and byte metadata use raw
UTF-8/bytes. Numeric metadata uses the positional codec. Null and empty key,
payload, and header values remain distinct. Timestamps are Unix milliseconds.

Derivation generates `EventRecordRef<'a>`, `EventBatch`, and a `Record`
implementation. Structs use named fields and own their storage; broker-buffer
borrows live in the generated projection. Generic vectors use an
`EncodedSequence` view with fallible lazy iteration; no aligned `&[T]` cast is
attempted on positional bytes. Nested vectors and optional elements retain their
borrowed structure until explicitly materialized. Tuple, map, and custom fields
keep their owned serde projections and can allocate. `ColumnValue` support for
those types concerns allocation accounting, not a borrowed representation. `codec = "zerocopy"` requires all five
layout traits: `FromBytes`, `IntoBytes`, `KnownLayout`, `Immutable`, and `Unaligned`.
It projects an exact-size payload directly and rejects metadata annotations.

## Serde without a derive macro

`SerdeBridge::encode_into` appends into a caller-owned `Vec<u8>` and rolls back
its appended bytes on serialization failure. `decode_borrowed` accepts
`Deserialize<'de>` and checks that exactly one payload was consumed. Strings and
bytes borrow from input; borrowed `Cow` fields require serde's `borrow` attribute
(and `serde_bytes` for byte Cow fields).

The built-in format is positional: fixed-width primitives are little-endian,
lengths use canonical unsigned LEB128, options use `0`/`1` absent/present tags,
and enum discriminants are little-endian `u32`. Names and rename attributes are
schema metadata rather than on-wire field names. Both peers must use the same
field order and schema. Self-describing decoding and conditional field omission
are unsupported.

`encode_batch_into` reuses caller scratch and appends to a `WireBatch`.
`decode_batch_with` streams callbacks without an intermediate vector;
`decode_batch_into` appends owned or borrowed values, and
`decode_batch_into_slice` fills existing slots. A later record failure leaves
earlier successful records delivered. `_using` variants accept custom codec
closures and preserve their errors. Encode closures must append without changing
the existing prefix. Configurable `DecodeLimits` bound nesting and collection
work for untrusted payloads.

## Memory and execution

`WireBatch` owns contiguous topic, key, payload, and header arenas with offset
tables. The derive writes directly into these arenas; a failed encode rolls
back appended data. The storage keeps no pointer into caller scratch.

`BufferPolicy` defaults to 65,536 retained record slots and 8 MiB aggregate
allocated capacity. `clear()` applies that policy, `clear_unbounded()` retains
warm allocations, and `shrink_to_fit()` releases unused storage. Typed columns
retain initialized variable-length slots and reuse their allocations. Changing
an `Option` from `None` to allocated `Some`, or exceeding warmed capacity, may
allocate. Materializing `EncodedSequence` fields into owned batch columns also
allocates temporary collections; borrowed view construction and iteration stay
allocation-free. Custom column types must implement `ColumnValue` accurately.

Batches and borrowed wire/schema views are `!Send + !Sync`. Codecs are
synchronous and invoke callbacks on the calling thread. The optional `tracing`
feature emits payload-size diagnostics without recording payload contents.

## Validation

Run `bash .github/scripts/ci-incremental.sh origin/main`. Tests exercise golden
bytes, borrow provenance, malformed input, derive diagnostics, retention,
thread-affinity, and warmed allocation behavior. Native producer/consumer and
live broker validation require the native-client integration.

## Calling-thread HTTP extensions

Enable `http` for `HttpTransport`, `OAuthBearerHttpClient`, and
`SchemaRegistryClient`. The transport owns a persistent `curl::EventLoop` and
rejects curl runtimes without HTTP/2 or the rustls TLS backend. Preferences
control HTTP version negotiation, `TCP_NODELAY`, connection reuse, and
multiplexing. Requests have positive deadlines and bounded response bodies and
headers; synchronous DNS resolution precedes the transfer deadline.

`TlsSettings` is shared with the broker transport. The standalone `tls` feature
exposes its verified `build_client_config()` helper without enabling HTTP.
Custom CA PEM, native trust roots, verified SNI override, and client identity
must agree with curl's TLS options. Unverified TLS is rejected. Linux HTTP mTLS
uses sealed anonymous memfd files because curl's rustls backend requires
certificate/key filenames; these descriptors stay alive with the transport.
Other platforms reject HTTP mTLS until equivalent backing is provided.

OAuth client-credentials refresh runs synchronously and reports success or
failure through `OAuthBearerTokenSetter`. Native poll callbacks can call
`refresh` with their token setter. Configure the native principal and extensions
explicitly. HTTPS is required by default; cleartext loopback endpoints require
explicit opt-in for local development. Errors and Debug output omit access
tokens, client secrets, and server response bodies.

Schema Registry operations are `register_schema`, `get_schema_by_id`,
`get_latest_subject_schema`, and `lookup_schema_id`. Caches include both subject
and schema language/definition; latest-version requests reach the registry.
`encode_confluent` replaces a caller-owned byte buffer with the magic byte,
big-endian schema ID, and payload; `decode_confluent` borrows the payload.

`ThreadLocalClient` and `WorkerClient` are shareable managers when their factory
is `Send + Sync`. Share a reference or an `Arc` directly across OS threads; each
thread lazily creates its own `!Send + !Sync` client or slot pool. Clones share
manager identity and the local pool. `WorkerBalance`
supports round robin, least-in-flight, and FNV-1a key routing. Fallible leases and
RAII restore local ownership after callback failure or panic. Public
`WorkerSlotLease` guards dereference to the client, remain `!Send + !Sync`, and
release their slots on drop. `reset()` releases only the calling thread's pool;
call it explicitly if clients retain their manager and create an ownership cycle. After the last manager drops, remote caches are reclaimed
on their next manager access or thread exit. No client state crosses threads.

HTTP tests include real HTTP/2/rustls, mTLS, OIDC, and an isolated Docker Redpanda
Schema Registry. The live test starts and removes its own container unless
`RDKAFKA_SCHEMA_REGISTRY_URL` selects an existing test registry. Docker is a CI
prerequisite; a missing service is a failure, not a passing skipped test.

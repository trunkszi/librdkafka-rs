# librdkafka-rs

A synchronous Kafka client SDK with thread-affine Rust handles and a statically linked
`librdkafka` C++/C core. Producers, group consumers, partition consumers, topics, queues, headers,
and metadata stay on their creating thread. Delivery and local observer callbacks run when the
caller drives the client.

## Workspace

| Crate | Responsibility |
| --- | --- |
| `rdkafka` | Public synchronous clients, records, batches, TLS, and optional HTTP services |
| `rdkafka-macros` | `#[derive(Record)]` record views and SoA batch generation |
| `rdkafka-sys` | Static CMake native core, `cxx` C++/C bridge, and ring-backed SASL crypto |

## Features

| Feature | Dependencies and behavior |
| --- | --- |
| Default (`[]`) | Core Kafka clients, typed records, batches, and OpenSSL-free native SASL |
| `tls` | Verified broker TLS through `rustls`, `rustls-native-certs`, and `rustls-pemfile` |
| `http` | Implies `tls`; enables HTTP/2 `curl`, `url`, `serde_json`, and Linux `memfd` identity backing |
| `tracing` | Payload-size and operation diagnostics without payload contents |

The optional `curl` dependency is pinned by `Cargo.lock` from
`https://github.com/trunkszi/curl-rust.git`, branch `feat/event-loop-ws`, with
`http2`. HTTP transport verifies that the linked curl runtime uses rustls.
Neither OpenSSL crates nor async runtimes are used.

## Core synchronous clients

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

## Build and native source resolution

The build needs Git, CMake, a C/C++17 toolchain, and the Rust toolchain declared in
`rust-toolchain.toml`. The Kafka configuration sets `RDKAFKA_BUILD_STATIC=ON`, `WITH_SSL=OFF`, and
`WITH_CURL=OFF`. It builds static Kafka, zlib, and zstd archives; lz4, snappy, and crc32c use
Kafka's bundled implementations. The native Kafka build excludes OpenSSL and native curl;
optional HTTP services use the separate rustls-backed curl transport. Asynchronous Rust runtimes
are absent.

Kafka source resolution checks `LIBRDKAFKA_DIR`, then `../../vendor/librdkafka` relative to
`crates/rdkafka-sys` (the workspace `vendor/librdkafka`), then a verified source
checkout in `target/vendor-cache/librdkafka` under the workspace or configured target directory. Automatic downloads use pinned revisions. A source
override must contain `src-cpp/rdkafkacpp.h`.

The native core supports plaintext, SASL/PLAIN, SCRAM-SHA-256/512, and OAuth bearer
callbacks without OpenSSL. The native SASL providers use ring-backed SHA, HMAC,
PBKDF2, and secure random primitives. Native source adaptations are confined to
private build output; the pinned source cache stays unchanged.

Enable the `tls` feature for verified broker `SSL` and `SASL_SSL` connections:

```rust,no_run
use rdkafka::{Conf, Producer, TlsSettings};

fn main() -> rdkafka::Result<()> {
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", "broker.example:9093")?
        .set("security.protocol", "SSL")?;
    conf.tls_settings(TlsSettings::new())?;
    let producer = Producer::new(&conf)?;
    producer.poll(0)?;
    Ok(())
}
```

`TlsSettings` supports native roots, additional CA PEM, an explicit verified SNI
name, and client certificate/key pairs. Verification cannot be disabled. Requesting
SSL without verified settings fails before client creation. Native connect/close
hooks bridge each connection through a local Unix socket pair and a private
bounded rustls pump. Each pump owns its TLS state; user callbacks stay on the
calling thread. Connection close stops and joins the pump even under backpressure.
Certificate and handshake failures produce `Error::Tls` and caller-thread
`Event::Error` diagnostics. TLS connections currently require Unix sockets.

Run the real TLS/mTLS and SASL plaintext/TLS broker matrix with
`bash .github/scripts/test-broker-security.sh`. The harness creates ephemeral
rcgen identities and removes only its own Docker container and fixture directory.

## Callback ownership

Local callbacks can capture `Rc<RefCell<_>>`. Partition selection runs on the calling thread before
an unassigned-partition record is enqueued. Native fallback partitioners do not access local Rust
callback state. Direct native socket/open hooks require `Send + Sync`; their local observers are
drained during caller polling. Observer queue overflow returns a named error.

Consumed owned messages preserve null and empty keys, payloads, and duplicate header names.
Native-backed receive methods additionally expose borrowed payload/key slices while retaining
native storage and its client owner. Topic and queue children retain their parent resources.

## Transactions, administration, and batch receive

Producers expose transactional initialization, begin/commit/abort, transactional
consumer offsets, and purge. Native transaction errors retain retriable, fatal,
and abort-required flags. `AdminClient` polls dedicated completion queues for
resource operations and drains calling-thread observers after each request.

A registered rebalance callback owns assignment changes. Apply `assign`/`unassign`
or cooperative `incremental_assign`/`incremental_unassign` inside the callback;
without a callback the native client applies its default assignment behavior.

`produce_wire_batch` crosses the Rust/native boundary once and enqueues records
independently with native `produceva`; per-record failures do not roll back other
enqueues. `consume_batch` uses one native batch queue call and retains native
receive buffers. Use `ReceivedBatch::visit_from` when failed processing must retain
unvisited records. The convenience `consume_for_each` and `consume_views` consume
the entire fetched batch even when a visitor fails. For manual acknowledgement,
disable automatic offset storage and commit only successfully processed offsets.

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

`Producer::produce_serde_batch`, `produce_batch_using`, and
`produce_batch_using_with_policy` reuse caller-owned wire and byte scratch buffers.
`Consumer::consume_serde_with`, `consume_with_using`, `consume_into_using`, and
`consume_into_slice_using` provide corresponding decode/visitor paths.

`SerdeBridge::encode_batch_into` reuses caller scratch and appends to a `WireBatch`.
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

## Calling-thread HTTP extensions

Enable `http` for `HttpTransport`, `OAuthBearerHttpClient`, and
`SchemaRegistryClient`. The transport owns a persistent `curl::EventLoop` and
rejects curl runtimes without HTTP/2 or the rustls TLS backend. Preferences
control HTTP version negotiation, `TCP_NODELAY`, connection reuse, and
multiplexing. Requests have positive deadlines and bounded response bodies and
headers; synchronous DNS resolution precedes the transfer deadline.

`TlsSettings` is shared by broker and HTTP trust and identity. The standalone `tls` feature
exposes its verified `build_client_config()` helper without enabling HTTP.
Custom CA PEM, native trust roots, verified SNI override, and client identity
must agree with curl's TLS options. Unverified TLS is rejected. Linux HTTP mTLS
uses sealed anonymous memfd files because curl's rustls backend requires
certificate/key filenames; these descriptors stay alive with the transport.
Other platforms reject HTTP mTLS until equivalent backing is provided.

OAuth client-credentials refresh runs synchronously and reports success or
failure through `OAuthBearerTokenSetter`. Native poll callbacks can call
`refresh` directly with a `Producer`, `Consumer`, or `SimpleConsumer` token target. Configure the native principal and extensions
explicitly. HTTPS is required by default; cleartext loopback endpoints require
explicit opt-in for local development. Errors and Debug output omit access
tokens, client secrets, and server response bodies.

Schema Registry operations are `register_schema`, `get_schema_by_id`,
`get_latest_subject_schema`, and `lookup_schema_id`. Caches include both subject
and schema language/definition; latest-version requests reach the registry.
`encode_confluent` replaces a caller-owned byte buffer with the magic byte,
big-endian schema ID, and payload; `decode_confluent` borrows the payload.

HTTP tests include real HTTP/2/rustls, mTLS, OIDC, and an isolated Docker Redpanda
Schema Registry. The live test starts and removes its own container unless
`RDKAFKA_SCHEMA_REGISTRY_URL` selects an existing test registry. Docker is a CI
prerequisite; a missing service is a failure, not a passing skipped test.

## Scaling across OS threads

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

## Verification

```sh
bash .github/scripts/check-source-contracts.sh
python3 .github/scripts/test-ci-contracts.py
cargo test -p rdkafka --doc
cargo test -p rdkafka --features http --doc
bash .github/scripts/ci-incremental.sh origin/main
bash .github/scripts/test-redpanda.sh
bash .github/scripts/test-broker-security.sh
```

The gate checks source size, dependency syntax, resolved dependency bans, formatting, Clippy,
mock-broker tests, doctests, typed-record codecs and retention, real HTTP/2 TLS/OIDC,
Schema Registry, and a live Docker Redpanda round trip for all five compression modes.
Each live test container is scoped to that invocation and removed when it exits.

To reuse an existing plaintext broker:

```sh
RDKAFKA_TEST_BROKERS=127.0.0.1:9092 bash .github/scripts/test-redpanda.sh
```

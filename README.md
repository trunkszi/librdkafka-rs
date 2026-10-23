# librdkafka-rs

A synchronous Kafka client SDK with thread-affine Rust handles and a statically linked
`librdkafka` C++/C core. Producers, group consumers, partition consumers, topics, queues, headers,
and metadata stay on their creating thread. Delivery and local observer callbacks run when the
caller drives the client.

## Why librdkafka-rs?

A Kafka worker has to distinguish a record accepted into a producer queue from
one acknowledged by the broker, and a fetched record from one whose processing
and offset commit are complete. In a low-latency, per-worker pipeline, those
transitions should be visible alongside the business logic. Moving them through
an executor adds scheduling decisions and synchronous/asynchronous handoffs that
can make tail latency harder to control.

Rust-rdkafka's [StreamConsumer and FutureProducer](https://docs.rs/rdkafka/latest/rdkafka/)
provide asynchronous streams and delivery futures. Their Futures, Wakers and
runtime polling can spread a consume-process-commit loop across `.await` points.
Stepping from a delivery report or cooperative rebalance into executor machinery
makes ownership changes harder to follow in a debugger. AI-assisted edits face
the same difficulty: generated code can confuse enqueue success with delivery,
commit before processing completes, or keep using a partition after revocation.
A small, explicit calling-thread loop makes those mistakes easier to review.

Dependency scope matters for these workers too. Rust-rdkafka declares
[serde_json](https://github.com/fede1024/rust-rdkafka/blob/master/Cargo.toml),
while its [optional native TLS and HTTP features](https://github.com/fede1024/rust-rdkafka/blob/master/rdkafka-sys/Cargo.toml)
can bring OpenSSL and curl into the build. This SDK excludes those backends:
broker TLS uses rustls, and optional OAuth and Schema Registry HTTP runs through
nexora-rs on a single-thread Proactor with sonic-rs JSON. These services need no
Rust asynchronous runtime.

`librdkafka-rs` makes `Producer` and `Consumer` synchronous and thread-affine
(`!Send + !Sync`). Polling, delivery reports and local rebalance callbacks run
on the owning worker, where assignment and offset decisions can be inspected
together. `#[derive(Record)]` supplies borrowed record views and SoA batches;
the `zerocopy` codec provides validated zero-copy payload views, while reusable
batch storage limits repeated allocation. Native librdkafka still owns broker networking threads, while the
Rust client handles and local callback dispatch retain their worker ownership.

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
| `http` | Implies `tls`; enables completion-driven `nexora` HTTP and `sonic-rs` JSON |
| `tracing` | Payload-size and operation diagnostics without payload contents |

The optional `nexora` dependency follows
`https://github.com/trunkszi/nexora-rs.git`, branch `main`; `Cargo.lock` pins its
revision. JSON processing uses `sonic-rs` 0.5.10. HTTP sockets and c-ares DNS use private
io_uring proactors on the calling thread without an asynchronous Rust runtime. Broker
networking remains the statically linked native Kafka core.

```toml
[dependencies]
rdkafka = { git = "https://github.com/trunkszi/librdkafka-rs.git", branch = "main", features = ["http"] }
```

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
Kafka's bundled implementations. The native Kafka build excludes OpenSSL and its HTTP
backend; optional HTTP services use Nexora's io_uring, c-ares, and rustls stack.
HTTP requires Linux with usable io_uring and the native c-ares build prerequisites.

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

Provision TLS/mTLS and SASL plaintext/TLS broker listeners, then run
`cargo test -p rdkafka --features tls --test sasl_live --locked -- --ignored --test-threads 1`.
The environment variables and PEM inputs are documented in
[`sasl_live.rs`](crates/rdkafka/tests/sasl_live.rs).

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
`SchemaRegistryClient`. The synchronous public API drives a persistent Nexora
`HttpClient` and private `Proactor` on the caller's thread; callbacks stay on
that thread. Ordinary name resolution uses c-ares. Response headers are bounded
to 65,536 bytes, including decoded HTTP/2 fields, and each request has an
independent response body limit. Header-limit diagnostics are preserved across
HTTP versions.
Timeouts must be positive and at most `i32::MAX` milliseconds; fractional
milliseconds round up. Redirects are not followed automatically.

`HttpPreferences` selects HTTP/1.1 (`Http1` or `Http1Only`), TLS ALPN HTTP/2
(`Http2Tls`, requiring an HTTP/2 response over HTTPS), cleartext prior knowledge
(`Http2PriorKnowledge`), or HTTP/2 with
HTTP/1.1 fallback (`Negotiate`). `keep_alive = false` disables idle connection
retention. Matching timeout/body policies share a persistent origin pool;
changing either policy rebuilds that private driver. The public transport is
synchronous, so one call completes before the next is submitted. The
`tcp_nodelay` compatibility preference currently follows the Nexora backend
and platform TCP defaults.

`TlsSettings` is shared by broker and HTTP trust and identity. The standalone
`tls` feature exposes the broker's verified `build_client_config()` helper.
HTTP loads native roots, custom CA PEM, and mTLS certificate/key PEM directly
into Nexora's in-memory TLS configuration. HTTP TLS uses TLS 1.3 and verified
peer names; unverified TLS is rejected. No credential file is created.

A `server_name` override selects TLS SNI and certificate verification independently
of the request URI. The original URI continues to select the socket destination,
HTTP/1.1 Host, and HTTP/2 `:authority`. A custom Host must agree with the original
URI authority for HTTP/2. Different original authorities have isolated pools;
changes to pinned destination addresses also rebuild the private driver.
Network DNS lookups, including override destinations, use c-ares on the
calling thread. Localhost names map directly to loopback addresses. Lookups
and transfers share one overall request deadline.
Override lookups use a temporary proactor before binding the destination to the
persistent client. Uploads use a separate budget from the configured response
limit, which remains enforced while receiving the response.

Endpoints are parsed with `nexora::Uri`. Credentials and fragments in endpoint
URLs are rejected. OAuth and Schema Registry encode and decode JSON using
`sonic-rs`; errors do not include response bodies or authentication values.

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

## Examples

Runnable examples in `crates/rdkafka/examples/` default to the in-process native
mock broker (`test.mock.num.brokers`) and switch to a live Kafka/Redpanda
cluster when `RDKAFKA_BROKERS` is set:

| Example | Command | Coverage |
| --- | --- | --- |
| `01_basic_produce_consume` | `cargo run -p rdkafka --example 01_basic_produce_consume` | `Conf`, caller-thread `delivery_report_cb`, `Headers`, `Producer::produce`, `Consumer::consume_native` |
| `02_derive_record_soa_batch` | `cargo run -p rdkafka --example 02_derive_record_soa_batch` | `#[derive(Record)]`, SoA `<Name>Batch`, `zerocopy` projection, `BufferPolicy`, `produce_batch`, `consume_batch`, `ReceivedBatch::visit_from` |
| `03_serde_bridge_scratch` | `cargo run -p rdkafka --example 03_serde_bridge_scratch` | `SerdeBridge`, borrowed zero-copy decode, `DecodeLimits`, `produce_batch_using_with_policy`, `consume_serde_with` |
| `04_admin_rebalance_transactions` | `cargo run -p rdkafka --example 04_admin_rebalance_transactions` | Transactional producer commit/abort/purge, `incremental_assign`/`incremental_unassign`, `AdminClient` |
| `05_multithread_workers` | `cargo run -p rdkafka --example 05_multithread_workers` | `ThreadLocalProducer`, `WorkerProducer`, `WorkerBalance::KeyHash`, `WorkerSlotLease` across OS threads |
| `06_tls_http_schema_oauth` | `cargo run -p rdkafka --features http --example 06_tls_http_schema_oauth` | `TlsSettings`, `HttpTransport`, Confluent wire framing, `SchemaRegistryClient`, `OAuthBearerHttpClient` |

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo test --workspace --all-features --locked
cargo test -p rdkafka --features http --doc --locked
```

The source contract test scans owned Rust and native files for removed HTTP and
JSON backends. Workspace tests cover mock brokers, typed records, retention, and
lifecycle rules. The `http` feature adds real loopback HTTP/2 TLS/OIDC and a
Docker Schema Registry round trip. These checks do not replace live Kafka broker
and security tests, which are ignored until their services are explicitly provisioned.

To run live cases against an existing plaintext broker:

```sh
RDKAFKA_TEST_BROKERS=127.0.0.1:9092 cargo test -p rdkafka _live_ --locked -- --ignored --test-threads 1
```

Cargo.lock retains Cargo's native generated format and is exempt from the
800-line source limit. Review the complete resolved graph when updating dependencies.

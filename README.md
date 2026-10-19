# librdkafka-rs

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

`ThreadLocalFactory` and `WorkerFactory` share factory configuration between OS
threads while producing `!Send + !Sync` local managers. Repeated `local()` calls
and cloned factories share one lazy pool on each OS thread. `WorkerBalance`
supports round robin, least-in-flight, and FNV-1a key routing. Fallible leases and
RAII restore local ownership after callback failure or panic. `reset()` releases
the current pool; call it explicitly if clients retain their manager and create
an ownership cycle. No client state crosses a thread boundary.

HTTP tests include real HTTP/2/rustls, mTLS, OIDC, and an isolated Docker Redpanda
Schema Registry. The live test starts and removes its own container unless
`RDKAFKA_SCHEMA_REGISTRY_URL` selects an existing test registry. Docker is a CI
prerequisite; a missing service is a failure, not a passing skipped test.

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
borrowed structure until explicitly materialized. `codec = "zerocopy"` requires all five
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

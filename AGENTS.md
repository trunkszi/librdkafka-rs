# librdkafka-rs Maintenance Standard

## Workspace ownership

- `crates/rdkafka`: public synchronous Kafka SDK, record codecs, broker TLS,
  HTTP services, and per-thread client managers.
- `crates/rdkafka-macros`: `#[derive(Record)]` and generated record projections
  and Structure-of-Arrays batches.
- `crates/rdkafka-sys`: static CMake `librdkafka` build, `cxx` C++/C shim,
  and `ring`-backed SASL crypto.

Keep native ownership and callback plumbing in `rdkafka-sys`; keep public Rust
interfaces free of `Cpp*` and `cpp_*` identifiers. Inspect the target diff before
editing and preserve unrelated work.

## Execution and ownership

Public client and data handles are `!Send + !Sync`: `Producer`, `Consumer`,
`SimpleConsumer`, `AdminClient`, `Topic`, `Queue`, `ReceivedMessage`,
`ReceivedBatch`, `Batch`, `WireBatch`, `HttpTransport`, `OAuthBearerHttpClient`,
`SchemaRegistryClient`, and `WorkerSlotLease`. Polling, record processing, and
local callback dispatch run synchronously on the calling thread without client
mutexes. Async runtime dependencies (`tokio`, `futures`, `futures-core`,
`futures-util`, `async-std`, `smol`) are forbidden.

`ThreadLocalClient` (`ThreadLocalProducer`, `ThreadLocalConsumer`) and
`WorkerClient` (`WorkerProducer`, `WorkerConsumer`) are `Send + Sync` managers
when their factories are `Send + Sync`. They lazily provision per-thread clients
or local lock-free slots `0..N`; clients and leases stay on their creating thread.

Local callbacks may capture `Rc<RefCell<_>>` and run during `poll`, `consume`,
or `flush`. Partitioner closures run on the caller before enqueueing. Direct
native socket/open hooks and background partitioner fallbacks require
`Send + Sync`; socket/open observers and log events are queued for caller-thread
delivery. Preserve callback slots and pending errors across unwinds, and retain
fetched records when reporting a background failure.

Broker TLS uses `connect_cb`/`closesocket_cb`, a local Unix socketpair, and a
bounded per-connection `rustls` pump thread. Native callback/session registries
and TLS diagnostic queues use mutexes at this boundary. Close must stop and join
the pump under backpressure; caller polling drains TLS diagnostics. Keep user
callbacks on the caller thread.

## Features and native builds

The default feature set is empty. `tls` enables `rustls`, native roots, and PEM
parsing. `http` implies `tls` and enables `curl`, `url`, `serde_json`, and Linux
`memfd` backing for HTTP client identities. `curl` comes from
`https://github.com/trunkszi/curl-rust.git`, branch `feat/event-loop-ws`, with
`http2`; the transport requires its rustls backend. `tracing` adds diagnostics.
Treat `crates/rdkafka/Cargo.toml` as the authoritative feature map.

Build static `librdkafka` with `RDKAFKA_BUILD_STATIC=ON`, `WITH_SSL=OFF`, and
`WITH_CURL=OFF`, bundled lz4/snappy/crc32c, and static zlib/zstd. OpenSSL and
`openssl`/`openssl-sys` dependencies are forbidden. Source resolution is
`LIBRDKAFKA_DIR`, then `../../vendor/librdkafka` relative to `rdkafka-sys`, then
`target/vendor-cache/librdkafka` relative to the workspace (or configured target
directory). Source overrides must remain portable; patch only private build output.
Every dependency in every `Cargo.toml` uses inline-table syntax.

## Records, batches, and buffers

`#[derive(Record)]` generates `<Name>RecordRef<'a>`, `<Name>Batch`, and a `Record`
implementation. `codec = "zerocopy"` requires the zerocopy layout traits and
exact-size payload validation. Preserve null/empty distinctions and duplicate
headers; borrowed views retain the lifetime of their backing storage.

`Batch` and `WireBatch` reuse arena storage under `BufferPolicy`: `clear()`
applies the default bound, `clear_with_policy` applies an explicit bound,
`clear_unbounded()` retains allocations, and `shrink_to_fit()` releases unused
storage. `SerdeBridge` batch helpers reuse caller wire and byte scratch buffers;
their policy variants bound retention on error and unwind paths too.

`produce_wire_batch` crosses FFI once per batch; native code calls
`rd_kafka_produceva` per record and preserves independent enqueue results.
`consume_batch` fetches native batches. Use `ReceivedBatch::visit_from` to retry
processing without losing an unvisited tail. Test partial failures explicitly.

## Source standards and verification

Every `.rs`, `.cpp`, `.h`, and `.hpp` file stays at or below 800 lines. Use
English for comments, rustdoc, errors, logs, and commits. Every unsafe block has
`// SAFETY:`; public fallible functions document `# Errors`, panicking functions
`# Panics`, and unsafe functions `# Safety`.

Run the relevant gates before requesting review:

```sh
bash .github/scripts/check-source-contracts.sh
python3 .github/scripts/test-ci-contracts.py
bash .github/scripts/ci-incremental.sh origin/main
```

Native broker changes also require `bash .github/scripts/test-redpanda.sh`.
Broker TLS or SASL changes require `bash .github/scripts/test-broker-security.sh`.
Both harnesses need Docker unless their documented existing-broker option applies.
Report executed coverage and prerequisites accurately. Submit changes through a
PR and wait for `Unified Incremental CI`; main requires linear history.

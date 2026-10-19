# librdkafka-rs

A general-purpose, synchronous, thread-affine Apache Kafka client SDK for Rust.
Native broker transport uses `librdkafka` (`rdkafkacpp.h` + `rdkafka.h`) statically
compiled with CMake and bridged via `cxx`; TLS uses `rustls` (never OpenSSL);
HTTP/2 integrations use `curl-rust` (`feat/event-loop-ws`) with `rustls`; typed
records and SoA batches use `#[derive(Record)]` and `zerocopy`.

## Synchronous & Thread-Affine Execution

- **Synchronous**: public operations and local callbacks execute synchronously
  on the calling thread. `tokio`, `futures`, `async-std`, and `smol` stay out of
  the workspace. Native broker I/O and broker TLS pumps run independently.
- **Thread-affine core**: `Producer`, `Consumer`, `HttpTransport`, and
  `WorkerSlotLease` are `!Send + !Sync`. Caller-thread record processing and
  local callback dispatch use thread-affine state without client mutexes.
- **Broker TLS boundary**: [ADR-0001](docs/adr/0001-rustls-broker-transport.md)
  describes the `connect_cb` / `closesocket_cb` socketpair bridge. The native
  socket callback registry and Rust session registry use mutexes; each connection
  owns a TLS pump thread. A mutex-protected failure queue is drained during
  caller-thread polling, so TLS diagnostics are an explicit locking exception.
- **`thread_local!` scaling**: `ThreadLocalProducer` and `ThreadLocalConsumer`
  keep one client per calling OS thread. `WorkerProducer` and `WorkerConsumer`
  lease lock-free slots `0..N` on the calling thread.
- **Local observers**: poll-delivered callbacks run on the calling thread during
  `poll` / `consume` / `flush`; same-thread state may use `Rc<RefCell<_>>`.
  Partitioner closures run before enqueueing on the calling thread. Native
  background partitioner fallbacks and direct socket/open hooks must be
  `Send + Sync`; socket/open observer events are deferred to caller-thread polling.
  Native log delivery is queued so local event observers are never called on
  internal broker threads.

## Cargo Features & Dependencies

- **Static `librdkafka`**: `crates/rdkafka-sys` builds `librdkafka` statically
  with CMake (`RDKAFKA_BUILD_STATIC=ON`, `WITH_SSL=OFF`, `WITH_CURL=OFF`); `cxx`
  bridges the C++ API.
- **TLS with `rustls` only**: OpenSSL is strictly forbidden (`WITH_SSL=OFF`,
  zero `openssl` crates). TLS uses `rustls`.
- **HTTP/2 via `curl-rust`**: optional HTTP features use `curl` from
  `https://github.com/trunkszi/curl-rust.git` branch `feat/event-loop-ws` with
  `http2` + `rustls`.
- **Inline manifests**: every dependency in every `Cargo.toml` uses
  inline-table syntax, for example `rdkafka-sys = { workspace = true }`.
- **Portable source resolution**: `crates/rdkafka-sys/build.rs` resolves
  `librdkafka` through `LIBRDKAFKA_DIR`, then `../../vendor/librdkafka`, then
  `target/vendor-cache/librdkafka`, with no hardcoded absolute paths.

## Zero-Copy, SoA Batching & Buffers

- **Borrowed inputs & views**: APIs accept `&str`, `&[u8]`, `Cow<'a, str>`, or
  `Cow<'a, [u8]>`, and expose zero-copy `zerocopy` and borrowed views over
  broker buffers.
- **Reusable SoA batches**: `Batch` and arena buffers expose `clear()` and
  `clear_with_policy(BufferPolicy)` to bound retained capacity across cycles.
  Batch production reuses caller-owned SoA/wire buffers and crosses the Rust/C++
  boundary once through `produce_records`; C++ calls `rd_kafka_produceva` for
  each record, retaining independent enqueue results.
- **Serde support**: `SerdeBridge` supports caller-buffer serialization and
  zero-copy borrowed deserialization (`Deserialize<'de>`).

## Code & Repository Hygiene

- **File size**: every `.rs`, `.cpp`, and `.h` file stays at or below 800 lines.
- **Safety and docs**: every `unsafe` block carries `// SAFETY:`; every fallible
  or panicking public function documents `# Errors` or `# Panics`; every
  `unsafe fn` documents `# Safety`. Source comments, rustdoc, errors, logs, and
  commits are English.

## Agent skills

### Issue tracker

Issues and specs live as local markdown files under `.scratch/<feature-slug>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-role vocabulary (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout (`GLOSSARY.md` and `docs/adr/` at the repo root). See `docs/agents/domain.md`.

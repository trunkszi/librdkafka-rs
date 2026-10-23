This file is the authoritative operational contract for coding agents (OpenAI Codex, Claude Code, Gemini CLI) working in librdkafka-rs.

This document is the authoritative engineering and maintenance guide for AI coding agents (OpenAI Codex, Claude Code, Gemini CLI) working in `librdkafka-rs`.

## Project Overview

`librdkafka-rs` is a synchronous Kafka client SDK with thread-affine Rust handles, Structure-of-Arrays (SoA) batch codecs, verified `rustls` broker TLS, optional calling-thread HTTP/2 services (OAuthBearer and Confluent Schema Registry), and a statically linked `librdkafka` C++/C core.

## Workspace Layout

```text
librdkafka-rs/
  Cargo.toml               # Workspace manifest, dependency pins, and strict lints
  LICENSE                  # BSD-2-Clause license (matching upstream librdkafka)
  README.md                # Architecture, usage examples, and verification guide
  crates/
    rdkafka/               # Public synchronous Kafka SDK, record codecs, TLS, HTTP, and thread managers
    rdkafka-macros/        # #[derive(Record)] proc-macro for borrowed projections and SoA batches
    rdkafka-sys/           # Static CMake librdkafka build, cxx C++/C shim, and ring SASL crypto
  .github/
    workflows/             # GitHub Actions formatting, Clippy, tests, and documentation
```

## Crate Layering & Native Builds

- **`crates/rdkafka-sys`**: Compiles static `librdkafka` via CMake with `RDKAFKA_BUILD_STATIC=ON`, `WITH_SSL=OFF`, and `WITH_CURL=OFF`, bundled `lz4`/`snappy`/`crc32c`, and static `zlib`/`zstd`. Provides `cxx` bridges and `ring`-backed SASL crypto (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, OAuthBearer).
  - Source resolution order: `LIBRDKAFKA_DIR`, then `../../vendor/librdkafka` relative to `crates/rdkafka-sys`, then `target/vendor-cache/librdkafka` relative to the workspace or target directory. Source overrides must remain portable; patch only private build output.
- **`crates/rdkafka-macros`**: Implements `#[derive(Record)]`, generating `<Name>RecordRef<'a>`, `<Name>Batch`, and `Record` trait implementations.
- **`crates/rdkafka`**: Public safe Rust SDK. Keep native ownership and callback plumbing inside `rdkafka-sys`; public Rust modules (`crates/rdkafka/` and `crates/rdkafka-macros/`) must never expose or use `Cpp*` or `cpp_*` identifiers.

## Features

`crates/rdkafka/Cargo.toml` is the authoritative feature map:

| Feature | Behavior & Dependencies |
| --- | --- |
| `default` (`[]`) | Core synchronous Kafka clients, typed records, SoA batches, and OpenSSL-free native SASL |
| `tls` | Verified broker `SSL`/`SASL_SSL` via `rustls`, `rustls-native-certs`, and `rustls-pemfile` |
| `http` | Implies `tls`; enables `nexora` HTTP and `sonic-rs` JSON |
| `tracing` | Emits payload-size and operation diagnostics without recording payload contents |

`nexora` follows `https://github.com/trunkszi/nexora-rs.git` on `main`; the
lockfile pins its revision. Keep the inactive WebAssembly `js-sys` resolution at
0.3.82: newer releases introduce forbidden futures packages through optional
`faststr` serialization dependencies. Inspect the complete lockfile when updating dependencies.
JSON uses `sonic-rs` 0.5.10. Keep dependency declarations
as single-line inline tables inherited from the workspace.

HTTP is synchronous at its public boundary and completion-driven internally.
Drive Nexora's client, c-ares DNS, and io_uring on the creating thread. Keep
callbacks and handles thread-affine, and shut the client down before retiring
the proactor and releasing any pinned operation storage. If retirement fails,
retain resolver and client storage while the kernel may still reference it. Broker networking and
its bounded TLS pump threads remain separate from this HTTP driver.

Load HTTP CA and mTLS identity PEM directly into memory. Preserve mandatory
verification, ALPN policy, per-request body limits, and the 65,536-byte header
budget. Keep the upload allowance independent of the response receive limit.
An HTTPS server-name override changes only SNI and certificate verification;
retain the original URI, socket destination, HTTP Host, and HTTP/2 authority.
Isolate pools by original authority and pinned destination addresses.
Resolve override hostnames with deadline-bounded c-ares, including DNS time in
the overall request deadline. Both required HTTP/2 policies must reject HTTP/1 fallback over TLS.
Use `nexora::Uri` for endpoint validation and `sonic-rs` for JSON processing.
Keep endpoint credentials, fragments, response bodies, and secrets out of errors.

## Build, Lint, and Test Commands

| Task | Command |
| --- | --- |
| Format check | `cargo fmt --all -- --check` |
| Clippy (default features) | `cargo clippy --workspace --all-targets --locked -- -D warnings` |
| Unit, integration, and documentation tests | `cargo test --workspace --locked` |
| Build documentation | `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` |
| Rust and native source policy | `cargo test -p rdkafka --test source_contract --locked` |
| Clippy (HTTP and tracing) | `cargo clippy -p rdkafka --all-targets --features http,tracing --locked -- -D warnings` |
| Full feature acceptance | `cargo test --workspace --all-features --locked` |
| HTTP documentation tests | `cargo test -p rdkafka --features http --doc --locked` |
| Live broker tests (provision a broker first) | `RDKAFKA_TEST_BROKERS=127.0.0.1:9092 cargo test -p rdkafka _live_ --locked -- --ignored --test-threads 1` |
| Live TLS/SASL test (provision listeners and certificates first) | `cargo test -p rdkafka --features tls --test sasl_live --locked -- --ignored --test-threads 1` |

## Hard Invariants & Coding Standards

- **No Async Runtimes or OpenSSL**: `tokio`, `futures`, `futures-core`, `futures-util`, `async-std`, `smol`, `openssl`, `openssl-sys`, `curl`, `curl-sys`, `memfd`, `url`, and `serde_json` are strictly forbidden across all manifests and `Cargo.lock`.
- **Owned Source Boundaries**: Scan `.rs`, `.c`, `.cpp`, `.cc`, `.h`, and `.hpp` in source directories and native shims for removed backend includes, types, and API identifiers. Preserve the native build setting `WITH_CURL=OFF`.
- **File Size Limit**: Repository text files must stay at or below **800 lines** and use ASCII English. Automatically generated `Cargo.lock` is exempt from the line limit; preserve Cargo native formatting and never compact it.
- **Manifest Syntax**: Every dependency declaration in every `Cargo.toml` must use direct single-line inline-table syntax (`pkg = { ... }`). Dependency subtables and dotted keys are forbidden.
- **Language & Documentation**: Use English exclusively for all source comments, rustdoc, error messages, logs, documentation, and commit messages.
- **Safety & Lint Discipline**:
  - `unsafe_op_in_unsafe_fn = "deny"` and `undocumented_unsafe_blocks = "deny"` are enforced workspace-wide.
  - Every `unsafe` block must carry a `// SAFETY:` comment explaining why the invariants hold.
  - Every `unsafe fn` must document `# Safety`, every public fallible function must document `# Errors`, and every panicking function must document `# Panics`.
  - `todo!()` and `unimplemented!()` are denied by Clippy.

## Records, Batches, and Ownership Invariants

- **Single-Thread Client Affinity (`!Send + !Sync`)**: Public client and data handles (`Producer`, `Consumer`, `SimpleConsumer`, `AdminClient`, `Topic`, `Queue`, `ReceivedMessage`, `ReceivedBatch`, `Batch`, `WireBatch`, `HttpTransport`, `OAuthBearerHttpClient`, `SchemaRegistryClient`, `WorkerSlotLease`) are `!Send + !Sync`. Polling, record encoding/decoding, and local callback dispatch run synchronously on the calling thread without client mutexes.
- **Cross-Thread Scaling**: `ThreadLocalClient` (`ThreadLocalProducer`, `ThreadLocalConsumer`) and `WorkerClient` (`WorkerProducer`, `WorkerConsumer`) are `Send + Sync` when their factories are `Send + Sync`. They lazily provision thread-local clients or lock-free slots `0..N`; leases and clients never cross threads.
- **Callback Execution**: Local callbacks may capture `Rc<RefCell<_>>` and execute during `poll`, `consume`, or `flush`. Partitioner closures run on the calling thread before enqueueing. Direct native socket/open hooks and background partitioner fallbacks require `Send + Sync`; socket/open observers and log events are queued for caller-thread delivery. Preserve callback slots and pending errors across unwinds, and retain fetched records when reporting background failures.
- **Broker TLS Pump**: Broker TLS bridges `connect_cb`/`closesocket_cb` over a local Unix socketpair to a bounded per-connection `rustls` pump thread. Connection close must stop and join the pump thread even under backpressure, while TLS diagnostics drain on the calling thread during polling.
- **Records, SoA Batches, and Arena Retention**:
  - `#[derive(Record)]` generates borrowed views (`<Name>RecordRef<'a>`) and SoA batches (`<Name>Batch`). `codec = "zerocopy"` requires `FromBytes`, `IntoBytes`, `KnownLayout`, `Immutable`, and `Unaligned` with exact-size payload validation. Preserve null vs. empty distinctions and duplicate headers.
  - `Batch` and `WireBatch` reuse arena storage governed by `BufferPolicy`: `clear()` applies the default bound (65,536 slots / 8 MiB), `clear_with_policy()` applies an explicit bound, `clear_unbounded()` retains warm capacity, and `shrink_to_fit()` releases unused memory. `SerdeBridge` batch helpers bound scratch retention on both ok, error, and unwind paths.
  - `produce_wire_batch` crosses the FFI boundary once per batch and invokes native `rd_kafka_produceva` per record, preserving per-record enqueue status. `consume_batch` fetches native batches; use `ReceivedBatch::visit_from` to resume or retry processing without losing unvisited trailing records.

## Change Hygiene

- Inspect `git status` and `git diff` before editing, and preserve unrelated changes.
- Run the four workspace Cargo gates above after code or manifest changes. Review dependency pins, inline-table syntax, ASCII text, and file lengths.
- Native broker changes require the ignored live broker tests against an explicitly provisioned broker. Broker TLS or SASL changes also require the ignored `sasl_live` test; read its environment variables before provisioning listeners and certificates.

## Test Boundaries and Completion

Classify tests by their exercised boundary: unit rules, integration between
components, or end-to-end business entry points. Add lifecycle and scenario
labels when they describe transitions, cleanup, malformed input, or failures.
The HTTP loopback fixtures exercise real sockets, TLS verification, ALPN, and
connection teardown; they are not native Kafka broker tests. Compile-fail
record fixtures run Cargo against a private temporary consumer and assert the
expected diagnostic and source location without a JSON-dependent test framework.

Use targeted tests while implementing. Run the workspace Cargo gates and full
feature acceptance for releases. Run live broker and security cases against
explicitly provisioned services. Report missing prerequisites as unavailable checks.
Check final diff scope and whitespace; preserve unrelated concurrent work.

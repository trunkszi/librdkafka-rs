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

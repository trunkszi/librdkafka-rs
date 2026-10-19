# SoA `Batch` and `zerocopy` Record Projections

`#[derive(Record)]` generates a borrowed `<T>RecordRef<'a>` projection and a
Structure-of-Arrays `<T>Batch` container. Field metadata maps keys, timestamps,
partitions, and headers; payloads use compact little-endian encoding or direct
`zerocopy` casting with `#[record(codec = "zerocopy")]`.

Callers reuse their SoA columns, `WireBatch` arenas, and serialization scratch
buffers, applying `BufferPolicy` when clearing retained capacity. Producing a
batch crosses the Rust/C++ boundary once through `produce_records`. The C++
bridge enqueues each record separately with `rd_kafka_produceva` and returns
independent enqueue outcomes; this is neither a single native multi-record
produce operation nor an atomic batch commit. Native descriptor construction,
header handling, and `librdkafka` payload copies remain distinct from caller
buffer reuse.

Batch consumption calls `rd_kafka_consume_batch_queue` once at the native
boundary. Borrowed receive projections retain their native buffer owner for
the lifetime of the views.

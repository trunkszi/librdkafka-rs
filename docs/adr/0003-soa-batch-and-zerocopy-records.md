# SoA `Batch` and `zerocopy` Record Projections

`#[derive(Record)]` generates both a borrowed `RecordView<'a>` (`<T>RecordRef<'a>`) and a Structure-of-Arrays `<T>Batch` container alongside `WireBatch` arena buffers, supporting `#[record(codec = "zerocopy")]` for direct `zerocopy::{FromBytes, IntoBytes}` slice casting as well as compact little-endian field encoding with `#[record(key | timestamp | partition | header)]` metadata extraction. This avoids AoS-to-SoA re-layout and per-record heap allocations during high-throughput batch production and consumption.

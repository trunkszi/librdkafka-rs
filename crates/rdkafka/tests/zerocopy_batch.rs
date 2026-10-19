use rdkafka::{
    WireBatch, WireHeader, WireRecord,
    record::{zerocopy_bytes, zerocopy_ref},
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Frame {
    sequence: [u8; 8],
    flags: [u8; 2],
}

#[test]
fn unit_scenario_direct_projection_borrows_exact_layout_and_rejects_truncation() {
    let frame = Frame {
        sequence: 17u64.to_le_bytes(),
        flags: [1, 2],
    };
    let bytes = zerocopy_bytes(&frame);
    let projected = zerocopy_ref::<Frame>(bytes).unwrap();
    assert_eq!(projected.sequence, frame.sequence);
    assert_eq!(projected as *const Frame as *const u8, bytes.as_ptr());
    assert!(zerocopy_ref::<Frame>(&bytes[..9]).is_err());
    assert!(zerocopy_ref::<Frame>(&[0; 11]).is_err());
}

#[test]
fn integration_scenario_arenas_preserve_null_empty_duplicate_headers_and_metadata() {
    let headers = [
        WireHeader {
            name: "trace",
            value: None,
        },
        WireHeader {
            name: "trace",
            value: Some(b""),
        },
        WireHeader {
            name: "kind",
            value: Some(b"fill"),
        },
    ];
    let mut batch = WireBatch::new();
    batch
        .push(
            WireRecord::new("fills", None)
                .with_key(Some(b""))
                .with_headers(&headers)
                .with_partition(3)
                .with_offset(42)
                .with_timestamp(Some(1000)),
        )
        .unwrap();
    batch.push(WireRecord::new("fills", Some(b""))).unwrap();
    let first = batch.get(0).unwrap();
    assert_eq!(first.topic, "fills");
    assert_eq!(first.key, Some(&b""[..]));
    assert_eq!(first.payload, None);
    assert_eq!(first.partition, 3);
    assert_eq!(first.offset, 42);
    assert_eq!(first.timestamp, Some(1000));
    assert_eq!(first.headers.iter().collect::<Vec<_>>(), headers);
    let second = batch.get(1).unwrap();
    assert_eq!(second.key, None);
    assert_eq!(second.payload, Some(&b""[..]));
    assert!(batch.get(2).is_none());
    assert_eq!(batch.iter().len(), 2);
}

#[test]
fn integration_scenario_warmed_wire_push_performs_no_allocation() {
    let headers = [WireHeader {
        name: "trace",
        value: Some(&b"fixed"[..]),
    }];
    let record = WireRecord::new("fills", Some(b"payload"))
        .with_key(Some(b"key"))
        .with_headers(&headers);
    let mut batch = WireBatch::with_capacity(64);
    for _ in 0..64 {
        batch.push(record).unwrap();
    }
    batch.clear_unbounded();
    allocation_count::start();
    for _ in 0..64 {
        batch.push(record).unwrap();
    }
    let allocations = allocation_count::stop();
    assert_eq!(allocations, 0);
}

mod allocation_count {
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        cell::Cell,
    };
    thread_local! {
        static ENABLED: Cell<bool> = const { Cell::new(false) };
        static COUNT: Cell<usize> = const { Cell::new(0) };
    }
    pub struct Counting;
    // SAFETY: Every allocation and deallocation delegates to System unchanged.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ENABLED.with(|enabled| {
                if enabled.get() {
                    COUNT.with(|count| count.set(count.get() + 1));
                }
            });
            // SAFETY: The caller supplies a valid allocation layout.
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            // SAFETY: The pointer and layout originate from this allocator.
            unsafe {
                System.dealloc(pointer, layout);
            }
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            ENABLED.with(|enabled| {
                if enabled.get() {
                    COUNT.with(|count| count.set(count.get() + 1));
                }
            });
            // SAFETY: The caller supplies the original pointer and layout.
            unsafe { System.realloc(pointer, layout, size) }
        }
    }
    pub fn start() {
        COUNT.with(|count| count.set(0));
        ENABLED.with(|enabled| enabled.set(true));
    }
    pub fn stop() -> usize {
        ENABLED.with(|enabled| enabled.set(false));
        COUNT.with(Cell::get)
    }
}
#[global_allocator]
static ALLOCATOR: allocation_count::Counting = allocation_count::Counting;

#[test]
fn integration_scenario_direct_arena_writer_rolls_back_all_columns_on_codec_failure() {
    let mut batch = WireBatch::new();
    batch
        .push(WireRecord::new("existing", Some(b"stable")))
        .unwrap();
    let result = batch.push_with("failed", 2, 10, Some(100), |writer| {
        writer.key(Some(b"unused"))?;
        writer.encode_payload(|out| {
            out.extend_from_slice(b"discard");
            Ok(())
        })?;
        writer.header("trace", Some(b"discard"))?;
        Err(rdkafka::Error::Codec("rejected".into()))
    });
    assert!(result.is_err());
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.get(0).unwrap().payload, Some(&b"stable"[..]));
    batch
        .push_with("next", 4, 12, None, |writer| {
            writer.encode_payload(|out| {
                out.extend_from_slice(b"valid");
                Ok(())
            })?;
            writer.header("trace", None)
        })
        .unwrap();
    let next = batch.get(1).unwrap();
    assert_eq!(next.topic, "next");
    assert_eq!(next.partition, 4);
    assert_eq!(next.offset, 12);
    assert_eq!(next.payload, Some(&b"valid"[..]));
    assert_eq!(next.headers.len(), 1);
    assert_eq!(next.headers.get(0).unwrap().value, None);
}

#[test]
fn integration_scenario_direct_arena_encoding_reuses_warm_allocations() {
    fn encode(batch: &mut WireBatch) {
        for _ in 0..64 {
            batch
                .push_with("fills", 0, -1, None, |writer| {
                    writer.encode_key(|out| {
                        out.extend_from_slice(b"key");
                        Ok(())
                    })?;
                    writer.encode_payload(|out| {
                        out.extend_from_slice(b"payload");
                        Ok(())
                    })?;
                    writer.encode_header("trace", |out| {
                        out.extend_from_slice(b"fixed");
                        Ok(())
                    })
                })
                .unwrap();
        }
    }
    let mut batch = WireBatch::with_capacity(64);
    encode(&mut batch);
    batch.clear_unbounded();
    allocation_count::start();
    encode(&mut batch);
    let allocations = allocation_count::stop();
    assert_eq!(allocations, 0);
}

#[test]
fn unit_lifecycle_scenario_string_columns_reuse_slots_and_bound_nested_capacity() {
    use rdkafka::{buffer_policy::MemoryBuffer, column::ReusableColumn};
    let input = "x".repeat(4096);
    let mut column = ReusableColumn::with_capacity(64);
    for _ in 0..64 {
        column.push(&input);
    }
    assert!(column.allocated_bytes() >= 64 * 4096);
    column
        .clear_retained(rdkafka::BufferPolicy::unbounded().retention(64, column.allocated_bytes()));
    assert!(column.as_slice().is_empty());
    allocation_count::start();
    for _ in 0..64 {
        column.push(&input);
    }
    let allocations = allocation_count::stop();
    assert_eq!(allocations, 0);
    let retention = rdkafka::BufferPolicy {
        max_records: 16,
        max_bytes: 8192,
    }
    .retention(column.record_capacity(), column.allocated_bytes());
    column.clear_retained(retention);
    assert!(column.record_capacity() <= 16);
    assert!(column.allocated_bytes() <= 8192);
}

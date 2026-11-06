//! Caller-thread Rust hot paths; native polling is measured separately from pure codecs.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::{Cell, RefCell},
    error::Error,
    hint::black_box,
    rc::Rc,
};

use criterion::Criterion;
use rdkafka::{
    BufferPolicy, Conf, Error as KafkaError, ProduceRecord, Producer, Record, WireBatch,
    WireHeader, WireRecord, column::ReusableColumn,
};

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
struct CountingAllocator;
fn allocation() {
    if TRACK.try_with(Cell::get) == Ok(true) {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
    }
}
// SAFETY: all pointers and layouts are passed unchanged to the System allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocation();
        // SAFETY: the caller supplies a valid allocation layout.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocation();
        // SAFETY: the caller supplies a valid allocation layout.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the allocation originated from this System wrapper.
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocation();
        // SAFETY: the caller supplies the live allocation and valid new size.
        unsafe { System.realloc(pointer, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
struct AllocationScope;
impl Drop for AllocationScope {
    fn drop(&mut self) {
        TRACK.set(false);
    }
}
fn without_allocations<T>(operation: impl FnOnce() -> T) -> T {
    ALLOCATIONS.set(0);
    TRACK.set(true);
    let guard = AllocationScope;
    let result = operation();
    drop(guard);
    assert_eq!(ALLOCATIONS.get(), 0, "warmed Rust hot path allocated");
    result
}
fn measure<T>(c: &mut Criterion, name: &str, mut operation: impl FnMut() -> T) {
    for _ in 0..1024 {
        black_box(operation());
    }
    let _preflight = without_allocations(&mut operation);
    c.bench_function(name, |b| b.iter(&mut operation));
}

#[derive(Debug, Record)]
struct Sample {
    sequence: u64,
    ticks: i64,
    symbol: String,
    payload: Vec<u8>,
}
fn sample() -> Sample {
    Sample {
        sequence: 42,
        ticks: 12345,
        symbol: "BTCUSDT".into(),
        payload: vec![7; 48],
    }
}
fn codecs(c: &mut Criterion) -> Result<(), Box<dyn Error>> {
    let record = sample();
    for (name, policy) in [
        ("bounded", BufferPolicy::default()),
        ("unbounded", BufferPolicy::unbounded()),
    ] {
        let mut batch = SampleBatch::with_capacity(64);
        for _ in 0..64 {
            batch.push(&record);
        }
        batch.clear_with_policy(policy);
        measure(c, &format!("batch/push_clear_64/{name}"), || {
            for _ in 0..64 {
                batch.push(black_box(&record));
            }
            black_box(batch.column_sequence());
            batch.clear_with_policy(policy);
        });
    }
    let headers = [
        WireHeader {
            name: "trace",
            value: Some(&b"trace-token"[..]),
        },
        WireHeader {
            name: "trace",
            value: None,
        },
        WireHeader {
            name: "kind",
            value: Some(&b""[..]),
        },
    ];
    let frame = WireRecord::new("trades", Some(b"payload-data"))
        .with_key(Some(b"key"))
        .with_headers(&headers)
        .with_timestamp(Some(1000));
    let mut wire = WireBatch::with_capacity(16);
    for _ in 0..16 {
        wire.push(frame)?;
    }
    measure(c, "wire/pack_headers_16", || {
        wire.clear_unbounded();
        for _ in 0..16 {
            assert!(wire.push(black_box(frame)).is_ok());
        }
        black_box(wire.len())
    });
    measure(c, "wire/project_headers", || {
        let frame = black_box(&wire).get(black_box(8)).expect("warmed frame");
        black_box((
            frame.topic,
            frame.payload,
            frame
                .headers
                .iter()
                .fold(0, |n, h| n + h.name.len() + h.value.map_or(0, <[u8]>::len)),
        ))
    });
    let mut encoded = WireBatch::with_capacity(1);
    record.encode_into(&mut encoded)?;
    measure(c, "wire/encode_record", || {
        encoded.clear_unbounded();
        assert!(black_box(&record).encode_into(&mut encoded).is_ok());
    });
    let frame = encoded.get(0).ok_or("encoded frame missing")?;
    measure(c, "wire/borrow_record", || Sample::view(black_box(frame)));
    let mut primitive = ReusableColumn::with_capacity(64);
    let mut variable = ReusableColumn::with_capacity(64);
    for index in 0..64 {
        primitive.push(&(index as u64));
        variable.push(&record.symbol);
    }
    measure(c, "column/primitive_slice", || {
        black_box(&primitive).as_slice()[black_box(31)]
    });
    measure(c, "column/variable_slice", || {
        black_box(&variable).as_slice()[black_box(31)].as_bytes()
    });
    Ok(())
}
fn main() -> Result<(), Box<dyn Error>> {
    let criterion = Rc::new(RefCell::new(Criterion::default().configure_from_args()));
    codecs(&mut criterion.borrow_mut())?;
    let owner = Rc::new(RefCell::new(std::rc::Weak::<Producer>::new()));
    let callback_owner = Rc::clone(&owner);
    let callback_criterion = Rc::clone(&criterion);
    let invoked = Rc::new(Cell::new(false));
    let callback_invoked = Rc::clone(&invoked);
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?
        .set("log_level", "0")?;
    conf.delivery_report_cb(move |_| {
        let producer = callback_owner
            .borrow()
            .upgrade()
            .expect("callback owner exists");
        assert!(matches!(producer.poll(0), Err(KafkaError::ReentrantDrive)));
        // The callback owns the real drive scope; each timed call rejects before native polling.
        measure(
            &mut callback_criterion.borrow_mut(),
            "core/reentrant_poll_guard",
            || producer.poll(black_box(0)),
        );
        callback_invoked.set(true);
    });
    let producer = Rc::new(Producer::new(&conf)?);
    *owner.borrow_mut() = Rc::downgrade(&producer);
    measure(&mut criterion.borrow_mut(), "core/empty_poll_cycle", || {
        producer.poll(black_box(0))
    });
    producer.produce(ProduceRecord::new("benchmark-guard", Some(b"one")))?;
    producer.flush(10000)?;
    assert!(
        invoked.get(),
        "delivery callback did not run guard benchmark"
    );
    criterion.borrow_mut().final_summary();
    Ok(())
}

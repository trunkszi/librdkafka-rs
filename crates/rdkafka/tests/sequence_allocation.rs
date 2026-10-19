//! Borrowed collection projections scan retained wire bytes without allocation.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

use rdkafka::{Record, WireBatch, serde_bridge::EncodedSequence};
use static_assertions::assert_not_impl_any;

#[derive(Clone, Record)]
struct Collections {
    words: Vec<String>,
    nested: Vec<Vec<Option<String>>>,
    numbers: Vec<u64>,
    optional: Option<Vec<String>>,
    mixed: Vec<Option<Vec<String>>>,
}
assert_not_impl_any!(CollectionsRecordRef<'static>: Send, Sync);
assert_not_impl_any!(EncodedSequence<'static, &'static str>: Send, Sync);

#[test]
fn integration_scenario_nested_collection_views_and_iteration_allocate_nothing() {
    let record = Collections {
        words: vec!["alpha".into(), "beta".into()],
        nested: vec![vec![Some("one".into()), None], vec![Some("two".into())]],
        numbers: vec![1, 2, 3],
        optional: Some(vec!["optional".into()]),
        mixed: vec![None, Some(vec!["mixed".into()])],
    };
    let mut wire = WireBatch::new();
    record.encode_into(&mut wire).unwrap();
    let frame = wire.get(0).unwrap();
    COUNT.with(|count| count.set(0));
    ENABLED.with(|enabled| enabled.set(true));
    let view = Collections::view(frame).unwrap();
    let words = view
        .words
        .iter()
        .map(|word| word.unwrap().len())
        .sum::<usize>();
    let mut present = 0;
    let mut missing = 0;
    for group in view.nested.iter() {
        for word in group.unwrap().iter() {
            match word.unwrap() {
                Some(value) => present += value.len(),
                None => missing += 1,
            }
        }
    }
    let numbers = view.numbers.iter().map(Result::unwrap).sum::<u64>();
    let optional = view
        .optional
        .as_ref()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().len())
        .sum::<usize>();
    let mixed = view
        .mixed
        .iter()
        .filter_map(|value| value.unwrap())
        .flat_map(|group| group.into_iter())
        .map(|value| value.unwrap().len())
        .sum::<usize>();
    ENABLED.with(|enabled| enabled.set(false));
    let allocations = COUNT.with(Cell::get);
    assert_eq!(allocations, 0);
    assert_eq!(words, 9);
    assert_eq!(present, 6);
    assert_eq!(missing, 1);
    assert_eq!(numbers, 6);
    assert_eq!(optional, 8);
    assert_eq!(mixed, 5);
    assert_eq!(view.words.len(), 2);
    assert_eq!(view.nested.len(), 2);
    assert_eq!(view.numbers.len(), 3);
}

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static COUNT: Cell<usize> = const { Cell::new(0) };
}
struct Counting;
// SAFETY: Allocation operations delegate unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: The caller provides a valid allocation layout.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The allocation pointer and layout originate from this allocator.
        unsafe {
            System.dealloc(pointer, layout);
        }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        // SAFETY: The caller provides the original pointer and allocation layout.
        unsafe { System.realloc(pointer, layout, size) }
    }
}
fn count() {
    ENABLED.with(|enabled| {
        if enabled.get() {
            COUNT.with(|count| count.set(count.get() + 1));
        }
    });
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

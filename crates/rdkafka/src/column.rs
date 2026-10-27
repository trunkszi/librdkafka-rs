//! Reusable typed columns retaining nested allocation slots across clears.
use std::{marker::PhantomData, rc::Rc};

use crate::buffer_policy::{MemoryBuffer, Retention};

/// Capacity accounting and trimming for values kept in reusable columns.
///
/// Custom types containing allocations must include those allocations here.
pub trait ColumnValue {
    /// Allocated bytes owned by this value, excluding its inline representation.
    fn extra_bytes(&self) -> usize {
        0
    }
    /// Clear value contents while retaining a proportional allocation budget.
    fn clear_value(&mut self, _retention: Retention) {}
    /// Release unused nested capacity.
    fn shrink_value(&mut self) {}
}
macro_rules! plain {
    ($($ty:ty),* $(,)?) => { $(impl ColumnValue for $ty {})* };
}
plain!(
    bool,
    char,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    f32,
    f64,
    ()
);
impl ColumnValue for String {
    fn extra_bytes(&self) -> usize {
        self.capacity()
    }
    fn clear_value(&mut self, retention: Retention) {
        let target = retention.target(self.capacity());
        self.clear();
        self.shrink_to(target);
    }
    fn shrink_value(&mut self) {
        self.shrink_to_fit();
    }
}
impl<T: ColumnValue> ColumnValue for Vec<T> {
    fn extra_bytes(&self) -> usize {
        self.iter().fold(
            self.capacity().saturating_mul(size_of::<T>()),
            |bytes, v| bytes.saturating_add(v.extra_bytes()),
        )
    }
    fn clear_value(&mut self, retention: Retention) {
        let total_target = retention.target(self.extra_bytes());
        let target = retention.target(if size_of::<T>() == 0 {
            0
        } else {
            self.capacity()
        });
        let nested_bytes = self.iter().fold(0usize, |bytes, value| {
            bytes.saturating_add(value.extra_bytes())
        });
        if nested_bytes == 0 {
            self.clear();
            self.shrink_to(target);
            return;
        }
        self.truncate(target);
        self.shrink_to(target);
        let remaining = total_target.saturating_sub(self.capacity().saturating_mul(size_of::<T>()));
        let retained_nested = self.iter().fold(0usize, |bytes, value| {
            bytes.saturating_add(value.extra_bytes())
        });
        let child = crate::BufferPolicy {
            max_records: usize::MAX,
            max_bytes: remaining,
        }
        .retention(0, retained_nested, 0);
        for value in self.iter_mut() {
            value.clear_value(child);
        }
    }
    fn shrink_value(&mut self) {
        for value in self.iter_mut() {
            value.shrink_value();
        }
        self.shrink_to_fit();
    }
}
impl<T: ColumnValue> ColumnValue for Option<T> {
    fn extra_bytes(&self) -> usize {
        self.as_ref().map_or(0, ColumnValue::extra_bytes)
    }
    fn clear_value(&mut self, retention: Retention) {
        if let Some(value) = self {
            value.clear_value(retention);
        }
    }
    fn shrink_value(&mut self) {
        if let Some(value) = self {
            value.shrink_value();
        }
    }
}
impl<T: ColumnValue, const N: usize> ColumnValue for [T; N] {
    fn extra_bytes(&self) -> usize {
        self.iter()
            .fold(0usize, |bytes, v| bytes.saturating_add(v.extra_bytes()))
    }
    fn clear_value(&mut self, retention: Retention) {
        for value in self {
            value.clear_value(retention);
        }
    }
    fn shrink_value(&mut self) {
        for value in self {
            value.shrink_value();
        }
    }
}
/// A contiguous typed column with reusable initialized slots behind its length.
///
/// Values beyond the logical length are hidden and reused through `clone_from`.
/// Warm allocation reuse depends on the value's `Clone::clone_from` contract;
/// changes from `None` to an allocated `Some` may still allocate.
#[derive(Debug)]
pub struct ReusableColumn<T> {
    values: Vec<T>,
    len: usize,
    affinity: PhantomData<Rc<()>>,
}
impl<T> Default for ReusableColumn<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            len: 0,
            affinity: PhantomData,
        }
    }
}
impl<T: Clone + ColumnValue> ReusableColumn<T> {
    /// Reserve inline slots for a number of records.
    /// # Panics
    /// Panics if the requested inline allocation exceeds the vector size limit.
    pub fn with_capacity(records: usize) -> Self {
        Self {
            values: Vec::with_capacity(records),
            len: 0,
            affinity: PhantomData,
        }
    }
    /// Number of visible values.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
    /// Whether the column has no visible values.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// Borrow only the visible contiguous values.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: only initialized slots become visible. Appends advance len after success;
        // truncate/clear only decrease it, including when a user clone or cleanup panics.
        unsafe { self.values.get_unchecked(..self.len) }
    }
    /// Reduce the visible length, retaining initialized slots and their allocations.
    ///
    /// A length at least as large as the current length has no effect. Hidden
    /// values are reused by subsequent appends; truncation never drops values
    /// or invokes their cloning or cleanup methods.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len);
    }
    /// Update a retained slot or create a value only when no slot exists.
    /// # Panics
    /// Panics if field creation, cloning, or updating panics, or vector growth exceeds its size
    /// limit.
    #[inline]
    pub fn push_with(&mut self, create: impl FnOnce() -> T, update: impl FnOnce(&mut T)) {
        if let Some(slot) = self.values.get_mut(self.len) {
            update(slot);
        } else {
            self.values.push(create());
        }
        self.len += 1;
    }
    /// Release unused slots and nested allocations without discarding values.
    pub fn shrink_to_fit(&mut self) {
        MemoryBuffer::shrink_to_fit(self);
    }
    /// Move a newly decoded value into a cold slot, reusing a warm slot if present.
    /// # Panics
    /// Panics if field creation, cloning, or updating panics, or vector growth exceeds its size
    /// limit.
    #[inline]
    pub fn push_owned(&mut self, value: T) {
        if let Some(slot) = self.values.get_mut(self.len) {
            slot.clone_from(&value);
        } else {
            self.values.push(value);
        }
        self.len += 1;
    }
    /// Copy into an existing initialized slot, or append a new slot.
    /// # Panics
    /// Panics if field creation, cloning, or updating panics, or vector growth exceeds its size
    /// limit.
    #[inline]
    pub fn push(&mut self, value: &T) {
        if let Some(slot) = self.values.get_mut(self.len) {
            slot.clone_from(value);
        } else {
            self.values.push(value.clone());
        }
        self.len += 1;
    }
}
impl<T: Clone + ColumnValue> MemoryBuffer for ReusableColumn<T> {
    fn allocated_bytes(&self) -> usize {
        self.values.iter().fold(
            self.values.capacity().saturating_mul(size_of::<T>()),
            |bytes, value| bytes.saturating_add(value.extra_bytes()),
        )
    }
    fn inline_bytes(&self) -> usize {
        self.values.capacity().saturating_mul(size_of::<T>())
    }
    fn record_capacity(&self) -> usize {
        if size_of::<T>() == 0 {
            0
        } else {
            self.values.capacity()
        }
    }
    fn clear_retained(&mut self, retention: Retention) {
        let target = retention.record_target(self.record_capacity());
        self.len = 0;
        self.values.truncate(target);
        for value in &mut self.values {
            value.clear_value(retention);
        }
        self.values.shrink_to(target);
    }
    fn shrink_to_fit(&mut self) {
        self.values.truncate(self.len);
        for value in &mut self.values {
            value.shrink_value();
        }
        self.values.shrink_to_fit();
    }
}

impl<T: Clone + ColumnValue> std::ops::Index<usize> for ReusableColumn<T> {
    type Output = T;
    fn index(&self, index: usize) -> &T {
        &self.as_slice()[index]
    }
}

impl<B> ColumnValue for std::borrow::Cow<'_, B>
where
    B: ToOwned + ?Sized,
    B::Owned: ColumnValue,
{
    fn extra_bytes(&self) -> usize {
        match self {
            std::borrow::Cow::Borrowed(_) => 0,
            std::borrow::Cow::Owned(value) => value.extra_bytes(),
        }
    }
    fn clear_value(&mut self, retention: Retention) {
        if let std::borrow::Cow::Owned(value) = self {
            value.clear_value(retention);
        }
    }
    fn shrink_value(&mut self) {
        if let std::borrow::Cow::Owned(value) = self {
            value.shrink_value();
        }
    }
}

macro_rules! tuple_values {
    ($($ty:ident:$index:tt),+) => {
        impl<$($ty: ColumnValue),+> ColumnValue for ($($ty,)+) {
            fn extra_bytes(&self) -> usize { 0usize$(.saturating_add(self.$index.extra_bytes()))+ }
            fn clear_value(&mut self, retention: Retention) { $(self.$index.clear_value(retention);)+ }
            fn shrink_value(&mut self) { $(self.$index.shrink_value();)+ }
        }
    };
}
tuple_values!(A:0);
tuple_values!(A:0,B:1);
tuple_values!(A:0,B:1,C:2);
tuple_values!(A:0,B:1,C:2,D:3);
tuple_values!(A:0,B:1,C:2,D:3,E:4);
tuple_values!(A:0,B:1,C:2,D:3,E:4,F:5);
tuple_values!(A:0,B:1,C:2,D:3,E:4,F:5,G:6);
tuple_values!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7);

impl<K: ColumnValue + Ord, V: ColumnValue> ColumnValue for std::collections::BTreeMap<K, V> {
    fn extra_bytes(&self) -> usize {
        self.iter().fold(
            self.len().saturating_mul(size_of::<(K, V)>()),
            |bytes, (key, value)| {
                bytes
                    .saturating_add(key.extra_bytes())
                    .saturating_add(value.extra_bytes())
            },
        )
    }
    fn clear_value(&mut self, _retention: Retention) {
        // Tree nodes do not expose reusable capacity; releasing entries drops all nodes.
        self.clear();
    }
    fn shrink_value(&mut self) {
        for value in self.values_mut() {
            value.shrink_value();
        }
    }
}
impl<K, V, S> ColumnValue for std::collections::HashMap<K, V, S>
where
    K: ColumnValue + Eq + std::hash::Hash,
    V: ColumnValue,
    S: std::hash::BuildHasher,
{
    fn extra_bytes(&self) -> usize {
        self.iter().fold(
            self.capacity().saturating_mul(size_of::<(K, V)>()),
            |bytes, (key, value)| {
                bytes
                    .saturating_add(key.extra_bytes())
                    .saturating_add(value.extra_bytes())
            },
        )
    }
    fn clear_value(&mut self, retention: Retention) {
        // Map entries cannot retain cleared keys without changing lookup identity.
        let target = retention.target(self.capacity());
        self.clear();
        self.shrink_to(target);
    }
    fn shrink_value(&mut self) {
        for value in self.values_mut() {
            value.shrink_value();
        }
        self.shrink_to_fit();
    }
}

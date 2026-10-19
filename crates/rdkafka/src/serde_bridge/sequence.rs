//! Validated, allocation-free projections of positional sequence elements.
use std::{borrow::Cow, marker::PhantomData, rc::Rc};

use serde::{Deserialize, Serialize, ser::SerializeSeq};

use super::{DecodeLimits, Deserializer};
use crate::{Error, Result};

type SequenceMarker<T> = PhantomData<(fn() -> T, Rc<()>)>;

/// Decode a value whose variable-sized contents borrow from the input.
///
/// Implementations must consume exactly one positional value. Implementing this
/// trait for an owned allocating type forfeits allocation-free sequence walks.
/// Implementations must deterministically decode the same value shape from the
/// same bytes and limits; they must not reset or replace the supplied cursor.
pub trait BorrowDecode<'de>: Sized {
    /// Read one value and advance the supplied positional cursor.
    /// # Errors
    /// Returns an error for malformed input or an exceeded decoding limit.
    fn borrow_decode(decoder: &mut Deserializer<'de>) -> Result<Self>;
}

/// A validated sequence stored directly in its original positional wire bytes.
///
/// Construction walks the elements once to validate their shape without storing
/// them. Iteration decodes each element on demand and borrows variable-sized
/// fields. No element vector or offset table is allocated. The view stays on its
/// creating thread, like a `WireRecord` projection.
pub struct EncodedSequence<'de, T> {
    bytes: &'de [u8],
    len: usize,
    limits: DecodeLimits,
    depth: usize,
    marker: SequenceMarker<T>,
}
impl<T> Copy for EncodedSequence<'_, T> {}
impl<T> Clone for EncodedSequence<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> std::fmt::Debug for EncodedSequence<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedSequence")
            .field("len", &self.len)
            .field("encoded_bytes", &self.bytes.len())
            .finish()
    }
}
impl<'de, T> EncodedSequence<'de, T> {
    /// Number of validated sequence elements.
    pub fn len(&self) -> usize {
        self.len
    }
    /// Whether the sequence contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// Borrow encoded element bytes, excluding the sequence length prefix.
    pub fn encoded_bytes(&self) -> &'de [u8] {
        self.bytes
    }
}
impl<'de, T: BorrowDecode<'de>> EncodedSequence<'de, T> {
    /// Validate and borrow one length-prefixed positional sequence.
    /// The caller cursor advances only after all elements validate successfully.
    /// # Errors
    /// Malformed elements, truncation, excessive length or nesting return errors.
    pub fn decode(decoder: &mut Deserializer<'de>) -> Result<Self> {
        let mut cursor = decoder.clone();
        let len = cursor.length()?;
        enter(&mut cursor, Some(len))?;
        let contents = cursor.input;
        let depth = cursor.depth;
        for _ in 0..len {
            T::borrow_decode(&mut cursor)?;
        }
        let consumed = contents.len() - cursor.input.len();
        cursor.depth -= 1;
        let sequence = Self {
            bytes: &contents[..consumed],
            len,
            limits: cursor.limits,
            depth,
            marker: PhantomData,
        };
        *decoder = cursor;
        Ok(sequence)
    }
    /// Iterate already validated elements directly without allocating.
    ///
    /// # Panics
    /// Panics if a custom `BorrowDecode` implementation violates its deterministic
    /// decoding contract and rejects bytes it previously accepted. Built-in
    /// implementations always accept the immutable bytes validated by `decode`.
    pub fn values(&self) -> impl Iterator<Item = T> {
        self.iter()
            .map(|value| value.expect("validated sequence element changed its decode result"))
    }
    /// Decode validated elements lazily without an intermediate allocation.
    ///
    /// Errors from custom `BorrowDecode` implementations stop iteration. Built-in
    /// element codecs were already validated when the sequence was constructed.
    pub fn iter(&self) -> EncodedSequenceIter<'de, T> {
        EncodedSequenceIter {
            decoder: Deserializer {
                input: self.bytes,
                depth: self.depth,
                limits: self.limits,
            },
            remaining: self.len,
            marker: PhantomData,
        }
    }
}
/// A lazy iterator that borrows every element from validated positional bytes.
pub struct EncodedSequenceIter<'de, T> {
    decoder: Deserializer<'de>,
    remaining: usize,
    marker: SequenceMarker<T>,
}
impl<'de, T: BorrowDecode<'de>> Iterator for EncodedSequenceIter<'de, T> {
    type Item = Result<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let value = T::borrow_decode(&mut self.decoder);
        if value.is_err() {
            self.remaining = 0;
        } else {
            self.remaining -= 1;
        }
        Some(value)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.remaining))
    }
}
impl<'de, T: BorrowDecode<'de>> std::iter::FusedIterator for EncodedSequenceIter<'de, T> {}
impl<'de, T: BorrowDecode<'de>> IntoIterator for EncodedSequence<'de, T> {
    type Item = Result<T>;
    type IntoIter = EncodedSequenceIter<'de, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'de, T: BorrowDecode<'de>> BorrowDecode<'de> for EncodedSequence<'de, T> {
    fn borrow_decode(decoder: &mut Deserializer<'de>) -> Result<Self> {
        Self::decode(decoder)
    }
}
impl<'de, T: BorrowDecode<'de> + Serialize> Serialize for EncodedSequence<'de, T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.len))?;
        for element in self.iter() {
            let element = element.map_err(serde::ser::Error::custom)?;
            sequence.serialize_element(&element)?;
        }
        sequence.end()
    }
}
fn enter(decoder: &mut Deserializer<'_>, len: Option<usize>) -> Result<()> {
    if len.is_some_and(|n| n > decoder.limits.max_collection_elements)
        || decoder.depth >= decoder.limits.max_depth
    {
        return Err(Error::Codec("collection or nesting limit exceeded".into()));
    }
    decoder.depth += 1;
    Ok(())
}
macro_rules! scalar { ($($ty:ty),* $(,)?) => { $(
    impl<'de> BorrowDecode<'de> for $ty {
        fn borrow_decode(decoder:&mut Deserializer<'de>)->Result<Self> { Self::deserialize(decoder) }
    }
)* }; }
scalar!(
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
    (),
    &'de str
);
impl<'de> BorrowDecode<'de> for &'de [u8] {
    fn borrow_decode(decoder: &mut Deserializer<'de>) -> Result<Self> {
        let len = decoder.length()?;
        decoder.take(len)
    }
}
impl<'de> BorrowDecode<'de> for Cow<'de, str> {
    fn borrow_decode(decoder: &mut Deserializer<'de>) -> Result<Self> {
        <&str>::borrow_decode(decoder).map(Cow::Borrowed)
    }
}
impl<'de> BorrowDecode<'de> for Cow<'de, [u8]> {
    fn borrow_decode(decoder: &mut Deserializer<'de>) -> Result<Self> {
        <&[u8]>::borrow_decode(decoder).map(Cow::Borrowed)
    }
}
impl<'de, T: BorrowDecode<'de>> BorrowDecode<'de> for Option<T> {
    fn borrow_decode(decoder: &mut Deserializer<'de>) -> Result<Self> {
        match decoder.take(1)?[0] {
            0 => Ok(None),
            1 => {
                enter(decoder, None)?;
                let value = T::borrow_decode(decoder);
                decoder.depth -= 1;
                value.map(Some)
            }
            _ => Err(Error::Codec("invalid option tag".into())),
        }
    }
}
macro_rules! tuple { ($($ty:ident),+) => {
    impl<'de,$($ty:BorrowDecode<'de>),+> BorrowDecode<'de> for ($($ty,)+) {
        fn borrow_decode(decoder:&mut Deserializer<'de>)->Result<Self> {
            let fields=[$(stringify!($ty)),+].len();
            enter(decoder,Some(fields))?;
            let result=(||Ok(($($ty::borrow_decode(decoder)?,)+)))();
            decoder.depth-=1; result
        }
    }
}; }
tuple!(A);
tuple!(A, B);
tuple!(A, B, C);
tuple!(A, B, C, D);
tuple!(A, B, C, D, E);
tuple!(A, B, C, D, E, F);
tuple!(A, B, C, D, E, F, G);
tuple!(A, B, C, D, E, F, G, H);

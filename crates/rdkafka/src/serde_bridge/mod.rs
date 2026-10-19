//! Positional binary serde codecs and caller-buffer batch adapters.
//!
//! Structs and tuples encode fields in declaration order without names. Strings,
//! bytes, sequences and maps carry canonical unsigned LEB128 lengths. Fixed-width
//! primitives use little-endian bytes; options use 0 for absent and 1 for present.
//! Enum discriminants are little-endian u32. Both peers must use the same schema;
//! serde attributes that omit fields are unsuitable for this positional format.
//! Borrowed Cow fields require serde's `#[serde(borrow)]` (and `serde_bytes` for
//! byte Cow fields). Ordinary Cow deserialization may choose owned storage.
mod de;
mod sequence;
mod ser;
pub use de::{DecodeLimits, Deserializer};
pub use sequence::{BorrowDecode, EncodedSequence, EncodedSequenceIter};
pub use ser::Serializer;
use serde::{Deserialize, Serialize};

use crate::{BufferPolicy, Error, Result, WireBatch, WireRecord};

/// Stateless adapters for compact binary or caller-provided serde formats.
pub struct SerdeBridge;
impl SerdeBridge {
    /// Append one encoded value to `output`, returning its byte count.
    /// Existing bytes are retained; a serialization error rolls back appended bytes.
    /// # Errors
    /// Unsupported serde shapes or inconsistent compound lengths return an error.
    pub fn encode_into<T: ?Sized + Serialize>(value: &T, output: &mut Vec<u8>) -> Result<usize> {
        #[cfg(feature = "tracing")]
        tracing::trace!(buffer_bytes = output.len(), "encoding serde value");
        Self::encode_into_using(value, output, |value, out| {
            value.serialize(&mut Serializer::new(out))
        })
    }
    /// Append bytes using a caller codec, rolling back appended bytes on failure.
    /// The closure must only append to the buffer. Its error is returned unchanged.
    /// # Errors
    /// Returns the caller codec's error, or an error if it shortened the buffer.
    pub fn encode_into_using<T: ?Sized, F>(
        value: &T,
        output: &mut Vec<u8>,
        encode: F,
    ) -> Result<usize>
    where
        F: FnOnce(&T, &mut Vec<u8>) -> Result<()>,
    {
        let start = output.len();
        if let Err(error) = encode(value, output) {
            output.truncate(start);
            return Err(error);
        }
        output
            .len()
            .checked_sub(start)
            .ok_or_else(|| Error::Codec("codec shortened output buffer".into()))
    }
    /// Decode a single complete payload, borrowing supported fields from it.
    /// # Errors
    /// Malformed, truncated, excessively nested, or trailing bytes return an error.
    pub fn decode_borrowed<'de, T: Deserialize<'de>>(payload: &'de [u8]) -> Result<T> {
        #[cfg(feature = "tracing")]
        tracing::trace!(payload_bytes = payload.len(), "decoding serde value");
        Self::decode_borrowed_with_limits(payload, DecodeLimits::default())
    }
    /// Decode a complete payload with caller-selected resource limits.
    /// # Errors
    /// Malformed bytes, exceeded limits, or trailing bytes return an error.
    pub fn decode_borrowed_with_limits<'de, T: Deserialize<'de>>(
        payload: &'de [u8],
        limits: DecodeLimits,
    ) -> Result<T> {
        let mut decoder = Deserializer::with_limits(payload, limits);
        let value = T::deserialize(&mut decoder)?;
        decoder.finish()?;
        Ok(value)
    }
    /// Decode a nullable wire record payload, borrowing from the wire storage.
    /// # Errors
    /// Null payloads and binary decoding errors are returned to the caller.
    pub fn decode_record_borrowed<'de, T: Deserialize<'de>>(record: WireRecord<'de>) -> Result<T> {
        Self::decode_borrowed(payload(record)?)
    }
    /// Decode with a caller format, preserving its result and error unchanged.
    /// # Errors
    /// Returns the caller codec's error.
    pub fn decode_borrowed_using<'de, T, F>(payload: &'de [u8], decode: F) -> Result<T>
    where
        F: FnOnce(&'de [u8]) -> Result<T>,
    {
        decode(payload)
    }
    /// Append wire frames using scratch bounded by the default byte policy.
    /// Scratch is empty on return; successfully appended records survive later errors.
    /// # Errors
    /// Serialization or wire batch validation errors are returned unchanged.
    pub fn encode_batch_into<T: Serialize>(
        topic: &str,
        values: &[T],
        batch: &mut WireBatch,
        scratch: &mut Vec<u8>,
    ) -> Result<usize> {
        Self::encode_batch_into_with_policy(topic, values, batch, scratch, BufferPolicy::default())
    }
    /// Append wire frames with an explicit scratch retention budget.
    /// Only `policy.max_bytes` limits byte scratch; `max_records` applies to batches.
    /// Scratch is empty and trimmed on success, errors, and callback unwinding.
    /// # Errors
    /// Serialization or wire batch validation errors are returned unchanged.
    pub fn encode_batch_into_with_policy<T: Serialize>(
        topic: &str,
        values: &[T],
        batch: &mut WireBatch,
        scratch: &mut Vec<u8>,
        policy: BufferPolicy,
    ) -> Result<usize> {
        Self::encode_batch_into_using_with_policy(
            topic,
            values,
            batch,
            scratch,
            policy,
            |value, out| value.serialize(&mut Serializer::new(out)),
        )
    }
    /// Append wire frames using a caller codec and default scratch retention.
    /// Scratch is empty on return; successfully appended records survive later errors.
    /// # Errors
    /// Caller codec and wire validation errors are returned unchanged.
    pub fn encode_batch_into_using<T, F>(
        topic: &str,
        values: &[T],
        batch: &mut WireBatch,
        scratch: &mut Vec<u8>,
        encode: F,
    ) -> Result<usize>
    where
        F: FnMut(&T, &mut Vec<u8>) -> Result<()>,
    {
        Self::encode_batch_into_using_with_policy(
            topic,
            values,
            batch,
            scratch,
            BufferPolicy::default(),
            encode,
        )
    }
    /// Append wire frames with a caller codec and explicit scratch byte retention.
    /// `max_records` does not limit scratch. Successfully appended records remain
    /// on later errors, and scratch is emptied and trimmed before returning.
    /// # Errors
    /// Caller codec and wire validation errors are returned unchanged.
    pub fn encode_batch_into_using_with_policy<T, F>(
        topic: &str,
        values: &[T],
        batch: &mut WireBatch,
        scratch: &mut Vec<u8>,
        policy: BufferPolicy,
        mut encode: F,
    ) -> Result<usize>
    where
        F: FnMut(&T, &mut Vec<u8>) -> Result<()>,
    {
        let scratch = ScratchReset {
            buffer: scratch,
            policy,
        };
        for value in values {
            clear_scratch(scratch.buffer, policy);
            Self::encode_into_using(value, scratch.buffer, &mut encode)?;
            batch.push(WireRecord::new(topic, Some(scratch.buffer)))?;
        }
        Ok(values.len())
    }
    /// Decode and deliver one value at a time without an intermediate vector.
    /// Successfully delivered values remain delivered on a later failure.
    /// # Errors
    /// Null payloads, malformed bytes and callback failures return errors unchanged.
    pub fn decode_batch_with<'de, T: Deserialize<'de>, F>(
        batch: &'de WireBatch,
        on_value: F,
    ) -> Result<usize>
    where
        F: FnMut(T) -> Result<()>,
    {
        Self::decode_batch_with_using(batch, Self::decode_borrowed::<T>, on_value)
    }
    /// Decode and deliver values using a caller format without collecting them.
    /// # Errors
    /// Null payloads, caller codec errors and callback errors are returned unchanged.
    pub fn decode_batch_with_using<'de, T, D, F>(
        batch: &'de WireBatch,
        mut decode: D,
        mut on_value: F,
    ) -> Result<usize>
    where
        D: FnMut(&'de [u8]) -> Result<T>,
        F: FnMut(T) -> Result<()>,
    {
        let mut count = 0;
        for record in batch.iter() {
            let value = decode(payload(record)?)?;
            on_value(value)?;
            count += 1;
        }
        Ok(count)
    }
    /// Append decoded values to `output`, retaining previous values on errors.
    /// # Errors
    /// Null payloads and binary decoding errors return an error.
    pub fn decode_batch_into<'de, T: Deserialize<'de>>(
        batch: &'de WireBatch,
        output: &mut Vec<T>,
    ) -> Result<usize> {
        Self::decode_batch_into_using(batch, output, Self::decode_borrowed::<T>)
    }
    /// Append values decoded by a caller format without an intermediate vector.
    /// # Errors
    /// Null payloads and caller codec errors are returned unchanged.
    pub fn decode_batch_into_using<'de, T, D>(
        batch: &'de WireBatch,
        output: &mut Vec<T>,
        decode: D,
    ) -> Result<usize>
    where
        D: FnMut(&'de [u8]) -> Result<T>,
    {
        Self::decode_batch_with_using(batch, decode, |value| {
            output.push(value);
            Ok(())
        })
    }
    /// Fill output slots from index zero; later slots remain unchanged.
    /// An undersized output is rejected before writing any slots. On a decoding
    /// error, successfully decoded earlier slots remain overwritten.
    /// # Errors
    /// Insufficient slots, null payloads and binary decoding errors return an error.
    pub fn decode_batch_into_slice<'de, T: Deserialize<'de>>(
        batch: &'de WireBatch,
        output: &mut [T],
    ) -> Result<usize> {
        Self::decode_batch_into_slice_using(batch, output, Self::decode_borrowed::<T>)
    }
    /// Fill caller slots with a caller format, without an intermediate vector.
    /// # Errors
    /// Insufficient slots, null payloads and caller codec errors return an error.
    pub fn decode_batch_into_slice_using<'de, T, D>(
        batch: &'de WireBatch,
        output: &mut [T],
        decode: D,
    ) -> Result<usize>
    where
        D: FnMut(&'de [u8]) -> Result<T>,
    {
        if batch.len() > output.len() {
            return Err(Error::BatchLengthMismatch {
                expected: batch.len(),
                actual: output.len(),
            });
        }
        let mut index = 0;
        Self::decode_batch_with_using(batch, decode, |value| {
            output[index] = value;
            index += 1;
            Ok(())
        })
    }
}
fn payload(record: WireRecord<'_>) -> Result<&[u8]> {
    record
        .payload
        .ok_or_else(|| Error::Codec("cannot decode a null payload".into()))
}

struct ScratchReset<'a> {
    buffer: &'a mut Vec<u8>,
    policy: BufferPolicy,
}
impl Drop for ScratchReset<'_> {
    fn drop(&mut self) {
        clear_scratch(self.buffer, self.policy);
    }
}
fn clear_scratch(buffer: &mut Vec<u8>, policy: BufferPolicy) {
    buffer.clear();
    buffer.shrink_to(policy.max_bytes);
}

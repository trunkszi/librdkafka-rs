//! Typed records and checked direct payload projections.
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::{Batch, Error, Result, WireBatch, WireRecord};

/// A schema that can encode Kafka metadata and payloads.
pub trait Record: Sized {
    /// Borrowed projection over wire bytes.
    type View<'a>: RecordView<'a, Owned = Self>
    where
        Self: 'a;
    /// Reusable structure-of-arrays storage.
    type Batch: Batch<Record = Self>;
    /// Append the encoded record to reusable wire storage.
    /// # Errors
    /// Returns a codec error when a field cannot be represented.
    fn encode_into(&self, destination: &mut WireBatch) -> Result<()>;
    /// Decode an owned record.
    /// # Errors
    /// Returns an error for invalid or missing fields.
    fn decode(record: WireRecord<'_>) -> Result<Self>;
    /// Borrow a projection from wire bytes.
    /// # Errors
    /// Returns an error for invalid or missing fields.
    fn view<'a>(record: WireRecord<'a>) -> Result<Self::View<'a>>;
}
/// A borrowed typed projection that remains on its creating thread.
pub trait RecordView<'a> {
    /// Owned schema for this projection.
    type Owned: Record;
    /// Materialize the projection as an owned record.
    /// # Errors
    /// Returns an error when projected data violates the schema.
    fn to_owned(&self) -> Result<Self::Owned>;
}
/// Expose a layout-checked payload without encoding individual fields.
pub fn zerocopy_bytes<T>(value: &T) -> &[u8]
where
    T: FromBytes + IntoBytes + KnownLayout + Immutable + Unaligned,
{
    value.as_bytes()
}
/// Borrow a value directly from an exact-size payload.
/// # Errors
/// Rejects payloads whose size does not match the type's layout.
pub fn zerocopy_ref<T>(payload: &[u8]) -> Result<&T>
where
    T: FromBytes + IntoBytes + KnownLayout + Immutable + Unaligned,
{
    T::ref_from_bytes(payload)
        .map_err(|_| Error::Layout("payload size does not match record layout".into()))
}

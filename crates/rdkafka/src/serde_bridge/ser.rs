use serde::ser::{self, Serialize};

use crate::{Error, Result};

/// A positional serializer writing directly into a caller-owned buffer.
/// Lengths are canonical unsigned LEB128; numbers are little-endian.
/// Options use `0` for absent and `1` for present.
pub struct Serializer<'a> {
    output: &'a mut Vec<u8>,
}
impl<'a> Serializer<'a> {
    /// Append encoded fields to `output` without clearing it.
    pub fn new(output: &'a mut Vec<u8>) -> Self {
        Self { output }
    }
    fn length(&mut self, mut value: usize) {
        loop {
            let byte = (value & 127) as u8;
            value >>= 7;
            self.output.push(byte | if value == 0 { 0 } else { 128 });
            if value == 0 {
                break;
            }
        }
    }
}
/// A compound value with an exact declared field count.
pub struct Compound<'a, 'b> {
    serializer: &'a mut Serializer<'b>,
    remaining: usize,
    map_value: bool,
}
impl Compound<'_, '_> {
    fn element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        if self.remaining == 0 {
            return Err(Error::Codec("too many compound elements".into()));
        }
        value.serialize(&mut *self.serializer)?;
        self.remaining -= 1;
        Ok(())
    }
    fn finish(self) -> Result<()> {
        if self.remaining != 0 || self.map_value {
            Err(Error::Codec("compound element count mismatch".into()))
        } else {
            Ok(())
        }
    }
}
macro_rules! primitive { ($($method:ident($ty:ty)),* $(,)?) => { $(
    fn $method(self, value: $ty) -> Result<()> { self.output.extend_from_slice(&value.to_le_bytes()); Ok(()) }
)* }; }
impl<'a, 'b> ser::Serializer for &'a mut Serializer<'b> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Compound<'a, 'b>;
    type SerializeTuple = Compound<'a, 'b>;
    type SerializeTupleStruct = Compound<'a, 'b>;
    type SerializeTupleVariant = Compound<'a, 'b>;
    type SerializeMap = Compound<'a, 'b>;
    type SerializeStruct = Compound<'a, 'b>;
    type SerializeStructVariant = Compound<'a, 'b>;
    primitive!(
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_f32(f32),
        serialize_f64(f64)
    );
    fn serialize_bool(self, v: bool) -> Result<()> {
        self.output.push(u8::from(v));
        Ok(())
    }
    fn serialize_char(self, v: char) -> Result<()> {
        self.serialize_u32(v as u32)
    }
    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_bytes(v.as_bytes())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.length(v.len());
        self.output.extend_from_slice(v);
        Ok(())
    }
    fn serialize_none(self) -> Result<()> {
        self.output.push(0);
        Ok(())
    }
    fn serialize_some<T: ?Sized + Serialize>(self, v: &T) -> Result<()> {
        self.output.push(1);
        v.serialize(self)
    }
    fn serialize_unit(self) -> Result<()> {
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<()> {
        Ok(())
    }
    fn serialize_unit_variant(self, _: &'static str, index: u32, _: &'static str) -> Result<()> {
        self.serialize_u32(index)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(self, _: &'static str, v: &T) -> Result<()> {
        v.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        index: u32,
        _: &'static str,
        v: &T,
    ) -> Result<()> {
        ser::Serializer::serialize_u32(&mut *self, index)?;
        v.serialize(self)
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let len = len.ok_or_else(|| Error::Codec("sequence length required".into()))?;
        self.length(len);
        self.serialize_tuple(len)
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        Ok(Compound {
            serializer: self,
            remaining: len,
            map_value: false,
        })
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_tuple(len)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        index: u32,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        ser::Serializer::serialize_u32(&mut *self, index)?;
        self.serialize_tuple(len)
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        let len = len.ok_or_else(|| Error::Codec("map length required".into()))?;
        self.length(len);
        self.serialize_tuple(len)
    }
    fn serialize_struct(self, _: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_tuple(len)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        index: u32,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        ser::Serializer::serialize_u32(&mut *self, index)?;
        self.serialize_tuple(len)
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}
macro_rules! compound {
    ($trait:ident,$method:ident) => {
        impl ser::$trait for Compound<'_, '_> {
            type Ok = ();
            type Error = Error;
            fn $method<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<()> {
                self.element(v)
            }
            fn end(self) -> Result<()> {
                self.finish()
            }
        }
    };
}
compound!(SerializeSeq, serialize_element);
compound!(SerializeTuple, serialize_element);
compound!(SerializeTupleStruct, serialize_field);
compound!(SerializeTupleVariant, serialize_field);
macro_rules! structure {
    ($trait:ident) => {
        impl ser::$trait for Compound<'_, '_> {
            type Ok = ();
            type Error = Error;
            fn serialize_field<T: ?Sized + Serialize>(
                &mut self,
                _: &'static str,
                v: &T,
            ) -> Result<()> {
                self.element(v)
            }
            fn end(self) -> Result<()> {
                self.finish()
            }
        }
    };
}
structure!(SerializeStruct);
structure!(SerializeStructVariant);
impl ser::SerializeMap for Compound<'_, '_> {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
        if self.map_value || self.remaining == 0 {
            return Err(Error::Codec("unexpected map key".into()));
        }
        key.serialize(&mut *self.serializer)?;
        self.map_value = true;
        Ok(())
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        if !self.map_value {
            return Err(Error::Codec("map value without key".into()));
        }
        self.element(value)?;
        self.map_value = false;
        Ok(())
    }
    fn end(self) -> Result<()> {
        self.finish()
    }
}

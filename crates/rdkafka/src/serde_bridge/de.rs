use serde::de::{self, DeserializeSeed, Visitor};

use crate::{Error, Result};

/// Resource limits for untrusted positional payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeLimits {
    /// Maximum elements in an individual sequence, tuple, struct or map.
    pub max_collection_elements: usize,
    /// Maximum recursively nested compound values.
    pub max_depth: usize,
}
impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_collection_elements: 1_048_576,
            max_depth: 128,
        }
    }
}

/// A positional binary cursor borrowing string and byte values from its input.
/// Collection lengths are limited to 1,048,576 elements and nesting to 128.
#[derive(Clone)]
pub struct Deserializer<'de> {
    pub(super) input: &'de [u8],
    pub(super) depth: usize,
    pub(super) limits: DecodeLimits,
}
impl<'de> Deserializer<'de> {
    /// Start decoding fields from `input`.
    pub fn new(input: &'de [u8]) -> Self {
        Self::with_limits(input, DecodeLimits::default())
    }
    /// Start decoding with explicit collection and nesting limits.
    pub fn with_limits(input: &'de [u8], limits: DecodeLimits) -> Self {
        Self {
            input,
            depth: 0,
            limits,
        }
    }
    /// Verify that the entire payload was consumed.
    /// # Errors
    /// Returns an error if bytes remain after the decoded value.
    pub fn finish(&self) -> Result<()> {
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(Error::Codec("trailing payload bytes".into()))
        }
    }
    pub(super) fn take(&mut self, len: usize) -> Result<&'de [u8]> {
        if len > self.input.len() {
            return Err(Error::Codec("truncated payload".into()));
        }
        let (value, rest) = self.input.split_at(len);
        self.input = rest;
        Ok(value)
    }
    pub(super) fn length(&mut self) -> Result<usize> {
        let mut value = 0u64;
        for index in 0..10 {
            let byte = self.take(1)?[0];
            if index == 9 && byte > 1 {
                return Err(Error::Codec("length overflow".into()));
            }
            value |= u64::from(byte & 127) << (index * 7);
            if byte & 128 == 0 {
                if index != 0 && byte == 0 {
                    return Err(Error::Codec("noncanonical length".into()));
                }
                return usize::try_from(value).map_err(|_| Error::Codec("length overflow".into()));
            }
        }
        Err(Error::Codec("length overflow".into()))
    }
    fn sequence<V: Visitor<'de>>(&mut self, len: usize, visitor: V) -> Result<V::Value> {
        if len > self.limits.max_collection_elements || self.depth >= self.limits.max_depth {
            return Err(Error::Codec("collection or nesting limit exceeded".into()));
        }
        self.depth += 1;
        let mut access = Access {
            de: self,
            remaining: len,
            pending: false,
        };
        let result = visitor.visit_seq(&mut access);
        let remaining = access.remaining;
        self.depth -= 1;
        let value = result?;
        if remaining != 0 {
            return Err(Error::Codec("unconsumed sequence elements".into()));
        }
        Ok(value)
    }
    fn nested<V: Visitor<'de>>(&mut self, visitor: V, option: bool) -> Result<V::Value> {
        if self.depth >= self.limits.max_depth {
            return Err(Error::Codec("nesting limit exceeded".into()));
        }
        self.depth += 1;
        let result = if option {
            visitor.visit_some(&mut *self)
        } else {
            visitor.visit_newtype_struct(&mut *self)
        };
        self.depth -= 1;
        result
    }
}
struct Access<'a, 'de> {
    de: &'a mut Deserializer<'de>,
    remaining: usize,
    pending: bool,
}
impl<'de> de::SeqAccess<'de> for &mut Access<'_, 'de> {
    type Error = Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let value = seed.deserialize(&mut *self.de)?;
        self.remaining -= 1;
        Ok(Some(value))
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining.min(4096))
    }
}
impl<'de> de::MapAccess<'de> for &mut Access<'_, 'de> {
    type Error = Error;
    fn next_key_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.pending {
            return Err(Error::Codec("map value was not read".into()));
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        let value = seed.deserialize(&mut *self.de)?;
        self.pending = true;
        Ok(Some(value))
    }
    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value> {
        if !self.pending {
            return Err(Error::Codec("map value without key".into()));
        }
        let value = seed.deserialize(&mut *self.de)?;
        self.remaining -= 1;
        self.pending = false;
        Ok(value)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining.min(4096))
    }
}
macro_rules! primitive { ($($method:ident,$visit:ident,$ty:ty,$len:expr);* $(;)?) => { $(
    fn $method<V:Visitor<'de>>(self,visitor:V)->Result<V::Value> {
        let mut bytes=[0u8;$len]; bytes.copy_from_slice(self.take($len)?);
        visitor.$visit(<$ty>::from_le_bytes(bytes))
    }
)* }; }
impl<'de> de::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = Error;
    primitive!(deserialize_i8,visit_i8,i8,1; deserialize_i16,visit_i16,i16,2; deserialize_i32,visit_i32,i32,4; deserialize_i64,visit_i64,i64,8; deserialize_i128,visit_i128,i128,16; deserialize_u8,visit_u8,u8,1; deserialize_u16,visit_u16,u16,2; deserialize_u32,visit_u32,u32,4; deserialize_u64,visit_u64,u64,8; deserialize_u128,visit_u128,u128,16; deserialize_f32,visit_f32,f32,4; deserialize_f64,visit_f64,f64,8);
    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        Err(Error::Codec(
            "positional format requires a concrete type".into(),
        ))
    }
    fn deserialize_bool<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        match self.take(1)?[0] {
            0 => v.visit_bool(false),
            1 => v.visit_bool(true),
            _ => Err(Error::Codec("invalid boolean tag".into())),
        }
    }
    fn deserialize_char<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(self.take(4)?);
        let value = char::from_u32(u32::from_le_bytes(bytes))
            .ok_or_else(|| Error::Codec("invalid Unicode scalar".into()))?;
        v.visit_char(value)
    }
    fn deserialize_str<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        let len = self.length()?;
        let text = std::str::from_utf8(self.take(len)?)
            .map_err(|_| Error::Codec("invalid UTF-8".into()))?;
        v.visit_borrowed_str(text)
    }
    fn deserialize_string<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        self.deserialize_str(v)
    }
    fn deserialize_bytes<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        let len = self.length()?;
        v.visit_borrowed_bytes(self.take(len)?)
    }
    fn deserialize_byte_buf<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        self.deserialize_bytes(v)
    }
    fn deserialize_option<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        match self.take(1)?[0] {
            0 => v.visit_none(),
            1 => self.nested(v, true),
            _ => Err(Error::Codec("invalid option tag".into())),
        }
    }
    fn deserialize_unit<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        v.visit_unit()
    }
    fn deserialize_unit_struct<V: Visitor<'de>>(self, _: &'static str, v: V) -> Result<V::Value> {
        self.deserialize_unit(v)
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        v: V,
    ) -> Result<V::Value> {
        self.nested(v, false)
    }
    fn deserialize_seq<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        let len = self.length()?;
        self.sequence(len, v)
    }
    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, v: V) -> Result<V::Value> {
        self.sequence(len, v)
    }
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        len: usize,
        v: V,
    ) -> Result<V::Value> {
        self.sequence(len, v)
    }
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        fields: &'static [&'static str],
        v: V,
    ) -> Result<V::Value> {
        self.sequence(fields.len(), v)
    }
    fn deserialize_map<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        let len = self.length()?;
        if len > self.limits.max_collection_elements || self.depth >= self.limits.max_depth {
            return Err(Error::Codec("collection or nesting limit exceeded".into()));
        }
        self.depth += 1;
        let mut access = Access {
            de: self,
            remaining: len,
            pending: false,
        };
        let result = v.visit_map(&mut access);
        let remaining = access.remaining;
        let pending = access.pending;
        self.depth -= 1;
        let value = result?;
        if remaining != 0 || pending {
            return Err(Error::Codec("unconsumed map entries".into()));
        }
        Ok(value)
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        v: V,
    ) -> Result<V::Value> {
        if self.depth >= self.limits.max_depth {
            return Err(Error::Codec("nesting limit exceeded".into()));
        }
        self.depth += 1;
        let result = v.visit_enum(Enum { de: self });
        self.depth -= 1;
        result
    }
    fn deserialize_identifier<V: Visitor<'de>>(self, v: V) -> Result<V::Value> {
        self.deserialize_u32(v)
    }
    fn deserialize_ignored_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
        Err(Error::Codec(
            "cannot skip an unknown positional value".into(),
        ))
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}
struct Enum<'a, 'de> {
    de: &'a mut Deserializer<'de>,
}
impl<'a, 'de> de::EnumAccess<'de> for Enum<'a, 'de> {
    type Error = Error;
    type Variant = Self;
    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self)> {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(self.de.take(4)?);
        let index = u32::from_le_bytes(bytes);
        let value = seed.deserialize(de::value::U32Deserializer::<Error>::new(index))?;
        Ok((value, self))
    }
}
impl<'de> de::VariantAccess<'de> for Enum<'_, 'de> {
    type Error = Error;
    fn unit_variant(self) -> Result<()> {
        Ok(())
    }
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(self.de)
    }
    fn tuple_variant<V: Visitor<'de>>(self, len: usize, v: V) -> Result<V::Value> {
        self.de.sequence(len, v)
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        v: V,
    ) -> Result<V::Value> {
        self.de.sequence(fields.len(), v)
    }
}

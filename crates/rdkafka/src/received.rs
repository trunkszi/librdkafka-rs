//! Native message ownership with borrowed payload and key access.
use std::rc::Rc;

use rdkafka_sys::ffi;
use uarch::prelude::*;

use crate::{Headers, Result, core::Core};

#[register_pass]
#[derive(Clone, Copy)]
struct MessagePosition {
    offset: i64,
    timestamp: i64,
}
assert_layout!(MessagePosition, size = 16, align = align_of::<i64>());
/// Owns one native broker message; byte slices borrow its native storage.
/// The originating client remains alive until the message is released.
pub struct ReceivedMessage {
    native: cxx::UniquePtr<ffi::Message>,
    _owner: Rc<Core>,
}
impl ReceivedMessage {
    #[hot_inline]
    fn position(&self) -> MessagePosition {
        MessagePosition {
            offset: ffi::message_offset(&self.native),
            timestamp: ffi::message_timestamp(&self.native),
        }
    }
    pub(crate) fn wrap(
        native: cxx::UniquePtr<ffi::Message>,
        owner: &Rc<Core>,
    ) -> Result<Option<Self>> {
        let Some(m) = native.as_ref() else {
            return Ok(None);
        };
        let code = ffi::message_error(m);
        if code == -185 {
            return Ok(None);
        }
        if code != 0 {
            return Err(crate::core::consumer_error(m, code));
        }
        Ok(Some(Self {
            native,
            _owner: Rc::clone(owner),
        }))
    }
    pub fn topic(&self) -> String {
        ffi::message_topic(&self.native)
    }
    pub fn partition(&self) -> i32 {
        ffi::message_partition(&self.native)
    }
    #[hot_inline]
    pub fn offset(&self) -> i64 {
        ffi::message_offset(&self.native)
    }
    #[hot_inline]
    pub fn timestamp(&self) -> Option<i64> {
        let t = ffi::message_timestamp(&self.native);
        (t >= 0).then_some(t)
    }
    #[hot_inline]
    pub fn key(&self) -> Option<&[u8]> {
        ffi::message_has_key(&self.native).then(|| ffi::message_key(&self.native))
    }
    #[hot_inline]
    pub fn payload(&self) -> Option<&[u8]> {
        ffi::message_has_payload(&self.native).then(|| ffi::message_payload(&self.native))
    }
    /// Copies header metadata; key and payload access do not copy native storage.
    pub fn headers(&self) -> Headers {
        let mut headers = Headers::new();
        for h in ffi::message_headers(&self.native) {
            headers.add(h.name, if h.has_value { Some(&h.value) } else { None });
        }
        headers
    }
    pub fn to_owned(&self) -> crate::Message {
        let position = self.position();
        crate::Message {
            topic: self.topic(),
            partition: self.partition(),
            offset: position.offset,
            timestamp: (position.timestamp >= 0).then_some(position.timestamp),
            key: self.key().map(<[u8]>::to_vec),
            payload: self.payload().map(<[u8]>::to_vec),
            headers: self.headers(),
            token: 0,
        }
    }
}

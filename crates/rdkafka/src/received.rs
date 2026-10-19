//! Native message ownership with borrowed payload and key access.
use std::rc::Rc;

use rdkafka_sys::ffi;

use crate::{Error, Headers, Result, core::Core};
/// Owns one native broker message; byte slices borrow its native storage.
/// The originating client remains alive until the message is released.
pub struct ReceivedMessage {
    native: cxx::UniquePtr<ffi::Message>,
    _owner: Rc<Core>,
}
impl ReceivedMessage {
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
            return Err(Error::native(code, ffi::error_message(code)));
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
    pub fn offset(&self) -> i64 {
        ffi::message_offset(&self.native)
    }
    pub fn timestamp(&self) -> Option<i64> {
        let t = ffi::message_timestamp(&self.native);
        (t >= 0).then_some(t)
    }
    pub fn key(&self) -> Option<&[u8]> {
        ffi::message_has_key(&self.native).then(|| ffi::message_key(&self.native))
    }
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
        crate::Message {
            topic: self.topic(),
            partition: self.partition(),
            offset: self.offset(),
            timestamp: self.timestamp(),
            key: self.key().map(<[u8]>::to_vec),
            payload: self.payload().map(<[u8]>::to_vec),
            headers: self.headers(),
            token: 0,
        }
    }
}

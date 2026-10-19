//! Shared ownership ensures child queues and topics outlive their native client.
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use rdkafka_sys::ffi;

use crate::{callbacks::*, *};
pub(crate) struct Core {
    pub native: RefCell<cxx::UniquePtr<ffi::Client>>,
    pub callbacks: Rc<RefCell<Callbacks>>,
    active: Rc<Cell<bool>>,
}
pub(crate) struct DriveGuard {
    active: Rc<Cell<bool>>,
}
impl Drop for DriveGuard {
    fn drop(&mut self) {
        self.active.set(false);
    }
}
pub(crate) fn status(code: i32) -> Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(Error::native(code, ffi::error_message(code)))
    }
}
pub(crate) fn partitions(parts: &[TopicPartition]) -> Vec<ffi::NativePartition> {
    parts
        .iter()
        .map(|p| ffi::NativePartition {
            topic: p.topic.clone(),
            partition: p.partition,
            offset: p.offset,
            error: p.error,
        })
        .collect()
}
pub(crate) fn from_parts(parts: Vec<ffi::NativePartition>) -> Vec<TopicPartition> {
    parts
        .into_iter()
        .map(|p| {
            let mut v = TopicPartition::new(p.topic, p.partition, p.offset);
            v.error = p.error;
            v
        })
        .collect()
}
pub(crate) fn message(native: cxx::UniquePtr<ffi::Message>) -> Result<Option<Message>> {
    let Some(m) = native.as_ref() else {
        return Ok(None);
    };
    let code = ffi::message_error(m);
    if code == -185 {
        return Ok(None);
    }
    status(code)?;
    let mut headers = Headers::new();
    for h in ffi::message_headers(m) {
        headers.add(h.name, if h.has_value { Some(&h.value) } else { None });
    }
    let timestamp = ffi::message_timestamp(m);
    Ok(Some(Message {
        topic: ffi::message_topic(m),
        partition: ffi::message_partition(m),
        offset: ffi::message_offset(m),
        timestamp: (timestamp >= 0).then_some(timestamp),
        key: ffi::message_has_key(m).then(|| ffi::message_key(m).to_vec()),
        payload: ffi::message_has_payload(m).then(|| ffi::message_payload(m).to_vec()),
        headers,
        token: 0,
    }))
}
impl Core {
    pub fn new(conf: &Conf, kind: i32) -> Result<Rc<Self>> {
        Ok(Rc::new(Self {
            native: RefCell::new(ffi::client_new(&conf.native, kind)?),
            callbacks: Rc::clone(&conf.callbacks),
            active: Rc::clone(&conf.callbacks.borrow().active),
        }))
    }
    pub fn begin_drive(&self) -> Result<DriveGuard> {
        if self.active.get() {
            return Err(Error::ReentrantDrive);
        }
        self.active.set(true);
        Ok(DriveGuard {
            active: Rc::clone(&self.active),
        })
    }
    pub fn poll(&self, timeout: i32) -> Result<usize> {
        let _guard = self.begin_drive()?;
        self.drain(timeout)
    }
    pub fn drain(&self, timeout: i32) -> Result<usize> {
        let events = ffi::client_poll(self.native.borrow_mut().pin_mut(), timeout);
        let n = events.len();
        macro_rules! invoke {
            ($slot:ident, $($arg:expr),*) => {{
                let callback=self.callbacks.borrow_mut().$slot.take();
                if let Some(mut cb)=callback {
                    cb($($arg),*);
                    let mut callbacks=self.callbacks.borrow_mut();
                    if callbacks.$slot.is_none(){callbacks.$slot=Some(cb);}
                }
            }}
        }
        for e in events {
            if e.kind == 12 {
                return Err(Error::native(e.code, e.message));
            }
            match e.kind {
                0 => invoke!(
                    delivery,
                    &DeliveryReport {
                        topic: e.topic,
                        partition: e.partition,
                        offset: e.offset,
                        token: e.token,
                        error: (e.code != 0).then_some(ErrorCode(e.code))
                    }
                ),
                1..=4 => invoke!(
                    event,
                    &match e.kind {
                        2 => Event::Stats(e.message),
                        3 => Event::Log(e.message),
                        4 => Event::Throttle(e.message),
                        _ => Event::Error {
                            code: e.code,
                            message: e.message
                        },
                    }
                ),
                5 => invoke!(rebalance, e.code, &from_parts(e.partitions)),
                6 => invoke!(commit, e.code, &from_parts(e.partitions)),
                7 => {
                    let callback = self.callbacks.borrow_mut().oauth.take();
                    if let Some(mut cb) = callback {
                        let result = cb(&e.message);
                        let mut state = self.callbacks.borrow_mut();
                        if state.oauth.is_none() {
                            state.oauth = Some(cb);
                        }
                        result?;
                    }
                }
                8 => invoke!(socket, e.code, e.partition, e.offset as i32),
                9 => invoke!(open, &e.message, e.code, e.partition),
                _ => {}
            }
        }
        Ok(n)
    }
    pub fn observe(&self, m: &Message) {
        let callback = self.callbacks.borrow_mut().consume.take();
        if let Some(mut cb) = callback {
            cb(m);
            let mut state = self.callbacks.borrow_mut();
            if state.consume.is_none() {
                state.consume = Some(cb);
            }
        }
    }
    pub fn operation(
        &self,
        op: i32,
        parts: &[TopicPartition],
        timeout: i32,
    ) -> Result<Vec<TopicPartition>> {
        for p in parts {
            text(&p.topic)?;
        }
        Ok(from_parts(ffi::client_partitions(
            self.native.borrow_mut().pin_mut(),
            op,
            &partitions(parts),
            timeout,
        )?))
    }
    pub fn metadata(&self, topic: Option<&str>, timeout: i32) -> Result<Metadata> {
        if let Some(topic) = topic {
            text(topic)?;
        }
        let m = ffi::client_metadata(
            self.native.borrow_mut().pin_mut(),
            topic.unwrap_or(""),
            timeout,
        )?;
        Ok(Metadata {
            brokers: m
                .broker_ids
                .into_iter()
                .zip(m.broker_hosts)
                .zip(m.broker_ports)
                .map(|((id, host), port)| BrokerMetadata { id, host, port })
                .collect(),
            topics: m
                .topics
                .into_iter()
                .map(|t| TopicMetadata {
                    name: t.name,
                    error: t.error,
                    partitions: t
                        .partition_metadata
                        .into_iter()
                        .map(|p| PartitionMetadata {
                            id: p.id,
                            error: p.error,
                            leader: p.leader,
                            replicas: p.replicas,
                            isrs: p.isr,
                        })
                        .collect(),
                })
                .collect(),
            _thread: Default::default(),
        })
    }
}

pub(crate) fn text(value: &str) -> Result<()> {
    if value.contains('\0') {
        Err(Error::Config("native text contains an embedded NUL".into()))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unit_scenario_dispatch_guard_resets_during_unwind() {
        let active = Rc::new(Cell::new(true));
        let flag = Rc::clone(&active);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = DriveGuard { active: flag };
            panic!("user callback panic");
        }));
        assert!(result.is_err());
        assert!(!active.get());
    }
}

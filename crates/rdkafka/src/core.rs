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
    pub partition_counts: RefCell<std::collections::HashMap<String, i32>>,
    pub pending_error: RefCell<Option<Error>>,
    pending_events: RefCell<std::collections::VecDeque<ffi::NativeEvent>>,
    pub rebalance_snapshot: RefCell<Option<(bool, String)>>,
}
pub(crate) struct DriveGuard {
    active: Rc<Cell<bool>>,
}
impl Drop for DriveGuard {
    fn drop(&mut self) {
        self.active.set(false);
    }
}
struct RebalanceScope<'a>(&'a RefCell<Option<(bool, String)>>);
impl Drop for RebalanceScope<'_> {
    fn drop(&mut self) {
        self.0.borrow_mut().take();
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
            leader_epoch: p.leader_epoch.unwrap_or(-1),
            metadata: p.metadata.clone(),
        })
        .collect()
}
pub(crate) fn from_parts(parts: Vec<ffi::NativePartition>) -> Vec<TopicPartition> {
    parts
        .into_iter()
        .map(|p| {
            let mut v = TopicPartition::new(p.topic, p.partition, p.offset);
            v.error = p.error;
            v.leader_epoch = (p.leader_epoch >= 0).then_some(p.leader_epoch);
            v.metadata = p.metadata;
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
        conf.validate_transport()?;
        Ok(Rc::new(Self {
            native: RefCell::new(ffi::client_new_with_partitioner(&conf.native, kind, {
                let state = conf.callbacks.borrow();
                state.partitioner.is_some() || state.key_partitioner.is_some()
            })?),
            callbacks: Rc::clone(&conf.callbacks),
            active: Rc::clone(&conf.callbacks.borrow().active),
            rebalance_snapshot: RefCell::default(),
            pending_error: RefCell::default(),
            partition_counts: RefCell::default(),
            pending_events: RefCell::default(),
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
        self.take_pending_error()?;
        self.drain(timeout)
    }
    pub fn drain(&self, timeout: i32) -> Result<usize> {
        let events = ffi::client_poll(self.native.borrow_mut().pin_mut(), timeout);
        self.dispatch(events)
    }
    fn dispatch(&self, events: Vec<ffi::NativeEvent>) -> Result<usize> {
        let mut failure = None;
        macro_rules! record {
            ($error:expr) => {{
                let error = $error;
                if failure.is_none()
                    || (matches!(&error, Error::Tls(_)) && !matches!(&failure, Some(Error::Tls(_))))
                {
                    failure = Some(error);
                }
            }};
        }
        macro_rules! invoke {
            ($slot:ident, $($arg:expr),*) => {{
                with_callback(&self.callbacks, |state| &mut state.$slot, |cb| cb($($arg),*));
            }}
        }
        self.pending_events.borrow_mut().extend(events);
        let n = self.pending_events.borrow().len();
        loop {
            let Some(e) = self.pending_events.borrow_mut().pop_front() else {
                break;
            };
            if e.kind == 12 {
                record!(Error::Native {
                    code: ErrorCode(e.code),
                    message: e.message,
                    retriable: true,
                    fatal: false,
                    txn_requires_abort: false,
                });
                continue;
            }
            match e.kind {
                13 => {
                    invoke!(
                        event,
                        &Event::Error {
                            code: e.code,
                            message: e.message.clone(),
                            is_fatal: true
                        }
                    );
                    record!(Error::Tls(e.message));
                }
                0 => invoke!(delivery, &DeliveryReport::from_native(&e)),
                1..=4 => invoke!(event, &Event::from_native(&e)),
                5 => {
                    *self.rebalance_snapshot.borrow_mut() =
                        Some((e.assignment_lost, e.rebalance_protocol));
                    let _scope = RebalanceScope(&self.rebalance_snapshot);
                    invoke!(rebalance, e.code, &from_parts(e.partitions));
                }
                6 => invoke!(commit, e.code, &from_parts(e.partitions)),
                7 => {
                    if let Some(Err(error)) = with_callback(
                        &self.callbacks,
                        |state| &mut state.oauth,
                        |cb| cb(&e.message),
                    ) {
                        record!(error);
                    }
                }
                8 => invoke!(socket, e.code, e.partition, e.offset as i32),
                9 => invoke!(open, &e.message, e.code, e.partition),
                10 => {
                    if let Err(error) = status(e.code) {
                        record!(error);
                        continue;
                    }
                    let mut headers = Headers::new();
                    for header in e.headers {
                        headers.add(
                            header.name,
                            header.has_value.then_some(header.value.as_slice()),
                        );
                    }
                    self.observe(&Message {
                        topic: e.topic,
                        partition: e.partition,
                        offset: e.offset,
                        timestamp: (e.timestamp >= 0).then_some(e.timestamp),
                        key: e.has_key.then_some(e.key),
                        payload: e.has_payload.then_some(e.payload),
                        headers,
                        token: e.token,
                    });
                }
                _ => {}
            }
        }
        failure.map_or(Ok(n), Err)
    }
    /// Completes an operation after its native borrow has been released.
    /// The caller retains its drive guard throughout callback dispatch. TLS
    /// failures take priority; other callback failures follow operation errors.
    pub fn complete<T>(&self, outcome: Result<T>) -> Result<T> {
        let callbacks = self.drain(0);
        match callbacks {
            Err(error @ Error::Tls(_)) => Err(error),
            callbacks => outcome.and_then(|value| callbacks.map(|_| value)),
        }
    }
    /// Reports a deferred callback failure before another receive advances offsets.
    pub fn take_pending_error(&self) -> Result<()> {
        self.pending_error.borrow_mut().take().map_or(Ok(()), Err)
    }
    /// Returns fetched records before reporting failures from background callbacks.
    pub fn complete_received<T>(
        &self,
        outcome: Result<T>,
        nonempty: impl FnOnce(&T) -> bool,
    ) -> Result<T> {
        let callbacks = self.drain(0);
        self.finish_received(outcome, callbacks, nonempty)
    }
    fn finish_received<T>(
        &self,
        outcome: Result<T>,
        callbacks: Result<usize>,
        nonempty: impl FnOnce(&T) -> bool,
    ) -> Result<T> {
        if let Ok(value) = &outcome
            && nonempty(value)
        {
            if let Err(error) = callbacks {
                let mut pending = self.pending_error.borrow_mut();
                if pending.is_none() {
                    *pending = Some(error);
                }
            }
            return outcome;
        }
        match callbacks {
            Err(error @ Error::Tls(_)) => Err(error),
            callbacks => outcome.and_then(|value| callbacks.map(|_| value)),
        }
    }
    /// Queries from callbacks reuse the active drive without recursive dispatch.
    pub fn query<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        let guard = if self.active.get() {
            None
        } else {
            Some(self.begin_drive()?)
        };
        let outcome = operation();
        if guard.is_some() {
            self.complete(outcome)
        } else {
            outcome
        }
    }
    pub fn observe(&self, m: &Message) {
        with_callback(&self.callbacks, |state| &mut state.consume, |cb| cb(m));
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
        self.query(|| {
            let outcome = ffi::client_partitions(
                self.native.borrow_mut().pin_mut(),
                op,
                &partitions(parts),
                timeout,
            );
            outcome.map(from_parts).map_err(Error::from)
        })
    }
    pub fn metadata(&self, topic: Option<&str>, timeout: i32) -> Result<Metadata> {
        if let Some(topic) = topic {
            text(topic)?;
        }
        let m = self.query(|| {
            ffi::client_metadata(
                self.native.borrow_mut().pin_mut(),
                topic.unwrap_or(""),
                timeout,
            )
            .map_err(Error::from)
        })?;
        let metadata = Metadata {
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
        };
        let mut counts = self.partition_counts.borrow_mut();
        if let Some(topic) = topic {
            counts.remove(topic);
        } else {
            counts.clear();
        }
        for topic in &metadata.topics {
            if topic.error == 0 && !topic.partitions.is_empty() {
                counts.insert(topic.name.clone(), topic.partitions.len() as i32);
            }
        }
        Ok(metadata)
    }
}

pub(crate) fn text(value: &str) -> Result<()> {
    if value.contains('\0') {
        Err(Error::Config("native text contains an embedded NUL".into()))
    } else {
        Ok(())
    }
}

pub(crate) fn timeout(duration: std::time::Duration) -> Result<i32> {
    i32::try_from(duration.as_millis())
        .map_err(|_| Error::Config("timeout exceeds native millisecond range".into()))
}

#[cfg(test)]
#[path = "core_tests.rs"]
mod dispatch_tests;

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

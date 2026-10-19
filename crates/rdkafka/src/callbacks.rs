//! Calling-thread callbacks drained after native operations release their borrow.
use std::time::Duration;

use crate::{Error, Message, TopicPartition};
#[path = "event_conversion.rs"]
mod event_conversion;

/// Broker persistence evidence attached to one delivery report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MsgStatus {
    /// The message was never transmitted or was rejected before persistence.
    NotPersisted,
    /// Transmission occurred but persistence could not be confirmed.
    PossiblyPersisted,
    /// The broker acknowledged persistence under the configured acknowledgements.
    Persisted,
    /// An unrecognized native status, retained without claiming persistence.
    Unknown(i32),
}
impl MsgStatus {
    /// Preserves all native status values, including future additions.
    pub const fn from_raw(status: i32) -> Self {
        match status {
            0 => Self::NotPersisted,
            1 => Self::PossiblyPersisted,
            2 => Self::Persisted,
            v => Self::Unknown(v),
        }
    }
}
#[derive(Clone, Debug)]
pub struct DeliveryReport {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub token: u64,
    pub error: Option<crate::ErrorCode>,
    pub status: MsgStatus,
    /// Native producer latency; absent when native timing was unavailable.
    pub latency: Option<Duration>,
}
#[derive(Clone, Debug)]
pub enum Event {
    Error {
        code: i32,
        message: String,
        is_fatal: bool,
    },
    Stats(String),
    Log {
        level: i32,
        fac: String,
        message: String,
    },
    Throttle {
        broker_name: String,
        broker_id: i32,
        throttle_time: Duration,
    },
    Socket {
        domain: i32,
        kind: i32,
        protocol: i32,
    },
    Open {
        path: String,
        flags: i32,
        mode: i32,
    },
}
pub type DeliveryReportCb = Box<dyn FnMut(&DeliveryReport)>;
pub type EventCb = Box<dyn FnMut(&Event)>;
pub type ConsumeCb = Box<dyn FnMut(&Message)>;
pub type RebalanceCb = Box<dyn FnMut(i32, &[TopicPartition])>;
pub type OffsetCommitCb = Box<dyn FnMut(i32, &[TopicPartition])>;
pub type PartitionerCb = Box<dyn FnMut(&str, Option<&[u8]>, i32) -> i32>;
pub type PartitionerKeyPointerCb = Box<dyn FnMut(&str, Option<&[u8]>, i32) -> i32>;
pub type SocketCb = Box<dyn FnMut(i32, i32, i32)>;
pub type OpenCb = Box<dyn FnMut(&str, i32, i32)>;
pub type OAuthBearerTokenRefreshCb = Box<dyn FnMut(&str) -> std::result::Result<(), Error>>;
#[derive(Default)]
pub(crate) struct Callbacks {
    pub active: std::rc::Rc<std::cell::Cell<bool>>,
    pub delivery: Option<DeliveryReportCb>,
    pub event: Option<EventCb>,
    pub consume: Option<ConsumeCb>,
    pub rebalance: Option<RebalanceCb>,
    pub commit: Option<OffsetCommitCb>,
    pub partitioner: Option<PartitionerCb>,
    pub key_partitioner: Option<PartitionerKeyPointerCb>,
    pub socket: Option<SocketCb>,
    pub open: Option<OpenCb>,
    pub oauth: Option<OAuthBearerTokenRefreshCb>,
}

/// Restores a temporarily borrowed callback, including during user unwinding.
pub(crate) fn with_callback<T, R>(
    callbacks: &std::cell::RefCell<Callbacks>,
    slot: fn(&mut Callbacks) -> &mut Option<T>,
    invoke: impl FnOnce(&mut T) -> R,
) -> Option<R> {
    struct Restore<'a, T> {
        callbacks: &'a std::cell::RefCell<Callbacks>,
        slot: fn(&mut Callbacks) -> &mut Option<T>,
        callback: Option<T>,
    }
    impl<T> Drop for Restore<'_, T> {
        fn drop(&mut self) {
            let mut state = self.callbacks.borrow_mut();
            let slot = (self.slot)(&mut state);
            if slot.is_none() {
                *slot = self.callback.take();
            }
        }
    }
    let callback = slot(&mut callbacks.borrow_mut()).take();
    let mut restore = Restore {
        callbacks,
        slot,
        callback,
    };
    restore.callback.as_mut().map(invoke)
}

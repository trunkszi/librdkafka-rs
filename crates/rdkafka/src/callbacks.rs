//! Calling-thread callbacks drained after native operations release their borrow.
use crate::{Error, Message, TopicPartition};
#[derive(Clone, Debug)]
pub struct DeliveryReport {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub token: u64,
    pub error: Option<crate::ErrorCode>,
}
#[derive(Clone, Debug)]
pub enum Event {
    Error {
        code: i32,
        message: String,
    },
    Stats(String),
    Log(String),
    Throttle(String),
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

//! Administrative resource identities and broker responses.
pub use ffi::{AdminConfig, AdminPartition, AdminResource};
use rdkafka_sys::admin::ffi;

/// Kafka resource identity shared by configuration and ACL operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ResourceType {
    Any = 1,
    Topic = 2,
    Group = 3,
    Broker = 4,
    TransactionalId = 5,
}

/// Whether an ACL name is literal, prefixed, or a filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ResourcePattern {
    Any = 1,
    Match = 2,
    Literal = 3,
    Prefixed = 4,
}

/// Kafka authorization operation; `Any` is valid only for filters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum AclOperation {
    Any = 1,
    All = 2,
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum AclPermission {
    Any = 1,
    Deny = 2,
    Allow = 3,
}

/// The change to apply in an incremental configuration update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ConfigOperation {
    Set = 0,
    Delete = 1,
    Append = 2,
    Subtract = 3,
}

#[derive(Clone, Debug)]
pub struct NewTopic {
    pub name: String,
    pub partitions: i32,
    pub replication: i32,
    pub configs: Vec<AdminConfig>,
}

impl NewTopic {
    pub fn new(name: impl Into<String>, partitions: i32, replication: i32) -> Self {
        Self {
            name: name.into(),
            partitions,
            replication,
            configs: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NewPartitions {
    pub topic: String,
    /// Total desired partition count, including the existing partitions.
    pub total_count: i32,
}

#[derive(Clone, Debug)]
pub struct ConfigResource {
    pub resource_type: ResourceType,
    pub name: String,
    pub entries: Vec<AdminConfig>,
}

impl ConfigResource {
    pub fn new(resource_type: ResourceType, name: impl Into<String>) -> Self {
        Self {
            resource_type,
            name: name.into(),
            entries: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AclBinding {
    pub resource_type: ResourceType,
    /// `None` matches any resource name in describe/delete filters.
    pub name: Option<String>,
    pub pattern: ResourcePattern,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: AclOperation,
    pub permission: AclPermission,
}

/// Per-resource outcomes retain all broker error codes, including partial failures.
#[derive(Clone, Debug)]
pub struct AdminResult {
    pub resources: Vec<AdminResource>,
}
impl AdminResult {
    pub fn has_errors(&self) -> bool {
        self.resources
            .iter()
            .any(|r| r.code != 0 || r.partitions.iter().any(|p| p.error != 0))
    }
}

/// Builds a configuration update, preserving deletion as `None` rather than empty text.
pub fn config_entry(
    name: impl Into<String>,
    value: Option<&str>,
    operation: ConfigOperation,
) -> AdminConfig {
    AdminConfig {
        name: name.into(),
        value: value.unwrap_or_default().into(),
        has_value: value.is_some(),
        operation: operation as i32,
        source: 0,
        read_only: false,
        sensitive: false,
    }
}

pub(super) fn request(name: String) -> ffi::AdminRequest {
    ffi::AdminRequest {
        name,
        resource_type: 0,
        count: 0,
        replication: 0,
        pattern: 0,
        principal: String::new(),
        host: String::new(),
        operation: 0,
        permission: 0,
        configs: Vec::new(),
        partitions: Vec::new(),
    }
}

pub(super) fn partition(p: &crate::TopicPartition) -> AdminPartition {
    AdminPartition {
        topic: p.topic.clone(),
        partition: p.partition,
        offset: p.offset,
        timestamp: -1,
        error: p.error,
        leader: -1,
        replicas: Vec::new(),
        isr: Vec::new(),
    }
}

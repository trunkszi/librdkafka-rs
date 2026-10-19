//! Synchronous administration using a dedicated native completion queue per request.
mod types;
use std::rc::Rc;

use rdkafka_sys::admin::ffi;
pub use types::*;
use types::{partition, request};

use crate::{
    Conf, Error, Result, TopicPartition,
    core::{Core, text},
};

/// Thread-affine administrative handle sharing the same native configuration contract.
pub struct AdminClient {
    core: Rc<Core>,
}
impl AdminClient {
    /// # Errors
    /// Returns invalid configuration or native client initialization errors.
    pub fn new(conf: &Conf) -> Result<Self> {
        Ok(Self {
            core: Core::new(conf, 0)?,
        })
    }
    /// Drives queued native events and calling-thread observers.
    /// # Errors
    /// Returns callback, queue overflow, or reentrant drive errors.
    pub fn poll(&self, timeout_ms: i32) -> Result<usize> {
        self.core.poll(timeout_ms)
    }
    fn execute(
        &self,
        op: i32,
        requests: &[ffi::AdminRequest],
        timeout_ms: i32,
    ) -> Result<AdminResult> {
        if timeout_ms < 0 {
            return Err(Error::Config(
                "administrative timeout must be nonnegative".into(),
            ));
        }
        if !matches!(op, 7 | 13) && requests.is_empty() {
            return Err(Error::Config(
                "administrative request must contain at least one resource".into(),
            ));
        }
        for r in requests {
            for s in [&r.name, &r.principal, &r.host] {
                text(s)?;
            }
            for c in &r.configs {
                text(&c.name)?;
                text(&c.value)?;
            }
            for p in &r.partitions {
                text(&p.topic)?;
            }
        }
        let _guard = self.core.begin_drive()?;
        let outcome = {
            let native = self.core.native.borrow();
            let native = native
                .as_ref()
                .ok_or_else(|| Error::Config("administrative client unavailable".into()))?;
            ffi::admin_request(native, op, requests, timeout_ms)
        };
        let callbacks = self.core.drain(0);
        if let Err(Error::Tls(message)) = &callbacks {
            return Err(Error::Tls(message.clone()));
        }
        let result = outcome?;
        if result.code != 0 {
            return Err(Error::native(result.code, result.message));
        }
        callbacks?;
        Ok(AdminResult {
            resources: result.resources,
        })
    }
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-topic errors.
    pub fn create_topics(&self, topics: &[NewTopic], timeout_ms: i32) -> Result<AdminResult> {
        let requests = topics
            .iter()
            .map(|t| {
                let mut r = request(t.name.clone());
                r.count = t.partitions;
                r.replication = t.replication;
                r.configs = t.configs.clone();
                r
            })
            .collect::<Vec<_>>();
        self.execute(0, &requests, timeout_ms)
    }
    fn names(&self, op: i32, names: &[&str], timeout_ms: i32) -> Result<AdminResult> {
        self.execute(
            op,
            &names
                .iter()
                .map(|s| request((*s).into()))
                .collect::<Vec<_>>(),
            timeout_ms,
        )
    }
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-topic errors.
    pub fn delete_topics(&self, topics: &[&str], timeout_ms: i32) -> Result<AdminResult> {
        self.names(1, topics, timeout_ms)
    }
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-topic errors.
    pub fn create_partitions(
        &self,
        partitions: &[NewPartitions],
        timeout_ms: i32,
    ) -> Result<AdminResult> {
        let requests = partitions
            .iter()
            .map(|p| {
                let mut r = request(p.topic.clone());
                r.count = p.total_count;
                r
            })
            .collect::<Vec<_>>();
        self.execute(2, &requests, timeout_ms)
    }
    fn configs(
        &self,
        op: i32,
        resources: &[ConfigResource],
        timeout_ms: i32,
    ) -> Result<AdminResult> {
        let requests = resources
            .iter()
            .map(|c| {
                let mut r = request(c.name.clone());
                r.resource_type = c.resource_type as i32;
                r.configs = c.entries.clone();
                r
            })
            .collect::<Vec<_>>();
        self.execute(op, &requests, timeout_ms)
    }
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-resource errors.
    pub fn describe_configs(
        &self,
        resources: &[ConfigResource],
        timeout_ms: i32,
    ) -> Result<AdminResult> {
        self.configs(3, resources, timeout_ms)
    }
    /// Replaces resource configuration; use incremental updates to preserve unspecified values.
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-resource errors.
    pub fn alter_configs(
        &self,
        resources: &[ConfigResource],
        timeout_ms: i32,
    ) -> Result<AdminResult> {
        self.configs(4, resources, timeout_ms)
    }
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-resource errors.
    pub fn incremental_alter_configs(
        &self,
        resources: &[ConfigResource],
        timeout_ms: i32,
    ) -> Result<AdminResult> {
        self.configs(5, resources, timeout_ms)
    }
    fn offsets(
        &self,
        op: i32,
        group: &str,
        offsets: &[TopicPartition],
        timeout_ms: i32,
    ) -> Result<AdminResult> {
        if offsets.is_empty() {
            return Err(Error::Config("offset request requires partitions".into()));
        }
        let mut r = request(group.into());
        r.partitions = offsets.iter().map(partition).collect();
        self.execute(op, &[r], timeout_ms)
    }
    /// Deletes records strictly before each specified offset.
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-partition errors.
    pub fn delete_records(
        &self,
        before_offsets: &[TopicPartition],
        timeout_ms: i32,
    ) -> Result<AdminResult> {
        self.offsets(6, "", before_offsets, timeout_ms)
    }
    /// # Errors
    /// Returns timeout or request-wide broker errors; partial broker errors remain in resources.
    pub fn list_consumer_groups(&self, timeout_ms: i32) -> Result<AdminResult> {
        self.execute(7, &[], timeout_ms)
    }
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-group errors.
    pub fn describe_consumer_groups(
        &self,
        groups: &[&str],
        timeout_ms: i32,
    ) -> Result<AdminResult> {
        self.names(8, groups, timeout_ms)
    }
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-group errors.
    pub fn delete_groups(&self, groups: &[&str], timeout_ms: i32) -> Result<AdminResult> {
        self.names(9, groups, timeout_ms)
    }
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect group and partition
    /// errors.
    pub fn delete_consumer_group_offsets(
        &self,
        group: &str,
        offsets: &[TopicPartition],
        timeout_ms: i32,
    ) -> Result<AdminResult> {
        self.offsets(10, group, offsets, timeout_ms)
    }
    /// Offsets accept timestamps or native special offsets (`-2` earliest, `-1` latest).
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-partition errors.
    pub fn list_offsets(&self, offsets: &[TopicPartition], timeout_ms: i32) -> Result<AdminResult> {
        self.offsets(11, "", offsets, timeout_ms)
    }
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-topic errors.
    pub fn describe_topics(&self, topics: &[&str], timeout_ms: i32) -> Result<AdminResult> {
        self.names(12, topics, timeout_ms)
    }
    /// Cluster identity is the first resource, followed by broker node resources.
    /// # Errors
    /// Returns timeout or request-wide broker errors.
    pub fn describe_cluster(&self, timeout_ms: i32) -> Result<AdminResult> {
        self.execute(13, &[], timeout_ms)
    }
    fn acls(&self, op: i32, acls: &[AclBinding], timeout_ms: i32) -> Result<AdminResult> {
        for acl in acls {
            if [&acl.name, &acl.principal, &acl.host]
                .iter()
                .any(|v| v.as_deref() == Some(""))
            {
                return Err(Error::Config(
                    "empty ACL values are invalid; use None for explicit filter wildcards".into(),
                ));
            }
        }
        let requests = acls
            .iter()
            .map(|a| {
                let mut r = request(a.name.clone().unwrap_or_default());
                r.resource_type = a.resource_type as i32;
                r.principal = a.principal.clone().unwrap_or_default();
                r.host = a.host.clone().unwrap_or_default();
                r.pattern = a.pattern as i32;
                r.operation = a.operation as i32;
                r.permission = a.permission as i32;
                r
            })
            .collect::<Vec<_>>();
        self.execute(op, &requests, timeout_ms)
    }
    /// # Errors
    /// Returns invalid input, timeout, or request-wide broker errors; inspect per-binding errors.
    pub fn create_acls(&self, acls: &[AclBinding], timeout_ms: i32) -> Result<AdminResult> {
        self.acls(14, acls, timeout_ms)
    }
    /// # Errors
    /// Returns invalid filter, timeout, or request-wide broker errors.
    pub fn describe_acls(&self, filter: &AclBinding, timeout_ms: i32) -> Result<AdminResult> {
        self.acls(15, std::slice::from_ref(filter), timeout_ms)
    }
    /// Responses retain each filter outcome before its matching deleted bindings.
    /// # Errors
    /// Returns invalid filters, timeout, or request-wide broker errors; inspect per-filter errors.
    pub fn delete_acls(&self, filters: &[AclBinding], timeout_ms: i32) -> Result<AdminResult> {
        self.acls(16, filters, timeout_ms)
    }
}

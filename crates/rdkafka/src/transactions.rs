//! Transactional production and cooperative consumer assignment.
use std::{marker::PhantomData, rc::Rc};

use rdkafka_sys::transactions::ffi;

use crate::{Consumer, Producer, Result, TopicPartition, core};

/// Owned consumer generation and member identity for transactional offsets.
///
/// The metadata remains valid after the consumer is dropped, but a transaction
/// must use the generation corresponding to the records being processed.
pub struct ConsumerGroupMetadata {
    native: cxx::UniquePtr<ffi::GroupMetadata>,
    _local: PhantomData<Rc<()>>,
}

/// Native purge scopes; purged deliveries still require caller-thread polling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PurgeFlags(i32);
impl PurgeFlags {
    pub const QUEUE: Self = Self(1);
    pub const IN_FLIGHT: Self = Self(2);
    pub const NON_BLOCKING: Self = Self(4);
    pub const ALL: Self = Self(3);
}
impl std::ops::BitOr for PurgeFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}
impl Producer {
    /// Initializes the transaction identity and fences prior producer instances.
    /// # Errors
    /// Returns timeout, fatal fencing, or native transaction errors. Inspect the
    /// returned error's recovery flags before retrying.
    pub fn init_transactions(&self, timeout_ms: i32) -> Result<()> {
        self.transaction(0, timeout_ms)
    }
    /// Opens a transaction after successful initialization.
    /// # Errors
    /// Returns native transaction state or fatal errors.
    pub fn begin_transaction(&self) -> Result<()> {
        self.transaction(1, 0)
    }
    /// Adds the next offsets to consume to the current transaction.
    ///
    /// Disable automatic consumer commits when using transactional offsets.
    /// # Errors
    /// Returns invalid topic strings, stale generation, or transaction errors.
    pub fn send_offsets_to_transaction(
        &self,
        offsets: &[TopicPartition],
        metadata: &ConsumerGroupMetadata,
        timeout_ms: i32,
    ) -> Result<()> {
        for p in offsets {
            core::text(&p.topic)?;
        }
        let _guard = self.core.begin_drive()?;
        let result = ffi::transaction_offsets(
            self.core.native.borrow_mut().pin_mut(),
            &core::partitions(offsets),
            &metadata.native,
            timeout_ms,
        )
        .map_err(crate::Error::from);
        self.core.complete(result)
    }
    /// Flushes deliveries and commits all transactional records and offsets.
    /// # Errors
    /// Returns native errors with retry, fatal, and abort-required flags, or a
    /// local callback failure after the native operation completes.
    pub fn commit_transaction(&self, timeout_ms: i32) -> Result<()> {
        self.transaction(2, timeout_ms)
    }
    /// Purges pending transactional records and aborts the current transaction.
    /// # Errors
    /// Returns native errors with operation-specific recovery flags or callback
    /// failures after the native operation completes.
    pub fn abort_transaction(&self, timeout_ms: i32) -> Result<()> {
        self.transaction(3, timeout_ms)
    }
    fn transaction(&self, operation: i32, timeout_ms: i32) -> Result<()> {
        let _guard = self.core.begin_drive()?;
        let result = ffi::transaction(
            self.core.native.borrow_mut().pin_mut(),
            operation,
            timeout_ms,
        )
        .map_err(crate::Error::from);
        self.core.complete(result)
    }
    /// Purges records from native queues or in-flight broker requests.
    ///
    /// Purging in-flight records can lose acknowledgement information, so a
    /// retry may duplicate previously delivered records.
    /// # Errors
    /// Returns native purge argument or state errors.
    pub fn purge(&self, flags: PurgeFlags) -> Result<()> {
        core::status(ffi::transaction_purge(
            self.core.native.borrow_mut().pin_mut(),
            flags.0,
        ))
    }
}
impl Consumer {
    /// Captures owned consumer-group metadata for transactional offset commits.
    /// # Errors
    /// Returns an error when native group metadata is unavailable.
    pub fn consumer_group_metadata(&self) -> Result<ConsumerGroupMetadata> {
        Ok(ConsumerGroupMetadata {
            native: ffi::group_metadata(&self.core.native.borrow())?,
            _local: PhantomData,
        })
    }
    /// Adds partitions without replacing the remaining cooperative assignment.
    ///
    /// A registered rebalance callback must apply its assignment explicitly.
    /// Repeating an already-applied delta is idempotent.
    /// # Errors
    /// Returns invalid topic strings or native incremental assignment errors.
    pub fn incremental_assign(&self, partitions: &[TopicPartition]) -> Result<()> {
        self.incremental(partitions, true)
    }
    /// Removes only the specified cooperative partitions.
    ///
    /// Already-removed partitions are ignored. A registered rebalance callback
    /// must apply its revocation explicitly.
    /// # Errors
    /// Returns invalid topic strings or native incremental assignment errors.
    pub fn incremental_unassign(&self, partitions: &[TopicPartition]) -> Result<()> {
        self.incremental(partitions, false)
    }
    fn incremental(&self, partitions: &[TopicPartition], assign: bool) -> Result<()> {
        for p in partitions {
            core::text(&p.topic)?;
        }
        ffi::consumer_incremental(
            self.core.native.borrow_mut().pin_mut(),
            &core::partitions(partitions),
            assign,
            self.core.rebalance_snapshot.borrow().is_some(),
        )?;
        Ok(())
    }
    /// Reports whether the native consumer lost ownership of its assignment.
    pub fn assignment_lost(&self) -> bool {
        self.core.rebalance_snapshot.borrow().as_ref().map_or_else(
            || ffi::consumer_assignment_lost(&self.core.native.borrow()),
            |(lost, _)| *lost,
        )
    }
    /// Returns the negotiated `COOPERATIVE`, `EAGER`, or `NONE` protocol.
    pub fn rebalance_protocol(&self) -> String {
        self.core.rebalance_snapshot.borrow().as_ref().map_or_else(
            || ffi::consumer_rebalance_protocol(&self.core.native.borrow()),
            |(_, protocol)| protocol.clone(),
        )
    }
}

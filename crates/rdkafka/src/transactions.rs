//! Transactional production and owned consumer-group metadata.
use std::{marker::PhantomData, rc::Rc};

use rdkafka_sys::ffi;

use crate::{Consumer, Producer, Result, TopicPartition, core};

#[repr(u8)]
#[derive(Clone, Copy)]
enum TransactionOperation {
    Initialize = 0,
    Begin = 1,
    Commit = 2,
    Abort = 3,
}
/// Owned consumer generation and member identity for transactional offsets.
///
/// The metadata remains valid after the consumer is dropped, but a transaction
/// must use the generation corresponding to the records being processed.
pub struct ConsumerGroupMetadata {
    native: cxx::UniquePtr<ffi::GroupMetadata>,
    _local: PhantomData<Rc<()>>,
}

impl Producer {
    /// Initializes the transaction identity and fences prior producer instances.
    /// # Errors
    /// Returns timeout, fatal fencing, or native transaction errors. Inspect the
    /// returned error's recovery flags before retrying.
    pub fn init_transactions(&self, timeout_ms: i32) -> Result<()> {
        self.transaction(TransactionOperation::Initialize, timeout_ms)
    }
    /// Opens a transaction after successful initialization.
    /// # Errors
    /// Returns native transaction state or fatal errors.
    pub fn begin_transaction(&self) -> Result<()> {
        self.transaction(TransactionOperation::Begin, 0)
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
        self.transaction(TransactionOperation::Commit, timeout_ms)
    }
    /// Purges pending transactional records and aborts the current transaction.
    /// # Errors
    /// Returns native errors with operation-specific recovery flags or callback
    /// failures after the native operation completes.
    pub fn abort_transaction(&self, timeout_ms: i32) -> Result<()> {
        self.transaction(TransactionOperation::Abort, timeout_ms)
    }
    fn transaction(&self, operation: TransactionOperation, timeout_ms: i32) -> Result<()> {
        let _guard = self.core.begin_drive()?;
        let result = ffi::transaction(
            self.core.native.borrow_mut().pin_mut(),
            operation as i32,
            timeout_ms,
        )
        .map_err(crate::Error::from);
        self.core.complete(result)
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
}

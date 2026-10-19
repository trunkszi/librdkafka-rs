//! Native transaction errors retain operation-specific recovery flags.
#[cxx::bridge(namespace = "kafka_bridge")]
pub mod ffi {
    unsafe extern "C++" {
        include!("rdkafka-sys/src/transactions.h");
        type Client = crate::ffi::Client;
        type NativePartition = crate::ffi::NativePartition;
        type GroupMetadata;
        /// # Errors
        /// Returns native transaction metadata acquisition errors.
        fn group_metadata(client: &Client) -> Result<UniquePtr<GroupMetadata>>;
        /// # Errors
        /// Returns native errors with fatal, retriable, and abort-required flags.
        fn transaction(client: Pin<&mut Client>, operation: i32, timeout_ms: i32) -> Result<()>;
        /// # Errors
        /// Returns native offset submission and transaction errors.
        fn transaction_offsets(
            client: Pin<&mut Client>,
            offsets: &[NativePartition],
            metadata: &GroupMetadata,
            timeout_ms: i32,
        ) -> Result<()>;
        fn transaction_purge(client: Pin<&mut Client>, flags: i32) -> i32;
        /// # Errors
        /// Returns native incremental assignment errors.
        fn consumer_incremental(
            client: Pin<&mut Client>,
            partitions: &[NativePartition],
            assign: bool,
            complete_rebalance: bool,
        ) -> Result<()>;
        fn consumer_assignment_lost(client: &Client) -> bool;
        fn consumer_rebalance_protocol(client: &Client) -> String;
    }
}

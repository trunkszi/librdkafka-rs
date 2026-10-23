//! Queue-polled native administrative requests and resource-level results.
#[cxx::bridge(namespace = "kafka_bridge")]
pub mod ffi {
    #[derive(Clone, Debug)]
    struct AdminConfig {
        name: String,
        value: String,
        has_value: bool,
        operation: i32,
        source: i32,
        read_only: bool,
        sensitive: bool,
    }
    #[derive(Clone, Debug)]
    struct AdminPartition {
        topic: String,
        partition: i32,
        offset: i64,
        timestamp: i64,
        error: i32,
        leader: i32,
        replicas: Vec<i32>,
        isr: Vec<i32>,
    }
    #[derive(Clone, Debug)]
    struct AdminRequest {
        name: String,
        resource_type: i32,
        count: i32,
        replication: i32,
        pattern: i32,
        principal: String,
        host: String,
        operation: i32,
        permission: i32,
        configs: Vec<AdminConfig>,
        partitions: Vec<AdminPartition>,
    }
    #[derive(Clone, Debug)]
    struct AdminResource {
        name: String,
        code: i32,
        message: String,
        resource_type: i32,
        state: i32,
        member_count: u64,
        simple: bool,
        internal: bool,
        assignor: String,
        configs: Vec<AdminConfig>,
        partitions: Vec<AdminPartition>,
        principal: String,
        host: String,
        pattern: i32,
        operation: i32,
        permission: i32,
        broker_id: i32,
        port: i32,
    }
    #[derive(Clone, Debug)]
    struct AdminResponse {
        code: i32,
        message: String,
        resources: Vec<AdminResource>,
    }
    unsafe extern "C++" {
        include!("librdkafka-rs-sys/src/admin.h");
        type Client = crate::ffi::Client;
        /// # Errors
        /// Returns native input-validation and allocation failures.
        fn admin_request(
            client: &Client,
            operation: i32,
            requests: &[AdminRequest],
            timeout: i32,
        ) -> Result<AdminResponse>;
    }
}

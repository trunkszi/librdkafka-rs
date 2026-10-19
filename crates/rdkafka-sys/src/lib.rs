//! Native synchronous Kafka bridge. Native handles own their broker resources.
#[cxx::bridge(namespace = "kafka_bridge")]
pub mod ffi {
    #[derive(Clone, Debug)]
    struct NativeProperty {
        name: String,
        value: String,
    }
    #[derive(Clone, Debug)]
    struct NativeHeader {
        name: String,
        value: Vec<u8>,
        has_value: bool,
    }
    #[derive(Clone, Debug)]
    struct NativePartition {
        topic: String,
        partition: i32,
        offset: i64,
        error: i32,
        leader_epoch: i32,
        metadata: Vec<u8>,
    }
    #[derive(Clone, Debug)]
    struct NativeEvent {
        kind: i32,
        code: i32,
        message: String,
        topic: String,
        partition: i32,
        offset: i64,
        token: u64,
        partitions: Vec<NativePartition>,
        status: i32,
        latency_us: i64,
        broker_name: String,
        broker_id: i32,
        throttle_ms: i32,
        log_level: i32,
        log_facility: String,
        fatal: bool,
        timestamp: i64,
        key: Vec<u8>,
        has_key: bool,
        payload: Vec<u8>,
        has_payload: bool,
        headers: Vec<NativeHeader>,
        assignment_lost: bool,
        rebalance_protocol: String,
    }
    #[derive(Clone, Debug)]
    struct NativePartitionMetadata {
        id: i32,
        error: i32,
        leader: i32,
        replicas: Vec<i32>,
        isr: Vec<i32>,
    }
    #[derive(Clone, Debug)]
    struct NativeTopicMetadata {
        name: String,
        error: i32,
        partitions: i32,
        partition_metadata: Vec<NativePartitionMetadata>,
    }
    #[derive(Clone, Debug)]
    struct NativeMetadata {
        broker_ids: Vec<i32>,
        broker_hosts: Vec<String>,
        broker_ports: Vec<i32>,
        topics: Vec<NativeTopicMetadata>,
    }
    extern "Rust" {
        type NativeHooks;
        fn hooks_empty() -> Box<NativeHooks>;
        fn hooks_clone(hooks: &NativeHooks) -> Result<Box<NativeHooks>>;
        fn hooks_socket(hooks: &NativeHooks, domain: i32, kind: i32, protocol: i32) -> i32;
        fn hooks_open(hooks: &NativeHooks, path: &str, flags: i32, mode: i32) -> i32;
        fn hooks_tls_enabled(hooks: &NativeHooks) -> bool;
        fn hooks_tls_connect(
            hooks: &NativeHooks,
            fd: i32,
            address: &str,
            port: u16,
            name: &str,
        ) -> i32;
        fn hooks_tls_close(hooks: &NativeHooks, fd: i32) -> i32;
        fn hooks_tls_errors(hooks: &NativeHooks) -> Vec<String>;
    }
    unsafe extern "C++" {
        include!("rdkafka-sys/src/shim.h");
        include!("rdkafka-sys/src/tls_callbacks.h");
        fn tls_callback_registry_teardown_test(hooks: Box<NativeHooks>) -> bool;
        fn tls_callback_peer_name_test(hooks: Box<NativeHooks>, name: &str) -> i32;
        type Conf;
        type Client;
        type Queue;
        type Topic;
        type Message;
        fn native_callback_contract_test() -> bool;
        fn queue_retains_forwarded_owner(queue: &Queue) -> bool;
        fn topic_murmur2_partition(topic: &Topic, key: &[u8], count: i32) -> i32;
        fn conf_new(topic: bool) -> UniquePtr<Conf>;
        /// # Errors
        /// Returns the native diagnostic when configuration or the operation fails.
        fn conf_set(conf: Pin<&mut Conf>, key: &str, value: &str) -> Result<()>;
        fn conf_set_hooks(conf: Pin<&mut Conf>, hooks: Box<NativeHooks>);
        fn conf_set_observers(conf: Pin<&mut Conf>, consume: bool, rebalance: bool);
        fn mock_bootstraps(client: &Client) -> String;
        /// # Errors
        /// Returns the native diagnostic when configuration or the operation fails.
        fn oauth_set_token(
            client: Pin<&mut Client>,
            token: &str,
            expiry_ms: i64,
            principal: &str,
            extensions: &[String],
        ) -> Result<()>;
        fn oauth_set_failure(client: Pin<&mut Client>, reason: &str) -> i32;
        /// # Errors
        /// Returns the native diagnostic when configuration or the operation fails.
        fn conf_get(conf: &Conf, key: &str) -> Result<String>;
        fn conf_dump(conf: &Conf) -> Vec<NativeProperty>;
        /// # Errors
        /// Returns the native diagnostic when configuration or the operation fails.
        fn client_new(conf: &Conf, kind: i32) -> Result<UniquePtr<Client>>;
        /// # Errors
        /// Returns the native diagnostic when client configuration is invalid.
        fn client_new_with_partitioner(
            conf: &Conf,
            kind: i32,
            has_partitioner: bool,
        ) -> Result<UniquePtr<Client>>;
        fn client_poll(client: Pin<&mut Client>, timeout: i32) -> Vec<NativeEvent>;
        fn client_flush(client: Pin<&mut Client>, timeout: i32) -> i32;
        fn client_outq(client: &Client) -> i32;
        fn client_name(client: &Client) -> String;
        fn client_id(client: Pin<&mut Client>, kind: i32, timeout: i32) -> String;
        fn client_controller(client: Pin<&mut Client>, timeout: i32) -> i32;
        fn client_fatal(client: Pin<&mut Client>) -> NativeEvent;
        #[allow(
            clippy::too_many_arguments,
            reason = "Mirrors independent native enqueue fields and nullability"
        )]
        fn produce(
            client: Pin<&mut Client>,
            topic: &str,
            partition: i32,
            key: &[u8],
            has_key: bool,
            payload: &[u8],
            has_payload: bool,
            headers: &[NativeHeader],
            timestamp: i64,
            token: u64,
        ) -> i32;
        #[allow(
            clippy::too_many_arguments,
            reason = "Mirrors native record fields and nullability"
        )]
        fn produce_topic(
            client: Pin<&mut Client>,
            topic: &Topic,
            partition: i32,
            key: &[u8],
            has_key: bool,
            payload: &[u8],
            has_payload: bool,
            headers: &[NativeHeader],
            timestamp: i64,
            token: u64,
        ) -> i32;
        /// # Errors
        /// Returns the native diagnostic when configuration or the operation fails.
        fn client_metadata(
            client: Pin<&mut Client>,
            topic: &str,
            timeout: i32,
        ) -> Result<NativeMetadata>;
        /// # Errors
        /// Returns the native diagnostic when configuration or the operation fails.
        fn client_watermarks(
            client: Pin<&mut Client>,
            topic: &str,
            partition: i32,
            timeout: i32,
            cached: bool,
        ) -> Result<Vec<i64>>;
        /// # Errors
        /// Returns the native diagnostic when configuration or the operation fails.
        fn client_partitions(
            client: Pin<&mut Client>,
            operation: i32,
            partitions: &[NativePartition],
            timeout: i32,
        ) -> Result<Vec<NativePartition>>;
        fn subscribe(client: Pin<&mut Client>, topics: &[String]) -> i32;
        fn unsubscribe(client: Pin<&mut Client>) -> i32;
        /// # Errors
        /// Returns the native diagnostic when configuration or the operation fails.
        fn consumer_list(
            client: Pin<&mut Client>,
            assignment: bool,
        ) -> Result<Vec<NativePartition>>;
        fn consume(client: Pin<&mut Client>, timeout: i32) -> UniquePtr<Message>;
        /// # Errors
        /// Returns native consumer close initialization errors.
        fn consumer_close_begin(client: Pin<&mut Client>) -> Result<()>;
        fn consumer_close_poll(client: Pin<&mut Client>, timeout: i32) -> bool;
        /// # Errors
        /// Returns the native diagnostic when configuration or the operation fails.
        fn topic_new(client: Pin<&mut Client>, name: &str, conf: &Conf)
        -> Result<UniquePtr<Topic>>;
        fn topic_name(topic: &Topic) -> String;
        fn topic_available(topic: &Topic, partition: i32) -> bool;
        fn simple_start(
            client: Pin<&mut Client>,
            topic: &Topic,
            partition: i32,
            offset: i64,
        ) -> i32;
        fn simple_stop(client: Pin<&mut Client>, topic: &Topic, partition: i32) -> i32;
        fn simple_seek(
            client: Pin<&mut Client>,
            topic: &Topic,
            partition: i32,
            offset: i64,
            timeout: i32,
        ) -> i32;
        fn simple_consume(
            client: Pin<&mut Client>,
            topic: &Topic,
            partition: i32,
            timeout: i32,
        ) -> UniquePtr<Message>;
        /// # Errors
        /// Returns the native diagnostic when configuration or the operation fails.
        fn queue_new(
            client: Pin<&mut Client>,
            topic: &str,
            partition: i32,
        ) -> Result<UniquePtr<Queue>>;
        fn queue_forward(queue: Pin<&mut Queue>, destination: &Queue);
        fn queue_unforward(queue: Pin<&mut Queue>);
        fn queue_poll(queue: Pin<&mut Queue>, timeout: i32) -> i32;
        fn queue_consume(queue: Pin<&mut Queue>, timeout: i32) -> UniquePtr<Message>;
        fn queue_io_event(queue: Pin<&mut Queue>, fd: i32, payload: &[u8]);
        fn simple_start_queue(
            client: Pin<&mut Client>,
            topic: &Topic,
            partition: i32,
            offset: i64,
            queue: &Queue,
        ) -> i32;
        fn message_error(message: &Message) -> i32;
        fn message_topic(message: &Message) -> String;
        fn message_partition(message: &Message) -> i32;
        fn message_offset(message: &Message) -> i64;
        fn message_timestamp(message: &Message) -> i64;
        fn message_has_key(message: &Message) -> bool;
        fn message_key(message: &Message) -> &[u8];
        fn message_has_payload(message: &Message) -> bool;
        fn message_payload(message: &Message) -> &[u8];
        fn message_headers(message: &Message) -> Vec<NativeHeader>;
        fn error_name(code: i32) -> String;
        fn error_message(code: i32) -> String;
        fn error_flags(code: i32) -> u8;
    }
}

use std::sync::Arc;
/// Native broker-thread socket hook. Closure state must be thread safe.
pub type SocketHook = Arc<dyn Fn(i32, i32, i32) -> i32 + Send + Sync>;
/// Native broker-thread file hook. Closure state must be thread safe.
pub type OpenHook = Arc<dyn Fn(&str, i32, i32) -> i32 + Send + Sync>;
/// Thread-safe native hooks; local observers are delivered through poll events.
#[derive(Clone, Default)]
pub struct NativeHooks {
    pub socket: Option<SocketHook>,
    pub open: Option<OpenHook>,
    pub tls: Option<Arc<dyn tls::BrokerTlsHooks>>,
}
fn hooks_empty() -> Box<NativeHooks> {
    Box::default()
}
fn hooks_clone(hooks: &NativeHooks) -> Result<Box<NativeHooks>, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut copy = hooks.clone();
        copy.tls = hooks.tls.as_ref().map(|tls| tls.fork());
        Box::new(copy)
    }))
    .map_err(|_| "TLS hook fork panicked".to_owned())
}
fn hooks_socket(hooks: &NativeHooks, domain: i32, kind: i32, protocol: i32) -> i32 {
    hooks.socket.as_ref().map_or(-2, |hook| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            hook(domain, kind, protocol)
        }))
        .unwrap_or(-1)
    })
}
fn hooks_open(hooks: &NativeHooks, path: &str, flags: i32, mode: i32) -> i32 {
    hooks.open.as_ref().map_or(-2, |hook| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook(path, flags, mode)))
            .unwrap_or(-1)
    })
}

pub mod admin;
pub mod batch;
pub mod transactions;

mod sasl_crypto;
pub mod tls;

fn hooks_tls_enabled(hooks: &NativeHooks) -> bool {
    hooks.tls.is_some()
}
fn hooks_tls_connect(hooks: &NativeHooks, fd: i32, address: &str, port: u16, name: &str) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        hooks
            .tls
            .as_ref()
            .map_or(libc::EINVAL, |tls| tls.connect(fd, address, port, name))
    }))
    .unwrap_or(libc::EIO)
}
fn hooks_tls_close(hooks: &NativeHooks, fd: i32) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        hooks.tls.as_ref().map_or(-1, |tls| tls.close(fd))
    }))
    .unwrap_or(-1)
}
fn hooks_tls_errors(hooks: &NativeHooks) -> Vec<String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        hooks.tls.as_ref().map_or_else(Vec::new, |tls| tls.errors())
    }))
    .unwrap_or_else(|_| vec!["TLS error hook panicked".to_owned()])
}

#[cfg(test)]
mod tls_registry_tests {
    use super::*;
    struct Hooks;
    impl tls::BrokerTlsHooks for Hooks {
        fn fork(&self) -> Arc<dyn tls::BrokerTlsHooks> {
            Arc::new(Self)
        }
        fn connect(&self, _: i32, _: &str, _: u16, _: &str) -> i32 {
            libc::ECONNREFUSED
        }
        fn close(&self, _: i32) -> i32 {
            -1
        }
        fn errors(&self) -> Vec<String> {
            Vec::new()
        }
    }
    #[test]
    fn integration_lifecycle_registry_removes_dropped_callbacks_before_fd_reuse() {
        let hooks = NativeHooks {
            tls: Some(Arc::new(Hooks)),
            ..Default::default()
        };
        assert!(ffi::tls_callback_registry_teardown_test(Box::new(hooks)));
    }
}

#[cfg(test)]
mod hook_panic_tests {
    use super::*;

    struct PanickingHooks(bool);
    impl tls::BrokerTlsHooks for PanickingHooks {
        fn fork(&self) -> Arc<dyn tls::BrokerTlsHooks> {
            assert!(!self.0, "fork failed");
            Arc::new(Self(false))
        }
        fn connect(&self, _: i32, _: &str, _: u16, _: &str) -> i32 {
            libc::ECONNREFUSED
        }
        fn close(&self, _: i32) -> i32 {
            -1
        }
        fn errors(&self) -> Vec<String> {
            panic!("error callback failed")
        }
    }

    #[test]
    fn unit_scenario_tls_hook_panics_become_errors_at_bridge_boundary() {
        let hooks = NativeHooks {
            tls: Some(Arc::new(PanickingHooks(true))),
            ..NativeHooks::default()
        };
        assert!(hooks_clone(&hooks).is_err());
        assert_eq!(hooks_tls_errors(&hooks), ["TLS error hook panicked"]);
        let mut conf = ffi::conf_new(false);
        ffi::conf_set_hooks(conf.pin_mut(), Box::new(hooks));
        let result = ffi::client_new(&conf, 0);
        assert!(matches!(result, Err(error) if error.what().contains("TLS hook fork panicked")));
    }
}

#[cfg(test)]
mod peer_name_tests {
    use super::*;
    struct Peer(std::sync::Mutex<String>);
    impl tls::BrokerTlsHooks for Peer {
        fn fork(&self) -> Arc<dyn tls::BrokerTlsHooks> {
            Arc::new(Self(std::sync::Mutex::default()))
        }
        fn connect(&self, _: i32, _: &str, _: u16, name: &str) -> i32 {
            *self.0.lock().expect("peer lock") = name.to_owned();
            libc::ECONNREFUSED
        }
        fn close(&self, _: i32) -> i32 {
            -1
        }
        fn errors(&self) -> Vec<String> {
            Vec::new()
        }
    }
    #[test]
    fn integration_scenario_native_tls_preserves_full_ipv6_peer_name() {
        for name in ["2001:db8::1", "[2001:db8::1]:9092", "broker.example:9092"] {
            let peer = Arc::new(Peer(std::sync::Mutex::default()));
            let hooks = NativeHooks {
                tls: Some(peer.clone()),
                ..NativeHooks::default()
            };
            assert_eq!(
                ffi::tls_callback_peer_name_test(Box::new(hooks), name),
                libc::ECONNREFUSED
            );
            assert_eq!(*peer.0.lock().expect("peer lock"), name);
        }
    }
}

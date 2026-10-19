//! Integration scenarios for native configuration, broker discovery, and ownership.

use rdkafka_sys::ffi;

fn mock_producer() -> cxx::UniquePtr<ffi::Client> {
    let mut conf = ffi::conf_new(false);
    ffi::conf_set(conf.pin_mut(), "test.mock.num.brokers", "2").expect("mock brokers");
    ffi::conf_set(conf.pin_mut(), "client.id", "native-contract").expect("client identity");
    ffi::conf_set(conf.pin_mut(), "log_level", "0").expect("quiet mock client");
    ffi::client_new(&conf, 0).expect("native producer")
}

#[test]
fn integration_scenario_global_and_topic_configuration() {
    for (topic, name, canonical, value) in [
        (false, "client.id", "client.id", "native-property-roundtrip"),
        (true, "compression.type", "compression.codec", "zstd"),
    ] {
        let mut conf = ffi::conf_new(topic);
        ffi::conf_set(conf.pin_mut(), name, value).expect("valid native property");
        assert_eq!(ffi::conf_get(&conf, name).expect("read property"), value);
        assert!(
            ffi::conf_dump(&conf)
                .iter()
                .any(|entry| entry.name == canonical && entry.value == value)
        );
        let error = ffi::conf_set(conf.pin_mut(), "nonexistent.property", "value")
            .expect_err("unknown property must be rejected");
        assert!(error.what().contains("nonexistent.property"));
        assert!(ffi::conf_get(&conf, "nonexistent.property").is_err());
        assert_eq!(
            ffi::conf_get(&conf, name).expect("unchanged valid property"),
            value
        );
    }
}

#[test]
fn integration_scenario_mock_bootstraps_and_broker_metadata() {
    let mut producer = mock_producer();
    let bootstraps = ffi::mock_bootstraps(&producer);
    assert_eq!(bootstraps.split(',').count(), 2);
    assert!(
        bootstraps
            .split(',')
            .all(|address| address.starts_with("127.0.0.1:"))
    );
    let metadata =
        ffi::client_metadata(producer.pin_mut(), "", 5_000).expect("mock broker metadata");
    assert_eq!(metadata.broker_ids.len(), 2);
    assert_eq!(metadata.broker_hosts.len(), 2);
    assert_eq!(metadata.broker_ports.len(), 2);
    assert!(metadata.broker_ports.iter().all(|port| *port > 0));
    assert!(ffi::client_name(&producer).contains("native-contract"));
    assert_eq!(ffi::client_fatal(producer.pin_mut()).code, 0);
}

#[test]
fn integration_lifecycle_topic_and_queues_retain_native_owner() {
    let mut producer = mock_producer();
    let topic_conf = ffi::conf_new(true);
    let topic =
        ffi::topic_new(producer.pin_mut(), "retained-topic", &topic_conf).expect("native topic");
    let mut source = ffi::queue_new(producer.pin_mut(), "", -1).expect("source queue");
    let mut destination = ffi::queue_new(producer.pin_mut(), "", -1).expect("destination queue");
    drop(topic_conf);
    drop(producer);
    assert_eq!(ffi::topic_name(&topic), "retained-topic");
    ffi::queue_forward(source.pin_mut(), &destination);
    assert_eq!(ffi::queue_poll(destination.pin_mut(), 0), 0);
    ffi::queue_unforward(source.pin_mut());
    assert!(ffi::queue_consume(source.pin_mut(), 0).is_null());
    drop(topic);
    drop(source);
    assert_eq!(ffi::queue_poll(destination.pin_mut(), 0), 0);
}

#[test]
fn integration_scenario_native_error_lookup_preserves_unknown_identity() {
    assert_eq!(ffi::error_name(0), "NO_ERROR");
    assert!(ffi::error_name(-184).contains("QUEUE_FULL"));
    for code in [123_456_i32, -123_456_i32] {
        let name = ffi::error_name(code);
        let message = ffi::error_message(code);
        assert!(name.contains(&code.to_string()), "{code}: {name}");
        assert!(message.contains(&code.to_string()), "{code}: {message}");
        assert_eq!(ffi::error_flags(code), 0);
    }
}

#[test]
fn integration_scenario_critical_callbacks_are_lossless_and_fds_are_cloexec() {
    assert!(ffi::native_callback_contract_test());
}

#[test]
fn integration_lifecycle_unforward_releases_destination_owner() {
    let mut producer = mock_producer();
    let mut other = mock_producer();
    let mut source = ffi::queue_new(producer.pin_mut(), "", -1).expect("source");
    let destination = ffi::queue_new(other.pin_mut(), "", -1).expect("destination");
    ffi::queue_forward(source.pin_mut(), &destination);
    assert!(ffi::queue_retains_forwarded_owner(&source));
    drop(destination);
    drop(other);
    ffi::queue_unforward(source.pin_mut());
    assert!(!ffi::queue_retains_forwarded_owner(&source));
}

#[test]
fn integration_scenario_native_murmur2_and_large_flush_preserve_all_deliveries() {
    let mut conf = ffi::conf_new(false);
    ffi::conf_set(conf.pin_mut(), "test.mock.num.brokers", "1").expect("mock");
    ffi::conf_set(conf.pin_mut(), "partitioner", "murmur2_random").expect("partitioner");
    ffi::conf_set(conf.pin_mut(), "log_level", "0").expect("quiet");
    let mut producer = ffi::client_new(&conf, 0).expect("producer");
    let mut topic_conf = ffi::conf_new(true);
    ffi::conf_set(topic_conf.pin_mut(), "partitioner", "murmur2_random")
        .expect("topic partitioner");
    let topic = ffi::topic_new(producer.pin_mut(), "large-flush", &topic_conf).expect("topic");
    let metadata =
        ffi::client_metadata(producer.pin_mut(), "large-flush", 5000).expect("discover mock topic");
    let count = metadata
        .topics
        .iter()
        .find(|topic| topic.name == "large-flush")
        .expect("requested topic")
        .partitions;
    assert!(count > 0);
    let total = 5000u64;
    let mut expected = Vec::new();
    for token in 0..total {
        let key = token.to_le_bytes();
        expected.push(ffi::topic_murmur2_partition(&topic, &key, count));
        let code = if token % 2 == 0 {
            ffi::produce(
                producer.pin_mut(),
                "large-flush",
                -1,
                &key,
                true,
                b"value",
                true,
                &[],
                -1,
                token,
            )
        } else {
            ffi::produce_topic(
                producer.pin_mut(),
                &topic,
                -1,
                &key,
                true,
                b"value",
                true,
                &[],
                -1,
                token,
            )
        };
        assert_eq!(code, 0);
    }
    assert_eq!(ffi::client_flush(producer.pin_mut(), 10000), 0);
    let mut seen = vec![false; total as usize];
    for event in ffi::client_poll(producer.pin_mut(), 0) {
        if event.kind == 0 {
            assert_eq!(event.code, 0);
            let token = event.token as usize;
            assert!(!seen[token]);
            seen[token] = true;
            assert_eq!(event.partition, expected[token]);
        }
    }
    assert!(seen.iter().all(|seen| *seen));
}

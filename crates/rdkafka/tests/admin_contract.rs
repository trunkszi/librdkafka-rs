//! Native administrative validation and queue deadline scenarios.
use rdkafka::{
    AclBinding, AclOperation, AclPermission, AdminClient, Conf, ConfigResource, NewTopic,
    ResourcePattern, ResourceType, Result,
};
static_assertions::assert_not_impl_any!(AdminClient: Send, Sync);

#[test]
fn integration_scenario_admin_rejects_invalid_resources_and_deadlines() -> Result<()> {
    let mut config = Conf::new();
    config.set("test.mock.num.brokers", "1")?;
    let admin = AdminClient::new(&config)?;
    assert!(admin.create_topics(&[], 100).is_err());
    assert!(
        admin
            .create_topics(&[NewTopic::new("invalid\0name", 1, 1)], 100)
            .is_err()
    );
    assert!(
        admin
            .create_topics(&[NewTopic::new("negative-partitions", -5, 1)], 100)
            .is_err()
    );
    assert!(admin.describe_cluster(-1).is_err());
    assert!(
        admin
            .describe_configs(
                &[ConfigResource::new(ResourceType::Topic, "broken\0name")],
                100
            )
            .is_err()
    );
    assert!(admin.list_offsets(&[], 100).is_err());
    let wildcard = AclBinding {
        resource_type: ResourceType::Any,
        name: Some(String::new()),
        pattern: ResourcePattern::Any,
        principal: None,
        host: None,
        operation: AclOperation::Any,
        permission: AclPermission::Any,
    };
    assert!(admin.delete_acls(&[wildcard], 100).is_err());
    Ok(())
}

#[test]
fn integration_scenario_admin_timeout_is_not_empty_success() -> Result<()> {
    let mut config = Conf::new();
    config.set("bootstrap.servers", "127.0.0.1:1")?;
    let observed = std::rc::Rc::new(std::cell::Cell::new(0));
    let captured = std::rc::Rc::clone(&observed);
    let caller = std::thread::current().id();
    config.socket_cb(move |_, _, _| {
        assert_eq!(std::thread::current().id(), caller);
        captured.set(captured.get() + 1);
    });
    let admin = AdminClient::new(&config)?;
    let start = std::time::Instant::now();
    let error = admin
        .describe_cluster(20)
        .expect_err("unreachable broker cannot succeed");
    assert_eq!(error.code().map(|c| c.value()), Some(-185));
    assert!(start.elapsed() < std::time::Duration::from_secs(3));
    admin.poll(0)?;
    assert!(
        observed.get() > 0,
        "administrative requests must drain native observers"
    );
    Ok(())
}

fn successful(result: rdkafka::AdminResult) -> rdkafka::AdminResult {
    assert!(!result.has_errors(), "native resource failure: {result:?}");
    result
}

#[test]
#[ignore = "requires the live broker provisioned by test-redpanda.sh"]
fn integration_lifecycle_live_admin_resources() -> Result<()> {
    use rdkafka::{
        ConfigOperation, Consumer, NewPartitions, ProduceRecord, Producer, TopicPartition,
        config_entry,
    };
    let brokers =
        std::env::var("RDKAFKA_TEST_BROKERS").map_err(|e| rdkafka::Error::Config(e.to_string()))?;
    let mut config = Conf::new();
    config.set("bootstrap.servers", &brokers)?;
    let admin = AdminClient::new(&config)?;
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    );
    let topic = format!("admin-{suffix}");
    let group = format!("admin-group-{suffix}");
    successful(admin.create_topics(&[NewTopic::new(&topic, 1, 1)], 10000)?);
    let duplicate = admin.create_topics(&[NewTopic::new(&topic, 1, 1)], 10000)?;
    assert!(
        duplicate.has_errors(),
        "duplicate topic must retain per-resource failure"
    );
    assert_eq!(duplicate.resources[0].name, topic);
    successful(admin.create_partitions(
        &[NewPartitions {
            topic: topic.clone(),
            total_count: 2,
        }],
        10000,
    )?);
    let description = successful(admin.describe_topics(&[&topic], 10000)?);
    assert_eq!(description.resources[0].partitions.len(), 2);
    let cluster = successful(admin.describe_cluster(10000)?);
    assert!(
        cluster.resources.len() >= 2,
        "cluster identity and at least one broker expected"
    );
    assert!(!cluster.resources[0].name.is_empty());
    let mut resource = ConfigResource::new(ResourceType::Topic, &topic);
    let settings = successful(admin.describe_configs(&[resource.clone()], 10000)?);
    assert!(!settings.resources[0].configs.is_empty());
    resource.entries.push(config_entry(
        "retention.ms",
        Some("600000"),
        ConfigOperation::Set,
    ));
    successful(admin.alter_configs(&[resource.clone()], 10000)?);
    resource.entries[0] = config_entry("retention.ms", Some("700000"), ConfigOperation::Set);
    successful(admin.incremental_alter_configs(&[resource.clone()], 10000)?);
    let settings = successful(admin.describe_configs(&[resource], 10000)?);
    assert!(
        settings.resources[0]
            .configs
            .iter()
            .any(|c| c.name == "retention.ms" && c.has_value && c.value == "700000")
    );
    let producer = Producer::new(&config)?;
    producer.produce(ProduceRecord {
        topic: &topic,
        partition: 0,
        key: None,
        payload: Some(b"admin-lifecycle"),
        headers: None,
        timestamp: None,
        token: 0,
    })?;
    producer.flush(10000)?;
    let offsets = successful(admin.list_offsets(&[TopicPartition::new(&topic, 0, -1)], 10000)?);
    assert_eq!(offsets.resources[0].partitions[0].offset, 1);
    successful(admin.delete_records(&[TopicPartition::new(&topic, 0, 1)], 10000)?);
    let earliest = successful(admin.list_offsets(&[TopicPartition::new(&topic, 0, -2)], 10000)?);
    assert_eq!(earliest.resources[0].partitions[0].offset, 1);
    let mut consumer_conf = Conf::new();
    consumer_conf
        .set("bootstrap.servers", &brokers)?
        .set("group.id", &group)?
        .set("enable.auto.commit", "false")?;
    let consumer = Consumer::new(&consumer_conf)?;
    consumer.assign(&[TopicPartition::new(&topic, 0, 1)])?;
    consumer.commit_sync(&[
        TopicPartition::new(&topic, 0, 1),
        TopicPartition::new(&topic, 1, 0),
    ])?;
    consumer.close()?;
    let groups = successful(admin.list_consumer_groups(10000)?);
    assert!(groups.resources.iter().any(|r| r.name == group));
    let group_description = successful(admin.describe_consumer_groups(&[&group], 10000)?);
    assert_eq!(group_description.resources[0].name, group);
    successful(admin.delete_consumer_group_offsets(
        &group,
        &[TopicPartition::new(&topic, 0, 1)],
        10000,
    )?);
    let reader = Consumer::new(&consumer_conf)?;
    let committed = reader.committed(
        &[TopicPartition::new(&topic, 0, 0)],
        std::time::Duration::from_secs(10),
    )?;
    assert_eq!(
        committed[0].offset, -1001,
        "deleted offset must be unavailable"
    );
    reader.close()?;
    successful(admin.delete_groups(&[&group], 10000)?);
    let groups = successful(admin.list_consumer_groups(10000)?);
    assert!(groups.resources.iter().all(|r| r.name != group));
    let acl = AclBinding {
        resource_type: ResourceType::Topic,
        name: Some(topic.clone()),
        pattern: ResourcePattern::Literal,
        principal: Some("User:admin-contract".into()),
        host: Some("*".into()),
        operation: AclOperation::Read,
        permission: AclPermission::Allow,
    };
    successful(admin.create_acls(std::slice::from_ref(&acl), 10000)?);
    let acls = successful(admin.describe_acls(&acl, 10000)?);
    assert!(
        acls.resources
            .iter()
            .any(|r| r.principal == "User:admin-contract" && r.name == topic)
    );
    let deleted = successful(admin.delete_acls(&[acl], 10000)?);
    assert!(
        deleted
            .resources
            .iter()
            .any(|r| r.principal == "User:admin-contract")
    );
    successful(admin.delete_topics(&[&topic], 10000)?);
    let removed = admin.describe_topics(&[&topic], 10000)?;
    assert!(
        removed.has_errors(),
        "deleted topic must not resolve successfully"
    );
    Ok(())
}

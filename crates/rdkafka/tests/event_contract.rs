//! Native event evidence, queue admission, and lossless error identities.
use std::{cell::RefCell, rc::Rc, time::Duration};

use rdkafka::*;

#[test]
fn unit_scenario_unknown_codes_and_persistence_evidence() {
    for code in [i32::MIN, -1, 134, 100_000, i32::MAX] {
        assert_eq!(ErrorCode::from_raw(code).value(), code);
    }
    assert_eq!(MsgStatus::from_raw(0), MsgStatus::NotPersisted);
    assert_eq!(MsgStatus::from_raw(1), MsgStatus::PossiblyPersisted);
    assert_eq!(MsgStatus::from_raw(2), MsgStatus::Persisted);
    assert_eq!(MsgStatus::from_raw(17), MsgStatus::Unknown(17));
    assert_eq!(ErrorCode::NATIVE_CODES.len(), 190);
    assert_eq!(ErrorCode::INVALID_ARGUMENT, ErrorCode::INVALID_ARG);
    assert_eq!(
        ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
        ErrorCode::UNKNOWN_TOPIC_OR_PART
    );
    assert_eq!(ErrorCode::FENCED_STATE_EPOCH.value(), 124);
    assert_eq!(ErrorCode::SHARE_SESSION_LIMIT_REACHED.value(), 133);
    assert_eq!(ErrorCode::LOG_TRUNCATION.value(), -139);
    assert_eq!(
        ErrorCode::NOT_LEADER_OR_FOLLOWER,
        ErrorCode::NOT_LEADER_FOR_PARTITION
    );
}

#[test]
fn integration_scenario_native_queue_full_is_typed() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?
        .set("queue.buffering.max.messages", "1")?
        .set("linger.ms", "1000")?;
    let producer = Producer::new(&conf)?;
    let mut first = ProduceRecord::new("queue-type-contract", Some(b"first"));
    first.partition = 0;
    producer.produce(first)?;
    let error = producer
        .produce(ProduceRecord::new("queue-type-contract", Some(b"second")))
        .expect_err("native admission queue must remain full until delivery polling");
    assert!(matches!(error, Error::QueueFull));
    assert!(error.is_queue_full());
    assert!(error.is_retriable());
    assert!(!error.is_fatal());
    assert_eq!(error.code(), Some(ErrorCode::QUEUE_FULL));
    producer.flush(10000)
}

#[test]
fn integration_lifecycle_native_delivery_persistence_latency_and_logs() -> Result<()> {
    let reports = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?
        .set("debug", "broker")?;
    let observed = Rc::clone(&reports);
    conf.delivery_report_cb(move |report| observed.borrow_mut().push(report.clone()));
    let observed = Rc::clone(&events);
    conf.event_cb(move |event| observed.borrow_mut().push(event.clone()));
    let producer = Producer::new(&conf)?;
    let mut record = ProduceRecord::new("delivery-evidence-contract", Some(b"persisted"));
    record.partition = 0;
    record.token = 7;
    producer.produce(record)?;
    producer.flush(10000)?;
    let reports = reports.borrow();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].token, 7);
    assert_eq!(reports[0].error, None);
    assert_eq!(reports[0].status, MsgStatus::Persisted);
    assert!(
        reports[0]
            .latency
            .is_some_and(|latency| latency < Duration::from_secs(10))
    );
    assert!(events.borrow().iter().any(|event| matches!(event,
        Event::Log { level, fac, message } if (0..=7).contains(level) && !fac.is_empty() && !message.is_empty())));
    Ok(())
}

#[test]
fn integration_scenario_all_pinned_error_names_match_native() {
    let codes = [
        (ErrorCode::BAD_MSG, "_BAD_MSG"),
        (ErrorCode::BAD_COMPRESSION, "_BAD_COMPRESSION"),
        (ErrorCode::DESTROY, "_DESTROY"),
        (ErrorCode::FAIL, "_FAIL"),
        (ErrorCode::TRANSPORT, "_TRANSPORT"),
        (ErrorCode::CRIT_SYS_RESOURCE, "_CRIT_SYS_RESOURCE"),
        (ErrorCode::RESOLVE, "_RESOLVE"),
        (ErrorCode::MSG_TIMED_OUT, "_MSG_TIMED_OUT"),
        (ErrorCode::PARTITION_EOF, "_PARTITION_EOF"),
        (ErrorCode::UNKNOWN_PARTITION, "_UNKNOWN_PARTITION"),
        (ErrorCode::FS, "_FS"),
        (ErrorCode::UNKNOWN_TOPIC, "_UNKNOWN_TOPIC"),
        (ErrorCode::ALL_BROKERS_DOWN, "_ALL_BROKERS_DOWN"),
        (ErrorCode::INVALID_ARG, "_INVALID_ARG"),
        (ErrorCode::TIMED_OUT, "_TIMED_OUT"),
        (ErrorCode::QUEUE_FULL, "_QUEUE_FULL"),
        (ErrorCode::ISR_INSUFF, "_ISR_INSUFF"),
        (ErrorCode::NODE_UPDATE, "_NODE_UPDATE"),
        (ErrorCode::SSL, "_SSL"),
        (ErrorCode::WAIT_COORD, "_WAIT_COORD"),
        (ErrorCode::UNKNOWN_GROUP, "_UNKNOWN_GROUP"),
        (ErrorCode::IN_PROGRESS, "_IN_PROGRESS"),
        (ErrorCode::PREV_IN_PROGRESS, "_PREV_IN_PROGRESS"),
        (ErrorCode::EXISTING_SUBSCRIPTION, "_EXISTING_SUBSCRIPTION"),
        (ErrorCode::ASSIGN_PARTITIONS, "_ASSIGN_PARTITIONS"),
        (ErrorCode::REVOKE_PARTITIONS, "_REVOKE_PARTITIONS"),
        (ErrorCode::CONFLICT, "_CONFLICT"),
        (ErrorCode::STATE, "_STATE"),
        (ErrorCode::UNKNOWN_PROTOCOL, "_UNKNOWN_PROTOCOL"),
        (ErrorCode::NOT_IMPLEMENTED, "_NOT_IMPLEMENTED"),
        (ErrorCode::AUTHENTICATION, "_AUTHENTICATION"),
        (ErrorCode::NO_OFFSET, "_NO_OFFSET"),
        (ErrorCode::OUTDATED, "_OUTDATED"),
        (ErrorCode::TIMED_OUT_QUEUE, "_TIMED_OUT_QUEUE"),
        (ErrorCode::UNSUPPORTED_FEATURE, "_UNSUPPORTED_FEATURE"),
        (ErrorCode::WAIT_CACHE, "_WAIT_CACHE"),
        (ErrorCode::INTR, "_INTR"),
        (ErrorCode::KEY_SERIALIZATION, "_KEY_SERIALIZATION"),
        (ErrorCode::VALUE_SERIALIZATION, "_VALUE_SERIALIZATION"),
        (ErrorCode::KEY_DESERIALIZATION, "_KEY_DESERIALIZATION"),
        (ErrorCode::VALUE_DESERIALIZATION, "_VALUE_DESERIALIZATION"),
        (ErrorCode::PARTIAL, "_PARTIAL"),
        (ErrorCode::READ_ONLY, "_READ_ONLY"),
        (ErrorCode::NOENT, "_NOENT"),
        (ErrorCode::UNDERFLOW, "_UNDERFLOW"),
        (ErrorCode::INVALID_TYPE, "_INVALID_TYPE"),
        (ErrorCode::RETRY, "_RETRY"),
        (ErrorCode::PURGE_QUEUE, "_PURGE_QUEUE"),
        (ErrorCode::PURGE_INFLIGHT, "_PURGE_INFLIGHT"),
        (ErrorCode::FATAL, "_FATAL"),
        (ErrorCode::INCONSISTENT, "_INCONSISTENT"),
        (ErrorCode::GAPLESS_GUARANTEE, "_GAPLESS_GUARANTEE"),
        (ErrorCode::MAX_POLL_EXCEEDED, "_MAX_POLL_EXCEEDED"),
        (ErrorCode::UNKNOWN_BROKER, "_UNKNOWN_BROKER"),
        (ErrorCode::NOT_CONFIGURED, "_NOT_CONFIGURED"),
        (ErrorCode::FENCED, "_FENCED"),
        (ErrorCode::APPLICATION, "_APPLICATION"),
        (ErrorCode::ASSIGNMENT_LOST, "_ASSIGNMENT_LOST"),
        (ErrorCode::NOOP, "_NOOP"),
        (ErrorCode::AUTO_OFFSET_RESET, "_AUTO_OFFSET_RESET"),
        (ErrorCode::LOG_TRUNCATION, "_LOG_TRUNCATION"),
        (
            ErrorCode::INVALID_DIFFERENT_RECORD,
            "_INVALID_DIFFERENT_RECORD",
        ),
        (ErrorCode::DESTROY_BROKER, "_DESTROY_BROKER"),
        (ErrorCode::UNKNOWN, "UNKNOWN"),
        (ErrorCode::NO_ERROR, "NO_ERROR"),
        (ErrorCode::OFFSET_OUT_OF_RANGE, "OFFSET_OUT_OF_RANGE"),
        (ErrorCode::INVALID_MSG, "INVALID_MSG"),
        (ErrorCode::UNKNOWN_TOPIC_OR_PART, "UNKNOWN_TOPIC_OR_PART"),
        (ErrorCode::INVALID_MSG_SIZE, "INVALID_MSG_SIZE"),
        (ErrorCode::LEADER_NOT_AVAILABLE, "LEADER_NOT_AVAILABLE"),
        (
            ErrorCode::NOT_LEADER_FOR_PARTITION,
            "NOT_LEADER_FOR_PARTITION",
        ),
        (ErrorCode::REQUEST_TIMED_OUT, "REQUEST_TIMED_OUT"),
        (ErrorCode::BROKER_NOT_AVAILABLE, "BROKER_NOT_AVAILABLE"),
        (ErrorCode::REPLICA_NOT_AVAILABLE, "REPLICA_NOT_AVAILABLE"),
        (ErrorCode::MSG_SIZE_TOO_LARGE, "MSG_SIZE_TOO_LARGE"),
        (ErrorCode::STALE_CTRL_EPOCH, "STALE_CTRL_EPOCH"),
        (
            ErrorCode::OFFSET_METADATA_TOO_LARGE,
            "OFFSET_METADATA_TOO_LARGE",
        ),
        (ErrorCode::NETWORK_EXCEPTION, "NETWORK_EXCEPTION"),
        (
            ErrorCode::COORDINATOR_LOAD_IN_PROGRESS,
            "COORDINATOR_LOAD_IN_PROGRESS",
        ),
        (
            ErrorCode::COORDINATOR_NOT_AVAILABLE,
            "COORDINATOR_NOT_AVAILABLE",
        ),
        (ErrorCode::NOT_COORDINATOR, "NOT_COORDINATOR"),
        (ErrorCode::TOPIC_EXCEPTION, "TOPIC_EXCEPTION"),
        (ErrorCode::RECORD_LIST_TOO_LARGE, "RECORD_LIST_TOO_LARGE"),
        (ErrorCode::NOT_ENOUGH_REPLICAS, "NOT_ENOUGH_REPLICAS"),
        (
            ErrorCode::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        ),
        (ErrorCode::INVALID_REQUIRED_ACKS, "INVALID_REQUIRED_ACKS"),
        (ErrorCode::ILLEGAL_GENERATION, "ILLEGAL_GENERATION"),
        (
            ErrorCode::INCONSISTENT_GROUP_PROTOCOL,
            "INCONSISTENT_GROUP_PROTOCOL",
        ),
        (ErrorCode::INVALID_GROUP_ID, "INVALID_GROUP_ID"),
        (ErrorCode::UNKNOWN_MEMBER_ID, "UNKNOWN_MEMBER_ID"),
        (
            ErrorCode::INVALID_SESSION_TIMEOUT,
            "INVALID_SESSION_TIMEOUT",
        ),
        (ErrorCode::REBALANCE_IN_PROGRESS, "REBALANCE_IN_PROGRESS"),
        (
            ErrorCode::INVALID_COMMIT_OFFSET_SIZE,
            "INVALID_COMMIT_OFFSET_SIZE",
        ),
        (
            ErrorCode::TOPIC_AUTHORIZATION_FAILED,
            "TOPIC_AUTHORIZATION_FAILED",
        ),
        (
            ErrorCode::GROUP_AUTHORIZATION_FAILED,
            "GROUP_AUTHORIZATION_FAILED",
        ),
        (
            ErrorCode::CLUSTER_AUTHORIZATION_FAILED,
            "CLUSTER_AUTHORIZATION_FAILED",
        ),
        (ErrorCode::INVALID_TIMESTAMP, "INVALID_TIMESTAMP"),
        (
            ErrorCode::UNSUPPORTED_SASL_MECHANISM,
            "UNSUPPORTED_SASL_MECHANISM",
        ),
        (ErrorCode::ILLEGAL_SASL_STATE, "ILLEGAL_SASL_STATE"),
        (ErrorCode::UNSUPPORTED_VERSION, "UNSUPPORTED_VERSION"),
        (ErrorCode::TOPIC_ALREADY_EXISTS, "TOPIC_ALREADY_EXISTS"),
        (ErrorCode::INVALID_PARTITIONS, "INVALID_PARTITIONS"),
        (
            ErrorCode::INVALID_REPLICATION_FACTOR,
            "INVALID_REPLICATION_FACTOR",
        ),
        (
            ErrorCode::INVALID_REPLICA_ASSIGNMENT,
            "INVALID_REPLICA_ASSIGNMENT",
        ),
        (ErrorCode::INVALID_CONFIG, "INVALID_CONFIG"),
        (ErrorCode::NOT_CONTROLLER, "NOT_CONTROLLER"),
        (ErrorCode::INVALID_REQUEST, "INVALID_REQUEST"),
        (
            ErrorCode::UNSUPPORTED_FOR_MESSAGE_FORMAT,
            "UNSUPPORTED_FOR_MESSAGE_FORMAT",
        ),
        (ErrorCode::POLICY_VIOLATION, "POLICY_VIOLATION"),
        (
            ErrorCode::OUT_OF_ORDER_SEQUENCE_NUMBER,
            "OUT_OF_ORDER_SEQUENCE_NUMBER",
        ),
        (
            ErrorCode::DUPLICATE_SEQUENCE_NUMBER,
            "DUPLICATE_SEQUENCE_NUMBER",
        ),
        (ErrorCode::INVALID_PRODUCER_EPOCH, "INVALID_PRODUCER_EPOCH"),
        (ErrorCode::INVALID_TXN_STATE, "INVALID_TXN_STATE"),
        (
            ErrorCode::INVALID_PRODUCER_ID_MAPPING,
            "INVALID_PRODUCER_ID_MAPPING",
        ),
        (
            ErrorCode::INVALID_TRANSACTION_TIMEOUT,
            "INVALID_TRANSACTION_TIMEOUT",
        ),
        (
            ErrorCode::CONCURRENT_TRANSACTIONS,
            "CONCURRENT_TRANSACTIONS",
        ),
        (
            ErrorCode::TRANSACTION_COORDINATOR_FENCED,
            "TRANSACTION_COORDINATOR_FENCED",
        ),
        (
            ErrorCode::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        ),
        (ErrorCode::SECURITY_DISABLED, "SECURITY_DISABLED"),
        (
            ErrorCode::OPERATION_NOT_ATTEMPTED,
            "OPERATION_NOT_ATTEMPTED",
        ),
        (ErrorCode::KAFKA_STORAGE_ERROR, "KAFKA_STORAGE_ERROR"),
        (ErrorCode::LOG_DIR_NOT_FOUND, "LOG_DIR_NOT_FOUND"),
        (
            ErrorCode::SASL_AUTHENTICATION_FAILED,
            "SASL_AUTHENTICATION_FAILED",
        ),
        (ErrorCode::UNKNOWN_PRODUCER_ID, "UNKNOWN_PRODUCER_ID"),
        (
            ErrorCode::REASSIGNMENT_IN_PROGRESS,
            "REASSIGNMENT_IN_PROGRESS",
        ),
        (
            ErrorCode::DELEGATION_TOKEN_AUTH_DISABLED,
            "DELEGATION_TOKEN_AUTH_DISABLED",
        ),
        (
            ErrorCode::DELEGATION_TOKEN_NOT_FOUND,
            "DELEGATION_TOKEN_NOT_FOUND",
        ),
        (
            ErrorCode::DELEGATION_TOKEN_OWNER_MISMATCH,
            "DELEGATION_TOKEN_OWNER_MISMATCH",
        ),
        (
            ErrorCode::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
        ),
        (
            ErrorCode::DELEGATION_TOKEN_AUTHORIZATION_FAILED,
            "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
        ),
        (
            ErrorCode::DELEGATION_TOKEN_EXPIRED,
            "DELEGATION_TOKEN_EXPIRED",
        ),
        (ErrorCode::INVALID_PRINCIPAL_TYPE, "INVALID_PRINCIPAL_TYPE"),
        (ErrorCode::NON_EMPTY_GROUP, "NON_EMPTY_GROUP"),
        (ErrorCode::GROUP_ID_NOT_FOUND, "GROUP_ID_NOT_FOUND"),
        (
            ErrorCode::FETCH_SESSION_ID_NOT_FOUND,
            "FETCH_SESSION_ID_NOT_FOUND",
        ),
        (
            ErrorCode::INVALID_FETCH_SESSION_EPOCH,
            "INVALID_FETCH_SESSION_EPOCH",
        ),
        (ErrorCode::LISTENER_NOT_FOUND, "LISTENER_NOT_FOUND"),
        (
            ErrorCode::TOPIC_DELETION_DISABLED,
            "TOPIC_DELETION_DISABLED",
        ),
        (ErrorCode::FENCED_LEADER_EPOCH, "FENCED_LEADER_EPOCH"),
        (ErrorCode::UNKNOWN_LEADER_EPOCH, "UNKNOWN_LEADER_EPOCH"),
        (
            ErrorCode::UNSUPPORTED_COMPRESSION_TYPE,
            "UNSUPPORTED_COMPRESSION_TYPE",
        ),
        (ErrorCode::STALE_BROKER_EPOCH, "STALE_BROKER_EPOCH"),
        (ErrorCode::OFFSET_NOT_AVAILABLE, "OFFSET_NOT_AVAILABLE"),
        (ErrorCode::MEMBER_ID_REQUIRED, "MEMBER_ID_REQUIRED"),
        (
            ErrorCode::PREFERRED_LEADER_NOT_AVAILABLE,
            "PREFERRED_LEADER_NOT_AVAILABLE",
        ),
        (ErrorCode::GROUP_MAX_SIZE_REACHED, "GROUP_MAX_SIZE_REACHED"),
        (ErrorCode::FENCED_INSTANCE_ID, "FENCED_INSTANCE_ID"),
        (
            ErrorCode::ELIGIBLE_LEADERS_NOT_AVAILABLE,
            "ELIGIBLE_LEADERS_NOT_AVAILABLE",
        ),
        (ErrorCode::ELECTION_NOT_NEEDED, "ELECTION_NOT_NEEDED"),
        (
            ErrorCode::NO_REASSIGNMENT_IN_PROGRESS,
            "NO_REASSIGNMENT_IN_PROGRESS",
        ),
        (
            ErrorCode::GROUP_SUBSCRIBED_TO_TOPIC,
            "GROUP_SUBSCRIBED_TO_TOPIC",
        ),
        (ErrorCode::INVALID_RECORD, "INVALID_RECORD"),
        (ErrorCode::UNSTABLE_OFFSET_COMMIT, "UNSTABLE_OFFSET_COMMIT"),
        (
            ErrorCode::THROTTLING_QUOTA_EXCEEDED,
            "THROTTLING_QUOTA_EXCEEDED",
        ),
        (ErrorCode::PRODUCER_FENCED, "PRODUCER_FENCED"),
        (ErrorCode::RESOURCE_NOT_FOUND, "RESOURCE_NOT_FOUND"),
        (ErrorCode::DUPLICATE_RESOURCE, "DUPLICATE_RESOURCE"),
        (
            ErrorCode::UNACCEPTABLE_CREDENTIAL,
            "UNACCEPTABLE_CREDENTIAL",
        ),
        (ErrorCode::INCONSISTENT_VOTER_SET, "INCONSISTENT_VOTER_SET"),
        (ErrorCode::INVALID_UPDATE_VERSION, "INVALID_UPDATE_VERSION"),
        (ErrorCode::FEATURE_UPDATE_FAILED, "FEATURE_UPDATE_FAILED"),
        (
            ErrorCode::PRINCIPAL_DESERIALIZATION_FAILURE,
            "PRINCIPAL_DESERIALIZATION_FAILURE",
        ),
        (ErrorCode::UNKNOWN_TOPIC_ID, "UNKNOWN_TOPIC_ID"),
        (ErrorCode::INCONSISTENT_TOPIC_ID, "INCONSISTENT_TOPIC_ID"),
        (ErrorCode::FENCED_MEMBER_EPOCH, "FENCED_MEMBER_EPOCH"),
        (ErrorCode::UNRELEASED_INSTANCE_ID, "UNRELEASED_INSTANCE_ID"),
        (ErrorCode::UNSUPPORTED_ASSIGNOR, "UNSUPPORTED_ASSIGNOR"),
        (ErrorCode::STALE_MEMBER_EPOCH, "STALE_MEMBER_EPOCH"),
        (
            ErrorCode::MISMATCHED_ENDPOINT_TYPE,
            "MISMATCHED_ENDPOINT_TYPE",
        ),
        (
            ErrorCode::UNSUPPORTED_ENDPOINT_TYPE,
            "UNSUPPORTED_ENDPOINT_TYPE",
        ),
        (ErrorCode::UNKNOWN_CONTROLLER_ID, "UNKNOWN_CONTROLLER_ID"),
        (
            ErrorCode::UNKNOWN_SUBSCRIPTION_ID,
            "UNKNOWN_SUBSCRIPTION_ID",
        ),
        (ErrorCode::TELEMETRY_TOO_LARGE, "TELEMETRY_TOO_LARGE"),
        (ErrorCode::INVALID_REGISTRATION, "INVALID_REGISTRATION"),
        (ErrorCode::TRANSACTION_ABORTABLE, "TRANSACTION_ABORTABLE"),
        (ErrorCode::INVALID_RECORD_STATE, "INVALID_RECORD_STATE"),
        (
            ErrorCode::SHARE_SESSION_NOT_FOUND,
            "SHARE_SESSION_NOT_FOUND",
        ),
        (
            ErrorCode::INVALID_SHARE_SESSION_EPOCH,
            "INVALID_SHARE_SESSION_EPOCH",
        ),
        (ErrorCode::FENCED_STATE_EPOCH, "FENCED_STATE_EPOCH"),
        (ErrorCode::INVALID_VOTER_KEY, "INVALID_VOTER_KEY"),
        (ErrorCode::DUPLICATE_VOTER, "DUPLICATE_VOTER"),
        (ErrorCode::VOTER_NOT_FOUND, "VOTER_NOT_FOUND"),
        (
            ErrorCode::INVALID_REGULAR_EXPRESSION,
            "INVALID_REGULAR_EXPRESSION",
        ),
        (ErrorCode::REBOOTSTRAP_REQUIRED, "REBOOTSTRAP_REQUIRED"),
        (
            ErrorCode::STREAMS_INVALID_TOPOLOGY,
            "STREAMS_INVALID_TOPOLOGY",
        ),
        (
            ErrorCode::STREAMS_INVALID_TOPOLOGY_EPOCH,
            "STREAMS_INVALID_TOPOLOGY_EPOCH",
        ),
        (
            ErrorCode::STREAMS_TOPOLOGY_FENCED,
            "STREAMS_TOPOLOGY_FENCED",
        ),
        (
            ErrorCode::SHARE_SESSION_LIMIT_REACHED,
            "SHARE_SESSION_LIMIT_REACHED",
        ),
    ];
    for (code, name) in codes {
        assert_eq!(code.name(), name);
    }
}

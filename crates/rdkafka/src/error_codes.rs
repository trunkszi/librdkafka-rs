//! Named numeric error codes from the statically pinned native API.
use super::ErrorCode;
impl ErrorCode {
    /// Native `RD_KAFKA_RESP_ERR__BEGIN`.
    pub const BEGIN: Self = Self(-200);
    /// Native `RD_KAFKA_RESP_ERR__BAD_MSG`.
    pub const BAD_MSG: Self = Self(-199);
    /// Native `RD_KAFKA_RESP_ERR__BAD_COMPRESSION`.
    pub const BAD_COMPRESSION: Self = Self(-198);
    /// Native `RD_KAFKA_RESP_ERR__DESTROY`.
    pub const DESTROY: Self = Self(-197);
    /// Native `RD_KAFKA_RESP_ERR__FAIL`.
    pub const FAIL: Self = Self(-196);
    /// Native `RD_KAFKA_RESP_ERR__TRANSPORT`.
    pub const TRANSPORT: Self = Self(-195);
    /// Native `RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE`.
    pub const CRIT_SYS_RESOURCE: Self = Self(-194);
    /// Native `RD_KAFKA_RESP_ERR__RESOLVE`.
    pub const RESOLVE: Self = Self(-193);
    /// Native `RD_KAFKA_RESP_ERR__MSG_TIMED_OUT`.
    pub const MSG_TIMED_OUT: Self = Self(-192);
    /// Native `RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION`.
    pub const UNKNOWN_PARTITION: Self = Self(-190);
    /// Native `RD_KAFKA_RESP_ERR__FS`.
    pub const FS: Self = Self(-189);
    /// Native `RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC`.
    pub const UNKNOWN_TOPIC: Self = Self(-188);
    /// Native `RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN`.
    pub const ALL_BROKERS_DOWN: Self = Self(-187);
    /// Native `RD_KAFKA_RESP_ERR__INVALID_ARG`.
    pub const INVALID_ARG: Self = Self(-186);
    /// Native `RD_KAFKA_RESP_ERR__ISR_INSUFF`.
    pub const ISR_INSUFF: Self = Self(-183);
    /// Native `RD_KAFKA_RESP_ERR__NODE_UPDATE`.
    pub const NODE_UPDATE: Self = Self(-182);
    /// Native `RD_KAFKA_RESP_ERR__SSL`.
    pub const SSL: Self = Self(-181);
    /// Native `RD_KAFKA_RESP_ERR__WAIT_COORD`.
    pub const WAIT_COORD: Self = Self(-180);
    /// Native `RD_KAFKA_RESP_ERR__UNKNOWN_GROUP`.
    pub const UNKNOWN_GROUP: Self = Self(-179);
    /// Native `RD_KAFKA_RESP_ERR__IN_PROGRESS`.
    pub const IN_PROGRESS: Self = Self(-178);
    /// Native `RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS`.
    pub const PREV_IN_PROGRESS: Self = Self(-177);
    /// Native `RD_KAFKA_RESP_ERR__EXISTING_SUBSCRIPTION`.
    pub const EXISTING_SUBSCRIPTION: Self = Self(-176);
    /// Native `RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS`.
    pub const ASSIGN_PARTITIONS: Self = Self(-175);
    /// Native `RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS`.
    pub const REVOKE_PARTITIONS: Self = Self(-174);
    /// Native `RD_KAFKA_RESP_ERR__CONFLICT`.
    pub const CONFLICT: Self = Self(-173);
    /// Native `RD_KAFKA_RESP_ERR__STATE`.
    pub const STATE: Self = Self(-172);
    /// Native `RD_KAFKA_RESP_ERR__UNKNOWN_PROTOCOL`.
    pub const UNKNOWN_PROTOCOL: Self = Self(-171);
    /// Native `RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED`.
    pub const NOT_IMPLEMENTED: Self = Self(-170);
    /// Native `RD_KAFKA_RESP_ERR__AUTHENTICATION`.
    pub const AUTHENTICATION: Self = Self(-169);
    /// Native `RD_KAFKA_RESP_ERR__NO_OFFSET`.
    pub const NO_OFFSET: Self = Self(-168);
    /// Native `RD_KAFKA_RESP_ERR__OUTDATED`.
    pub const OUTDATED: Self = Self(-167);
    /// Native `RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE`.
    pub const TIMED_OUT_QUEUE: Self = Self(-166);
    /// Native `RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE`.
    pub const UNSUPPORTED_FEATURE: Self = Self(-165);
    /// Native `RD_KAFKA_RESP_ERR__WAIT_CACHE`.
    pub const WAIT_CACHE: Self = Self(-164);
    /// Native `RD_KAFKA_RESP_ERR__INTR`.
    pub const INTR: Self = Self(-163);
    /// Native `RD_KAFKA_RESP_ERR__KEY_SERIALIZATION`.
    pub const KEY_SERIALIZATION: Self = Self(-162);
    /// Native `RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION`.
    pub const VALUE_SERIALIZATION: Self = Self(-161);
    /// Native `RD_KAFKA_RESP_ERR__KEY_DESERIALIZATION`.
    pub const KEY_DESERIALIZATION: Self = Self(-160);
    /// Native `RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION`.
    pub const VALUE_DESERIALIZATION: Self = Self(-159);
    /// Native `RD_KAFKA_RESP_ERR__PARTIAL`.
    pub const PARTIAL: Self = Self(-158);
    /// Native `RD_KAFKA_RESP_ERR__READ_ONLY`.
    pub const READ_ONLY: Self = Self(-157);
    /// Native `RD_KAFKA_RESP_ERR__NOENT`.
    pub const NOENT: Self = Self(-156);
    /// Native `RD_KAFKA_RESP_ERR__UNDERFLOW`.
    pub const UNDERFLOW: Self = Self(-155);
    /// Native `RD_KAFKA_RESP_ERR__INVALID_TYPE`.
    pub const INVALID_TYPE: Self = Self(-154);
    /// Native `RD_KAFKA_RESP_ERR__RETRY`.
    pub const RETRY: Self = Self(-153);
    /// Native `RD_KAFKA_RESP_ERR__PURGE_QUEUE`.
    pub const PURGE_QUEUE: Self = Self(-152);
    /// Native `RD_KAFKA_RESP_ERR__PURGE_INFLIGHT`.
    pub const PURGE_INFLIGHT: Self = Self(-151);
    /// Native `RD_KAFKA_RESP_ERR__INCONSISTENT`.
    pub const INCONSISTENT: Self = Self(-149);
    /// Native `RD_KAFKA_RESP_ERR__GAPLESS_GUARANTEE`.
    pub const GAPLESS_GUARANTEE: Self = Self(-148);
    /// Native `RD_KAFKA_RESP_ERR__MAX_POLL_EXCEEDED`.
    pub const MAX_POLL_EXCEEDED: Self = Self(-147);
    /// Native `RD_KAFKA_RESP_ERR__UNKNOWN_BROKER`.
    pub const UNKNOWN_BROKER: Self = Self(-146);
    /// Native `RD_KAFKA_RESP_ERR__NOT_CONFIGURED`.
    pub const NOT_CONFIGURED: Self = Self(-145);
    /// Native `RD_KAFKA_RESP_ERR__FENCED`.
    pub const FENCED: Self = Self(-144);
    /// Native `RD_KAFKA_RESP_ERR__APPLICATION`.
    pub const APPLICATION: Self = Self(-143);
    /// Native `RD_KAFKA_RESP_ERR__ASSIGNMENT_LOST`.
    pub const ASSIGNMENT_LOST: Self = Self(-142);
    /// Native `RD_KAFKA_RESP_ERR__NOOP`.
    pub const NOOP: Self = Self(-141);
    /// Native `RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET`.
    pub const AUTO_OFFSET_RESET: Self = Self(-140);
    /// Native `RD_KAFKA_RESP_ERR__LOG_TRUNCATION`.
    pub const LOG_TRUNCATION: Self = Self(-139);
    /// Native `RD_KAFKA_RESP_ERR__INVALID_DIFFERENT_RECORD`.
    pub const INVALID_DIFFERENT_RECORD: Self = Self(-138);
    /// Native `RD_KAFKA_RESP_ERR__DESTROY_BROKER`.
    pub const DESTROY_BROKER: Self = Self(-137);
    /// Native `RD_KAFKA_RESP_ERR__END`.
    pub const END: Self = Self(-100);
    /// Native `RD_KAFKA_RESP_ERR_UNKNOWN`.
    pub const UNKNOWN: Self = Self(-1);
    /// Native `RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE`.
    pub const OFFSET_OUT_OF_RANGE: Self = Self(1);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_MSG`.
    pub const INVALID_MSG: Self = Self(2);
    /// Native `RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART`.
    pub const UNKNOWN_TOPIC_OR_PART: Self = Self(3);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE`.
    pub const INVALID_MSG_SIZE: Self = Self(4);
    /// Native `RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE`.
    pub const LEADER_NOT_AVAILABLE: Self = Self(5);
    /// Native `RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION`.
    pub const NOT_LEADER_FOR_PARTITION: Self = Self(6);
    /// Native `RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT`.
    pub const REQUEST_TIMED_OUT: Self = Self(7);
    /// Native `RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE`.
    pub const BROKER_NOT_AVAILABLE: Self = Self(8);
    /// Native `RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE`.
    pub const REPLICA_NOT_AVAILABLE: Self = Self(9);
    /// Native `RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE`.
    pub const MSG_SIZE_TOO_LARGE: Self = Self(10);
    /// Native `RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH`.
    pub const STALE_CTRL_EPOCH: Self = Self(11);
    /// Native `RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE`.
    pub const OFFSET_METADATA_TOO_LARGE: Self = Self(12);
    /// Native `RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION`.
    pub const NETWORK_EXCEPTION: Self = Self(13);
    /// Native `RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS`.
    pub const COORDINATOR_LOAD_IN_PROGRESS: Self = Self(14);
    /// Native `RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE`.
    pub const COORDINATOR_NOT_AVAILABLE: Self = Self(15);
    /// Native `RD_KAFKA_RESP_ERR_NOT_COORDINATOR`.
    pub const NOT_COORDINATOR: Self = Self(16);
    /// Native `RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION`.
    pub const TOPIC_EXCEPTION: Self = Self(17);
    /// Native `RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE`.
    pub const RECORD_LIST_TOO_LARGE: Self = Self(18);
    /// Native `RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS`.
    pub const NOT_ENOUGH_REPLICAS: Self = Self(19);
    /// Native `RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND`.
    pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: Self = Self(20);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS`.
    pub const INVALID_REQUIRED_ACKS: Self = Self(21);
    /// Native `RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION`.
    pub const ILLEGAL_GENERATION: Self = Self(22);
    /// Native `RD_KAFKA_RESP_ERR_INCONSISTENT_GROUP_PROTOCOL`.
    pub const INCONSISTENT_GROUP_PROTOCOL: Self = Self(23);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_GROUP_ID`.
    pub const INVALID_GROUP_ID: Self = Self(24);
    /// Native `RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID`.
    pub const UNKNOWN_MEMBER_ID: Self = Self(25);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_SESSION_TIMEOUT`.
    pub const INVALID_SESSION_TIMEOUT: Self = Self(26);
    /// Native `RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS`.
    pub const REBALANCE_IN_PROGRESS: Self = Self(27);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE`.
    pub const INVALID_COMMIT_OFFSET_SIZE: Self = Self(28);
    /// Native `RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED`.
    pub const TOPIC_AUTHORIZATION_FAILED: Self = Self(29);
    /// Native `RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED`.
    pub const GROUP_AUTHORIZATION_FAILED: Self = Self(30);
    /// Native `RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED`.
    pub const CLUSTER_AUTHORIZATION_FAILED: Self = Self(31);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_TIMESTAMP`.
    pub const INVALID_TIMESTAMP: Self = Self(32);
    /// Native `RD_KAFKA_RESP_ERR_UNSUPPORTED_SASL_MECHANISM`.
    pub const UNSUPPORTED_SASL_MECHANISM: Self = Self(33);
    /// Native `RD_KAFKA_RESP_ERR_ILLEGAL_SASL_STATE`.
    pub const ILLEGAL_SASL_STATE: Self = Self(34);
    /// Native `RD_KAFKA_RESP_ERR_UNSUPPORTED_VERSION`.
    pub const UNSUPPORTED_VERSION: Self = Self(35);
    /// Native `RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS`.
    pub const TOPIC_ALREADY_EXISTS: Self = Self(36);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_PARTITIONS`.
    pub const INVALID_PARTITIONS: Self = Self(37);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_REPLICATION_FACTOR`.
    pub const INVALID_REPLICATION_FACTOR: Self = Self(38);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_REPLICA_ASSIGNMENT`.
    pub const INVALID_REPLICA_ASSIGNMENT: Self = Self(39);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_CONFIG`.
    pub const INVALID_CONFIG: Self = Self(40);
    /// Native `RD_KAFKA_RESP_ERR_NOT_CONTROLLER`.
    pub const NOT_CONTROLLER: Self = Self(41);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_REQUEST`.
    pub const INVALID_REQUEST: Self = Self(42);
    /// Native `RD_KAFKA_RESP_ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT`.
    pub const UNSUPPORTED_FOR_MESSAGE_FORMAT: Self = Self(43);
    /// Native `RD_KAFKA_RESP_ERR_POLICY_VIOLATION`.
    pub const POLICY_VIOLATION: Self = Self(44);
    /// Native `RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER`.
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: Self = Self(45);
    /// Native `RD_KAFKA_RESP_ERR_DUPLICATE_SEQUENCE_NUMBER`.
    pub const DUPLICATE_SEQUENCE_NUMBER: Self = Self(46);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH`.
    pub const INVALID_PRODUCER_EPOCH: Self = Self(47);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_TXN_STATE`.
    pub const INVALID_TXN_STATE: Self = Self(48);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_PRODUCER_ID_MAPPING`.
    pub const INVALID_PRODUCER_ID_MAPPING: Self = Self(49);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_TRANSACTION_TIMEOUT`.
    pub const INVALID_TRANSACTION_TIMEOUT: Self = Self(50);
    /// Native `RD_KAFKA_RESP_ERR_CONCURRENT_TRANSACTIONS`.
    pub const CONCURRENT_TRANSACTIONS: Self = Self(51);
    /// Native `RD_KAFKA_RESP_ERR_TRANSACTION_COORDINATOR_FENCED`.
    pub const TRANSACTION_COORDINATOR_FENCED: Self = Self(52);
    /// Native `RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED`.
    pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: Self = Self(53);
    /// Native `RD_KAFKA_RESP_ERR_SECURITY_DISABLED`.
    pub const SECURITY_DISABLED: Self = Self(54);
    /// Native `RD_KAFKA_RESP_ERR_OPERATION_NOT_ATTEMPTED`.
    pub const OPERATION_NOT_ATTEMPTED: Self = Self(55);
    /// Native `RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR`.
    pub const KAFKA_STORAGE_ERROR: Self = Self(56);
    /// Native `RD_KAFKA_RESP_ERR_LOG_DIR_NOT_FOUND`.
    pub const LOG_DIR_NOT_FOUND: Self = Self(57);
    /// Native `RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED`.
    pub const SASL_AUTHENTICATION_FAILED: Self = Self(58);
    /// Native `RD_KAFKA_RESP_ERR_UNKNOWN_PRODUCER_ID`.
    pub const UNKNOWN_PRODUCER_ID: Self = Self(59);
    /// Native `RD_KAFKA_RESP_ERR_REASSIGNMENT_IN_PROGRESS`.
    pub const REASSIGNMENT_IN_PROGRESS: Self = Self(60);
    /// Native `RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_AUTH_DISABLED`.
    pub const DELEGATION_TOKEN_AUTH_DISABLED: Self = Self(61);
    /// Native `RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_NOT_FOUND`.
    pub const DELEGATION_TOKEN_NOT_FOUND: Self = Self(62);
    /// Native `RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_OWNER_MISMATCH`.
    pub const DELEGATION_TOKEN_OWNER_MISMATCH: Self = Self(63);
    /// Native `RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`.
    pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: Self = Self(64);
    /// Native `RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_AUTHORIZATION_FAILED`.
    pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: Self = Self(65);
    /// Native `RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_EXPIRED`.
    pub const DELEGATION_TOKEN_EXPIRED: Self = Self(66);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_PRINCIPAL_TYPE`.
    pub const INVALID_PRINCIPAL_TYPE: Self = Self(67);
    /// Native `RD_KAFKA_RESP_ERR_NON_EMPTY_GROUP`.
    pub const NON_EMPTY_GROUP: Self = Self(68);
    /// Native `RD_KAFKA_RESP_ERR_GROUP_ID_NOT_FOUND`.
    pub const GROUP_ID_NOT_FOUND: Self = Self(69);
    /// Native `RD_KAFKA_RESP_ERR_FETCH_SESSION_ID_NOT_FOUND`.
    pub const FETCH_SESSION_ID_NOT_FOUND: Self = Self(70);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_FETCH_SESSION_EPOCH`.
    pub const INVALID_FETCH_SESSION_EPOCH: Self = Self(71);
    /// Native `RD_KAFKA_RESP_ERR_LISTENER_NOT_FOUND`.
    pub const LISTENER_NOT_FOUND: Self = Self(72);
    /// Native `RD_KAFKA_RESP_ERR_TOPIC_DELETION_DISABLED`.
    pub const TOPIC_DELETION_DISABLED: Self = Self(73);
    /// Native `RD_KAFKA_RESP_ERR_FENCED_LEADER_EPOCH`.
    pub const FENCED_LEADER_EPOCH: Self = Self(74);
    /// Native `RD_KAFKA_RESP_ERR_UNKNOWN_LEADER_EPOCH`.
    pub const UNKNOWN_LEADER_EPOCH: Self = Self(75);
    /// Native `RD_KAFKA_RESP_ERR_UNSUPPORTED_COMPRESSION_TYPE`.
    pub const UNSUPPORTED_COMPRESSION_TYPE: Self = Self(76);
    /// Native `RD_KAFKA_RESP_ERR_STALE_BROKER_EPOCH`.
    pub const STALE_BROKER_EPOCH: Self = Self(77);
    /// Native `RD_KAFKA_RESP_ERR_OFFSET_NOT_AVAILABLE`.
    pub const OFFSET_NOT_AVAILABLE: Self = Self(78);
    /// Native `RD_KAFKA_RESP_ERR_MEMBER_ID_REQUIRED`.
    pub const MEMBER_ID_REQUIRED: Self = Self(79);
    /// Native `RD_KAFKA_RESP_ERR_PREFERRED_LEADER_NOT_AVAILABLE`.
    pub const PREFERRED_LEADER_NOT_AVAILABLE: Self = Self(80);
    /// Native `RD_KAFKA_RESP_ERR_GROUP_MAX_SIZE_REACHED`.
    pub const GROUP_MAX_SIZE_REACHED: Self = Self(81);
    /// Native `RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID`.
    pub const FENCED_INSTANCE_ID: Self = Self(82);
    /// Native `RD_KAFKA_RESP_ERR_ELIGIBLE_LEADERS_NOT_AVAILABLE`.
    pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: Self = Self(83);
    /// Native `RD_KAFKA_RESP_ERR_ELECTION_NOT_NEEDED`.
    pub const ELECTION_NOT_NEEDED: Self = Self(84);
    /// Native `RD_KAFKA_RESP_ERR_NO_REASSIGNMENT_IN_PROGRESS`.
    pub const NO_REASSIGNMENT_IN_PROGRESS: Self = Self(85);
    /// Native `RD_KAFKA_RESP_ERR_GROUP_SUBSCRIBED_TO_TOPIC`.
    pub const GROUP_SUBSCRIBED_TO_TOPIC: Self = Self(86);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_RECORD`.
    pub const INVALID_RECORD: Self = Self(87);
    /// Native `RD_KAFKA_RESP_ERR_UNSTABLE_OFFSET_COMMIT`.
    pub const UNSTABLE_OFFSET_COMMIT: Self = Self(88);
    /// Native `RD_KAFKA_RESP_ERR_THROTTLING_QUOTA_EXCEEDED`.
    pub const THROTTLING_QUOTA_EXCEEDED: Self = Self(89);
    /// Native `RD_KAFKA_RESP_ERR_PRODUCER_FENCED`.
    pub const PRODUCER_FENCED: Self = Self(90);
    /// Native `RD_KAFKA_RESP_ERR_RESOURCE_NOT_FOUND`.
    pub const RESOURCE_NOT_FOUND: Self = Self(91);
    /// Native `RD_KAFKA_RESP_ERR_DUPLICATE_RESOURCE`.
    pub const DUPLICATE_RESOURCE: Self = Self(92);
    /// Native `RD_KAFKA_RESP_ERR_UNACCEPTABLE_CREDENTIAL`.
    pub const UNACCEPTABLE_CREDENTIAL: Self = Self(93);
    /// Native `RD_KAFKA_RESP_ERR_INCONSISTENT_VOTER_SET`.
    pub const INCONSISTENT_VOTER_SET: Self = Self(94);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_UPDATE_VERSION`.
    pub const INVALID_UPDATE_VERSION: Self = Self(95);
    /// Native `RD_KAFKA_RESP_ERR_FEATURE_UPDATE_FAILED`.
    pub const FEATURE_UPDATE_FAILED: Self = Self(96);
    /// Native `RD_KAFKA_RESP_ERR_PRINCIPAL_DESERIALIZATION_FAILURE`.
    pub const PRINCIPAL_DESERIALIZATION_FAILURE: Self = Self(97);
    /// Native `RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID`.
    pub const UNKNOWN_TOPIC_ID: Self = Self(100);
    /// Native `RD_KAFKA_RESP_ERR_INCONSISTENT_TOPIC_ID`.
    pub const INCONSISTENT_TOPIC_ID: Self = Self(103);
    /// Native `RD_KAFKA_RESP_ERR_FENCED_MEMBER_EPOCH`.
    pub const FENCED_MEMBER_EPOCH: Self = Self(110);
    /// Native `RD_KAFKA_RESP_ERR_UNRELEASED_INSTANCE_ID`.
    pub const UNRELEASED_INSTANCE_ID: Self = Self(111);
    /// Native `RD_KAFKA_RESP_ERR_UNSUPPORTED_ASSIGNOR`.
    pub const UNSUPPORTED_ASSIGNOR: Self = Self(112);
    /// Native `RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH`.
    pub const STALE_MEMBER_EPOCH: Self = Self(113);
    /// Native `RD_KAFKA_RESP_ERR_MISMATCHED_ENDPOINT_TYPE`.
    pub const MISMATCHED_ENDPOINT_TYPE: Self = Self(114);
    /// Native `RD_KAFKA_RESP_ERR_UNSUPPORTED_ENDPOINT_TYPE`.
    pub const UNSUPPORTED_ENDPOINT_TYPE: Self = Self(115);
    /// Native `RD_KAFKA_RESP_ERR_UNKNOWN_CONTROLLER_ID`.
    pub const UNKNOWN_CONTROLLER_ID: Self = Self(116);
    /// Native `RD_KAFKA_RESP_ERR_UNKNOWN_SUBSCRIPTION_ID`.
    pub const UNKNOWN_SUBSCRIPTION_ID: Self = Self(117);
    /// Native `RD_KAFKA_RESP_ERR_TELEMETRY_TOO_LARGE`.
    pub const TELEMETRY_TOO_LARGE: Self = Self(118);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_REGISTRATION`.
    pub const INVALID_REGISTRATION: Self = Self(119);
    /// Native `RD_KAFKA_RESP_ERR_TRANSACTION_ABORTABLE`.
    pub const TRANSACTION_ABORTABLE: Self = Self(120);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_RECORD_STATE`.
    pub const INVALID_RECORD_STATE: Self = Self(121);
    /// Native `RD_KAFKA_RESP_ERR_SHARE_SESSION_NOT_FOUND`.
    pub const SHARE_SESSION_NOT_FOUND: Self = Self(122);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_SHARE_SESSION_EPOCH`.
    pub const INVALID_SHARE_SESSION_EPOCH: Self = Self(123);
    /// Native `RD_KAFKA_RESP_ERR_FENCED_STATE_EPOCH`.
    pub const FENCED_STATE_EPOCH: Self = Self(124);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_VOTER_KEY`.
    pub const INVALID_VOTER_KEY: Self = Self(125);
    /// Native `RD_KAFKA_RESP_ERR_DUPLICATE_VOTER`.
    pub const DUPLICATE_VOTER: Self = Self(126);
    /// Native `RD_KAFKA_RESP_ERR_VOTER_NOT_FOUND`.
    pub const VOTER_NOT_FOUND: Self = Self(127);
    /// Native `RD_KAFKA_RESP_ERR_INVALID_REGULAR_EXPRESSION`.
    pub const INVALID_REGULAR_EXPRESSION: Self = Self(128);
    /// Native `RD_KAFKA_RESP_ERR_REBOOTSTRAP_REQUIRED`.
    pub const REBOOTSTRAP_REQUIRED: Self = Self(129);
    /// Native `RD_KAFKA_RESP_ERR_STREAMS_INVALID_TOPOLOGY`.
    pub const STREAMS_INVALID_TOPOLOGY: Self = Self(130);
    /// Native `RD_KAFKA_RESP_ERR_STREAMS_INVALID_TOPOLOGY_EPOCH`.
    pub const STREAMS_INVALID_TOPOLOGY_EPOCH: Self = Self(131);
    /// Native `RD_KAFKA_RESP_ERR_STREAMS_TOPOLOGY_FENCED`.
    pub const STREAMS_TOPOLOGY_FENCED: Self = Self(132);
    /// Native `RD_KAFKA_RESP_ERR_SHARE_SESSION_LIMIT_REACHED`.
    pub const SHARE_SESSION_LIMIT_REACHED: Self = Self(133);
    /// One past the final broker error defined by the pinned native header.
    pub const END_ALL: Self = Self(134);
    /// Native compatibility alias for `NOT_LEADER_FOR_PARTITION`.
    pub const NOT_LEADER_OR_FOLLOWER: Self = Self::NOT_LEADER_FOR_PARTITION;
    /// Native compatibility alias for `COORDINATOR_LOAD_IN_PROGRESS`.
    pub const GROUP_LOAD_IN_PROGRESS: Self = Self::COORDINATOR_LOAD_IN_PROGRESS;
    /// Native compatibility alias for `COORDINATOR_NOT_AVAILABLE`.
    pub const GROUP_COORDINATOR_NOT_AVAILABLE: Self = Self::COORDINATOR_NOT_AVAILABLE;
    /// Native compatibility alias for `NOT_COORDINATOR`.
    pub const NOT_COORDINATOR_FOR_GROUP: Self = Self::NOT_COORDINATOR;
    /// Every explicitly valued native code, including native range sentinels.
    pub const NATIVE_CODES: &'static [Self] = &[
        Self::BEGIN,
        Self::BAD_MSG,
        Self::BAD_COMPRESSION,
        Self::DESTROY,
        Self::FAIL,
        Self::TRANSPORT,
        Self::CRIT_SYS_RESOURCE,
        Self::RESOLVE,
        Self::MSG_TIMED_OUT,
        Self::PARTITION_EOF,
        Self::UNKNOWN_PARTITION,
        Self::FS,
        Self::UNKNOWN_TOPIC,
        Self::ALL_BROKERS_DOWN,
        Self::INVALID_ARG,
        Self::TIMED_OUT,
        Self::QUEUE_FULL,
        Self::ISR_INSUFF,
        Self::NODE_UPDATE,
        Self::SSL,
        Self::WAIT_COORD,
        Self::UNKNOWN_GROUP,
        Self::IN_PROGRESS,
        Self::PREV_IN_PROGRESS,
        Self::EXISTING_SUBSCRIPTION,
        Self::ASSIGN_PARTITIONS,
        Self::REVOKE_PARTITIONS,
        Self::CONFLICT,
        Self::STATE,
        Self::UNKNOWN_PROTOCOL,
        Self::NOT_IMPLEMENTED,
        Self::AUTHENTICATION,
        Self::NO_OFFSET,
        Self::OUTDATED,
        Self::TIMED_OUT_QUEUE,
        Self::UNSUPPORTED_FEATURE,
        Self::WAIT_CACHE,
        Self::INTR,
        Self::KEY_SERIALIZATION,
        Self::VALUE_SERIALIZATION,
        Self::KEY_DESERIALIZATION,
        Self::VALUE_DESERIALIZATION,
        Self::PARTIAL,
        Self::READ_ONLY,
        Self::NOENT,
        Self::UNDERFLOW,
        Self::INVALID_TYPE,
        Self::RETRY,
        Self::PURGE_QUEUE,
        Self::PURGE_INFLIGHT,
        Self::FATAL,
        Self::INCONSISTENT,
        Self::GAPLESS_GUARANTEE,
        Self::MAX_POLL_EXCEEDED,
        Self::UNKNOWN_BROKER,
        Self::NOT_CONFIGURED,
        Self::FENCED,
        Self::APPLICATION,
        Self::ASSIGNMENT_LOST,
        Self::NOOP,
        Self::AUTO_OFFSET_RESET,
        Self::LOG_TRUNCATION,
        Self::INVALID_DIFFERENT_RECORD,
        Self::DESTROY_BROKER,
        Self::END,
        Self::UNKNOWN,
        Self::NO_ERROR,
        Self::OFFSET_OUT_OF_RANGE,
        Self::INVALID_MSG,
        Self::UNKNOWN_TOPIC_OR_PART,
        Self::INVALID_MSG_SIZE,
        Self::LEADER_NOT_AVAILABLE,
        Self::NOT_LEADER_FOR_PARTITION,
        Self::REQUEST_TIMED_OUT,
        Self::BROKER_NOT_AVAILABLE,
        Self::REPLICA_NOT_AVAILABLE,
        Self::MSG_SIZE_TOO_LARGE,
        Self::STALE_CTRL_EPOCH,
        Self::OFFSET_METADATA_TOO_LARGE,
        Self::NETWORK_EXCEPTION,
        Self::COORDINATOR_LOAD_IN_PROGRESS,
        Self::COORDINATOR_NOT_AVAILABLE,
        Self::NOT_COORDINATOR,
        Self::TOPIC_EXCEPTION,
        Self::RECORD_LIST_TOO_LARGE,
        Self::NOT_ENOUGH_REPLICAS,
        Self::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        Self::INVALID_REQUIRED_ACKS,
        Self::ILLEGAL_GENERATION,
        Self::INCONSISTENT_GROUP_PROTOCOL,
        Self::INVALID_GROUP_ID,
        Self::UNKNOWN_MEMBER_ID,
        Self::INVALID_SESSION_TIMEOUT,
        Self::REBALANCE_IN_PROGRESS,
        Self::INVALID_COMMIT_OFFSET_SIZE,
        Self::TOPIC_AUTHORIZATION_FAILED,
        Self::GROUP_AUTHORIZATION_FAILED,
        Self::CLUSTER_AUTHORIZATION_FAILED,
        Self::INVALID_TIMESTAMP,
        Self::UNSUPPORTED_SASL_MECHANISM,
        Self::ILLEGAL_SASL_STATE,
        Self::UNSUPPORTED_VERSION,
        Self::TOPIC_ALREADY_EXISTS,
        Self::INVALID_PARTITIONS,
        Self::INVALID_REPLICATION_FACTOR,
        Self::INVALID_REPLICA_ASSIGNMENT,
        Self::INVALID_CONFIG,
        Self::NOT_CONTROLLER,
        Self::INVALID_REQUEST,
        Self::UNSUPPORTED_FOR_MESSAGE_FORMAT,
        Self::POLICY_VIOLATION,
        Self::OUT_OF_ORDER_SEQUENCE_NUMBER,
        Self::DUPLICATE_SEQUENCE_NUMBER,
        Self::INVALID_PRODUCER_EPOCH,
        Self::INVALID_TXN_STATE,
        Self::INVALID_PRODUCER_ID_MAPPING,
        Self::INVALID_TRANSACTION_TIMEOUT,
        Self::CONCURRENT_TRANSACTIONS,
        Self::TRANSACTION_COORDINATOR_FENCED,
        Self::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        Self::SECURITY_DISABLED,
        Self::OPERATION_NOT_ATTEMPTED,
        Self::KAFKA_STORAGE_ERROR,
        Self::LOG_DIR_NOT_FOUND,
        Self::SASL_AUTHENTICATION_FAILED,
        Self::UNKNOWN_PRODUCER_ID,
        Self::REASSIGNMENT_IN_PROGRESS,
        Self::DELEGATION_TOKEN_AUTH_DISABLED,
        Self::DELEGATION_TOKEN_NOT_FOUND,
        Self::DELEGATION_TOKEN_OWNER_MISMATCH,
        Self::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
        Self::DELEGATION_TOKEN_AUTHORIZATION_FAILED,
        Self::DELEGATION_TOKEN_EXPIRED,
        Self::INVALID_PRINCIPAL_TYPE,
        Self::NON_EMPTY_GROUP,
        Self::GROUP_ID_NOT_FOUND,
        Self::FETCH_SESSION_ID_NOT_FOUND,
        Self::INVALID_FETCH_SESSION_EPOCH,
        Self::LISTENER_NOT_FOUND,
        Self::TOPIC_DELETION_DISABLED,
        Self::FENCED_LEADER_EPOCH,
        Self::UNKNOWN_LEADER_EPOCH,
        Self::UNSUPPORTED_COMPRESSION_TYPE,
        Self::STALE_BROKER_EPOCH,
        Self::OFFSET_NOT_AVAILABLE,
        Self::MEMBER_ID_REQUIRED,
        Self::PREFERRED_LEADER_NOT_AVAILABLE,
        Self::GROUP_MAX_SIZE_REACHED,
        Self::FENCED_INSTANCE_ID,
        Self::ELIGIBLE_LEADERS_NOT_AVAILABLE,
        Self::ELECTION_NOT_NEEDED,
        Self::NO_REASSIGNMENT_IN_PROGRESS,
        Self::GROUP_SUBSCRIBED_TO_TOPIC,
        Self::INVALID_RECORD,
        Self::UNSTABLE_OFFSET_COMMIT,
        Self::THROTTLING_QUOTA_EXCEEDED,
        Self::PRODUCER_FENCED,
        Self::RESOURCE_NOT_FOUND,
        Self::DUPLICATE_RESOURCE,
        Self::UNACCEPTABLE_CREDENTIAL,
        Self::INCONSISTENT_VOTER_SET,
        Self::INVALID_UPDATE_VERSION,
        Self::FEATURE_UPDATE_FAILED,
        Self::PRINCIPAL_DESERIALIZATION_FAILURE,
        Self::UNKNOWN_TOPIC_ID,
        Self::INCONSISTENT_TOPIC_ID,
        Self::FENCED_MEMBER_EPOCH,
        Self::UNRELEASED_INSTANCE_ID,
        Self::UNSUPPORTED_ASSIGNOR,
        Self::STALE_MEMBER_EPOCH,
        Self::MISMATCHED_ENDPOINT_TYPE,
        Self::UNSUPPORTED_ENDPOINT_TYPE,
        Self::UNKNOWN_CONTROLLER_ID,
        Self::UNKNOWN_SUBSCRIPTION_ID,
        Self::TELEMETRY_TOO_LARGE,
        Self::INVALID_REGISTRATION,
        Self::TRANSACTION_ABORTABLE,
        Self::INVALID_RECORD_STATE,
        Self::SHARE_SESSION_NOT_FOUND,
        Self::INVALID_SHARE_SESSION_EPOCH,
        Self::FENCED_STATE_EPOCH,
        Self::INVALID_VOTER_KEY,
        Self::DUPLICATE_VOTER,
        Self::VOTER_NOT_FOUND,
        Self::INVALID_REGULAR_EXPRESSION,
        Self::REBOOTSTRAP_REQUIRED,
        Self::STREAMS_INVALID_TOPOLOGY,
        Self::STREAMS_INVALID_TOPOLOGY_EPOCH,
        Self::STREAMS_TOPOLOGY_FENCED,
        Self::SHARE_SESSION_LIMIT_REACHED,
    ];
    /// Preserves unknown future broker and client codes without coercion.
    pub const fn from_raw(value: i32) -> Self {
        Self(value)
    }
}

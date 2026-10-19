#include "rdkafka-sys/src/admin.h"
#include "rdkafka-sys/src/admin.rs.h"
namespace kafka_bridge {
namespace {
rust::String str(const char *v) { return v ? v : ""; }
void error(AdminResource &r, const rd_kafka_error_t *e) {
  if (e) {
    r.code = rd_kafka_error_code(e);
    r.message = str(rd_kafka_error_string(e));
  }
}
void partition(AdminResource &r, const rd_kafka_topic_partition_t &p) {
  AdminPartition v{};
  v.topic = str(p.topic);
  v.partition = p.partition;
  v.offset = p.offset;
  v.error = p.err;
  v.timestamp = -1;
  v.leader = -1;
  r.partitions.push_back(std::move(v));
}
void partitions(AdminResource &r, const rd_kafka_topic_partition_list_t *p) {
  if (p)
    for (int i = 0; i < p->cnt; ++i)
      partition(r, p->elems[i]);
}
void topics(AdminResponse &out, const rd_kafka_topic_result_t **p, size_t n) {
  for (size_t i = 0; i < n; ++i) {
    AdminResource r{};
    r.name = str(rd_kafka_topic_result_name(p[i]));
    r.code = rd_kafka_topic_result_error(p[i]);
    r.message = str(rd_kafka_topic_result_error_string(p[i]));
    out.resources.push_back(std::move(r));
  }
}
void configs(AdminResponse &out, const rd_kafka_ConfigResource_t **p,
             size_t n) {
  for (size_t i = 0; i < n; ++i) {
    AdminResource r{};
    r.name = str(rd_kafka_ConfigResource_name(p[i]));
    r.resource_type = rd_kafka_ConfigResource_type(p[i]);
    r.code = rd_kafka_ConfigResource_error(p[i]);
    r.message = str(rd_kafka_ConfigResource_error_string(p[i]));
    size_t count = 0;
    auto entries = rd_kafka_ConfigResource_configs(p[i], &count);
    for (size_t j = 0; j < count; ++j) {
      AdminConfig cfg{};
      cfg.name = str(rd_kafka_ConfigEntry_name(entries[j]));
      auto value = rd_kafka_ConfigEntry_value(entries[j]);
      cfg.value = str(value);
      cfg.has_value = value != nullptr;
      cfg.source = rd_kafka_ConfigEntry_source(entries[j]);
      cfg.read_only = rd_kafka_ConfigEntry_is_read_only(entries[j]);
      cfg.sensitive = rd_kafka_ConfigEntry_is_sensitive(entries[j]);
      r.configs.push_back(std::move(cfg));
    }
    out.resources.push_back(std::move(r));
  }
}
void groups(AdminResponse &out, const rd_kafka_group_result_t **p, size_t n) {
  for (size_t i = 0; i < n; ++i) {
    AdminResource r{};
    r.name = str(rd_kafka_group_result_name(p[i]));
    error(r, rd_kafka_group_result_error(p[i]));
    partitions(r, rd_kafka_group_result_partitions(p[i]));
    out.resources.push_back(std::move(r));
  }
}
AdminResource acl(const rd_kafka_AclBinding_t *p) {
  AdminResource r{};
  r.name = str(rd_kafka_AclBinding_name(p));
  r.resource_type = rd_kafka_AclBinding_restype(p);
  r.principal = str(rd_kafka_AclBinding_principal(p));
  r.host = str(rd_kafka_AclBinding_host(p));
  r.pattern = rd_kafka_AclBinding_resource_pattern_type(p);
  r.operation = rd_kafka_AclBinding_operation(p);
  r.permission = rd_kafka_AclBinding_permission_type(p);
  error(r, rd_kafka_AclBinding_error(p));
  return r;
}
void acls(AdminResponse &out, const rd_kafka_AclBinding_t **p, size_t n) {
  for (size_t i = 0; i < n; ++i)
    out.resources.push_back(acl(p[i]));
}
} // namespace
AdminResponse admin_result(rd_kafka_event_t *event, int32_t op) {
  AdminResponse out{};
  out.code = rd_kafka_event_error(event);
  out.message = str(rd_kafka_event_error_string(event));
  if (out.code)
    return out;
  size_t n = 0;
  switch (op) {
  case 0: {
    auto p = rd_kafka_CreateTopics_result_topics(
        rd_kafka_event_CreateTopics_result(event), &n);
    topics(out, p, n);
    break;
  }
  case 1: {
    auto p = rd_kafka_DeleteTopics_result_topics(
        rd_kafka_event_DeleteTopics_result(event), &n);
    topics(out, p, n);
    break;
  }
  case 2: {
    auto p = rd_kafka_CreatePartitions_result_topics(
        rd_kafka_event_CreatePartitions_result(event), &n);
    topics(out, p, n);
    break;
  }
  case 3: {
    auto p = rd_kafka_DescribeConfigs_result_resources(
        rd_kafka_event_DescribeConfigs_result(event), &n);
    configs(out, p, n);
    break;
  }
  case 4: {
    auto p = rd_kafka_AlterConfigs_result_resources(
        rd_kafka_event_AlterConfigs_result(event), &n);
    configs(out, p, n);
    break;
  }
  case 5: {
    auto p = rd_kafka_IncrementalAlterConfigs_result_resources(
        rd_kafka_event_IncrementalAlterConfigs_result(event), &n);
    configs(out, p, n);
    break;
  }
  case 6: {
    AdminResource r{};
    partitions(r, rd_kafka_DeleteRecords_result_offsets(
                      rd_kafka_event_DeleteRecords_result(event)));
    out.resources.push_back(std::move(r));
    break;
  }
  case 7: {
    auto result = rd_kafka_event_ListConsumerGroups_result(event);
    auto p = rd_kafka_ListConsumerGroups_result_valid(result, &n);
    for (size_t i = 0; i < n; ++i) {
      AdminResource r{};
      r.name = str(rd_kafka_ConsumerGroupListing_group_id(p[i]));
      r.state = rd_kafka_ConsumerGroupListing_state(p[i]);
      r.simple = rd_kafka_ConsumerGroupListing_is_simple_consumer_group(p[i]);
      out.resources.push_back(std::move(r));
    }
    auto e = rd_kafka_ListConsumerGroups_result_errors(result, &n);
    for (size_t i = 0; i < n; ++i) {
      AdminResource r{};
      error(r, e[i]);
      out.resources.push_back(std::move(r));
    }
    break;
  }
  case 8: {
    auto p = rd_kafka_DescribeConsumerGroups_result_groups(
        rd_kafka_event_DescribeConsumerGroups_result(event), &n);
    for (size_t i = 0; i < n; ++i) {
      AdminResource r{};
      r.name = str(rd_kafka_ConsumerGroupDescription_group_id(p[i]));
      error(r, rd_kafka_ConsumerGroupDescription_error(p[i]));
      r.state = rd_kafka_ConsumerGroupDescription_state(p[i]);
      r.member_count = rd_kafka_ConsumerGroupDescription_member_count(p[i]);
      r.simple =
          rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(p[i]);
      r.assignor =
          str(rd_kafka_ConsumerGroupDescription_partition_assignor(p[i]));
      const auto *node = rd_kafka_ConsumerGroupDescription_coordinator(p[i]);
      if (node) {
        r.broker_id = rd_kafka_Node_id(node);
        r.host = str(rd_kafka_Node_host(node));
        r.port = rd_kafka_Node_port(node);
      }
      for (size_t j = 0; j < r.member_count; ++j) {
        auto member = rd_kafka_ConsumerGroupDescription_member(p[i], j);
        auto assignment = rd_kafka_MemberDescription_assignment(member);
        if (assignment)
          partitions(r, rd_kafka_MemberAssignment_partitions(assignment));
      }
      out.resources.push_back(std::move(r));
    }
    break;
  }
  case 9: {
    auto p = rd_kafka_DeleteGroups_result_groups(
        rd_kafka_event_DeleteGroups_result(event), &n);
    groups(out, p, n);
    break;
  }
  case 10: {
    auto p = rd_kafka_DeleteConsumerGroupOffsets_result_groups(
        rd_kafka_event_DeleteConsumerGroupOffsets_result(event), &n);
    groups(out, p, n);
    break;
  }
  case 11: {
    auto p = rd_kafka_ListOffsets_result_infos(
        rd_kafka_event_ListOffsets_result(event), &n);
    for (size_t i = 0; i < n; ++i) {
      AdminResource r{};
      auto tp = rd_kafka_ListOffsetsResultInfo_topic_partition(p[i]);
      r.name = str(tp->topic);
      partition(r, *tp);
      r.partitions[0].timestamp =
          rd_kafka_ListOffsetsResultInfo_timestamp(p[i]);
      out.resources.push_back(std::move(r));
    }
    break;
  }
  case 12: {
    auto p = rd_kafka_DescribeTopics_result_topics(
        rd_kafka_event_DescribeTopics_result(event), &n);
    for (size_t i = 0; i < n; ++i) {
      AdminResource r{};
      r.name = str(rd_kafka_TopicDescription_name(p[i]));
      error(r, rd_kafka_TopicDescription_error(p[i]));
      r.internal = rd_kafka_TopicDescription_is_internal(p[i]);
      size_t count = 0;
      auto ps = rd_kafka_TopicDescription_partitions(p[i], &count);
      for (size_t j = 0; j < count; ++j) {
        AdminPartition v{};
        v.topic = r.name;
        v.partition = rd_kafka_TopicPartitionInfo_partition(ps[j]);
        auto leader = rd_kafka_TopicPartitionInfo_leader(ps[j]);
        v.leader = leader ? rd_kafka_Node_id(leader) : -1;
        size_t nn = 0;
        auto replicas = rd_kafka_TopicPartitionInfo_replicas(ps[j], &nn);
        for (size_t k = 0; k < nn; ++k)
          v.replicas.push_back(rd_kafka_Node_id(replicas[k]));
        auto isrs = rd_kafka_TopicPartitionInfo_isr(ps[j], &nn);
        for (size_t k = 0; k < nn; ++k)
          v.isr.push_back(rd_kafka_Node_id(isrs[k]));
        r.partitions.push_back(std::move(v));
      }
      out.resources.push_back(std::move(r));
    }
    break;
  }
  case 13: {
    auto result = rd_kafka_event_DescribeCluster_result(event);
    AdminResource cluster{};
    cluster.name = str(rd_kafka_DescribeCluster_result_cluster_id(result));
    auto controller = rd_kafka_DescribeCluster_result_controller(result);
    cluster.broker_id = controller ? rd_kafka_Node_id(controller) : -1;
    out.resources.push_back(std::move(cluster));
    auto p = rd_kafka_DescribeCluster_result_nodes(result, &n);
    for (size_t i = 0; i < n; ++i) {
      AdminResource r{};
      r.broker_id = rd_kafka_Node_id(p[i]);
      r.host = str(rd_kafka_Node_host(p[i]));
      r.port = rd_kafka_Node_port(p[i]);
      out.resources.push_back(std::move(r));
    }
    break;
  }
  case 14: {
    auto p = rd_kafka_CreateAcls_result_acls(
        rd_kafka_event_CreateAcls_result(event), &n);
    for (size_t i = 0; i < n; ++i) {
      AdminResource r{};
      error(r, rd_kafka_acl_result_error(p[i]));
      out.resources.push_back(std::move(r));
    }
    break;
  }
  case 15: {
    auto p = rd_kafka_DescribeAcls_result_acls(
        rd_kafka_event_DescribeAcls_result(event), &n);
    acls(out, p, n);
    break;
  }
  case 16: {
    auto p = rd_kafka_DeleteAcls_result_responses(
        rd_kafka_event_DeleteAcls_result(event), &n);
    for (size_t i = 0; i < n; ++i) {
      AdminResource r{};
      r.name = std::to_string(i);
      error(r, rd_kafka_DeleteAcls_result_response_error(p[i]));
      out.resources.push_back(std::move(r));
      size_t count = 0;
      auto bindings =
          rd_kafka_DeleteAcls_result_response_matching_acls(p[i], &count);
      acls(out, bindings, count);
    }
    break;
  }
  default:
    out.code = RD_KAFKA_RESP_ERR__INVALID_ARG;
    out.message = "unknown admin result operation";
  }
  return out;
}
} // namespace kafka_bridge

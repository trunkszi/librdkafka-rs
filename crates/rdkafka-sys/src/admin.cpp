#include "rdkafka-sys/src/admin.h"
#include "rdkafka-sys/src/admin.rs.h"
#include <deque>
#include <functional>
#include <stdexcept>
namespace kafka_bridge {
namespace {
struct Cleanup {
  std::deque<std::string> strings;
  const char *text(const rust::String &value) {
    strings.emplace_back(value);
    if (strings.back().find('\0') != std::string::npos)
      throw std::invalid_argument(
          "administrative text contains an embedded NUL");
    return strings.back().c_str();
  }
  std::vector<std::function<void()>> jobs;
  ~Cleanup() {
    for (auto i = jobs.rbegin(); i != jobs.rend(); ++i)
      (*i)();
  }
  template <class T, class F> T *own(T *p, F destroy) {
    if (!p)
      throw std::runtime_error("native admin request allocation failed");
    jobs.emplace_back([p, destroy] { destroy(p); });
    return p;
  }
};
void check(rd_kafka_resp_err_t code, const char *message) {
  if (code)
    throw std::runtime_error(message ? message : rd_kafka_err2str(code));
}
void check_error(rd_kafka_error_t *error) {
  if (!error)
    return;
  std::string message = rd_kafka_error_string(error);
  rd_kafka_error_destroy(error);
  throw std::runtime_error(message);
}
rd_kafka_topic_partition_list_t *parts(const AdminRequest &request,
                                       Cleanup &c) {
  auto *p = c.own(rd_kafka_topic_partition_list_new(request.partitions.size()),
                  rd_kafka_topic_partition_list_destroy);
  for (const auto &v : request.partitions)
    rd_kafka_topic_partition_list_add(p, c.text(v.topic), v.partition)->offset =
        v.offset;
  return p;
}
} // namespace
AdminResponse admin_request(const Client &client, int32_t op,
                            rust::Slice<const AdminRequest> requests,
                            int32_t timeout) {
  if (timeout < 0)
    throw std::invalid_argument("admin timeout must be nonnegative");
  if (op != 7 && op != 13 && requests.empty())
    throw std::invalid_argument("admin request requires resources");
  Cleanup c;
  char err[512] = {};
  auto *q = c.own(rd_kafka_queue_new(client.raw()), rd_kafka_queue_destroy);
  auto *options =
      c.own(rd_kafka_AdminOptions_new(client.raw(), RD_KAFKA_ADMIN_OP_ANY),
            rd_kafka_AdminOptions_destroy);
  check(rd_kafka_AdminOptions_set_request_timeout(options, timeout, err,
                                                  sizeof(err)),
        err);
  if (op == 0) {
    std::vector<rd_kafka_NewTopic_t *> v;
    for (const auto &r : requests) {
      auto *t = rd_kafka_NewTopic_new(c.text(r.name), r.count, r.replication,
                                      err, sizeof(err));
      if (!t)
        throw std::invalid_argument(err);
      c.own(t, rd_kafka_NewTopic_destroy);
      for (const auto &cfg : r.configs)
        check(rd_kafka_NewTopic_set_config(t, c.text(cfg.name),
                                           cfg.has_value ? c.text(cfg.value)
                                                         : nullptr),
              "invalid topic configuration");
      v.push_back(t);
    }
    rd_kafka_CreateTopics(client.raw(), v.data(), v.size(), options, q);
  } else if (op == 1) {
    std::vector<rd_kafka_DeleteTopic_t *> v;
    for (const auto &r : requests)
      v.push_back(c.own(rd_kafka_DeleteTopic_new(c.text(r.name)),
                        rd_kafka_DeleteTopic_destroy));
    rd_kafka_DeleteTopics(client.raw(), v.data(), v.size(), options, q);
  } else if (op == 2) {
    std::vector<rd_kafka_NewPartitions_t *> v;
    for (const auto &r : requests) {
      auto *p =
          rd_kafka_NewPartitions_new(c.text(r.name), r.count, err, sizeof(err));
      if (!p)
        throw std::invalid_argument(err);
      v.push_back(c.own(p, rd_kafka_NewPartitions_destroy));
    }
    rd_kafka_CreatePartitions(client.raw(), v.data(), v.size(), options, q);
  } else if (op >= 3 && op <= 5) {
    std::vector<rd_kafka_ConfigResource_t *> v;
    for (const auto &r : requests) {
      auto *p = c.own(rd_kafka_ConfigResource_new(
                          static_cast<rd_kafka_ResourceType_t>(r.resource_type),
                          c.text(r.name)),
                      rd_kafka_ConfigResource_destroy);
      for (const auto &cfg : r.configs) {
        if (op == 5)
          check_error(rd_kafka_ConfigResource_add_incremental_config(
              p, c.text(cfg.name),
              static_cast<rd_kafka_AlterConfigOpType_t>(cfg.operation),
              cfg.has_value ? c.text(cfg.value) : nullptr));
        else
          check(rd_kafka_ConfigResource_set_config(
                    p, c.text(cfg.name),
                    cfg.has_value ? c.text(cfg.value) : nullptr),
                "invalid resource configuration");
      }
      v.push_back(p);
    }
    if (op == 3)
      rd_kafka_DescribeConfigs(client.raw(), v.data(), v.size(), options, q);
    if (op == 4)
      rd_kafka_AlterConfigs(client.raw(), v.data(), v.size(), options, q);
    if (op == 5)
      rd_kafka_IncrementalAlterConfigs(client.raw(), v.data(), v.size(),
                                       options, q);
  } else if (op == 6) {
    auto *p = parts(requests[0], c);
    auto *r =
        c.own(rd_kafka_DeleteRecords_new(p), rd_kafka_DeleteRecords_destroy);
    rd_kafka_DeleteRecords(client.raw(), &r, 1, options, q);
  } else if (op == 7) {
    rd_kafka_ListConsumerGroups(client.raw(), options, q);
  } else if (op == 8) {
    std::vector<const char *> names;
    for (const auto &r : requests)
      names.push_back(c.text(r.name));
    rd_kafka_DescribeConsumerGroups(client.raw(), names.data(), names.size(),
                                    options, q);
  } else if (op == 9) {
    std::vector<rd_kafka_DeleteGroup_t *> v;
    for (const auto &r : requests)
      v.push_back(c.own(rd_kafka_DeleteGroup_new(c.text(r.name)),
                        rd_kafka_DeleteGroup_destroy));
    rd_kafka_DeleteGroups(client.raw(), v.data(), v.size(), options, q);
  } else if (op == 10) {
    std::vector<rd_kafka_DeleteConsumerGroupOffsets_t *> v;
    for (const auto &r : requests)
      v.push_back(c.own(
          rd_kafka_DeleteConsumerGroupOffsets_new(c.text(r.name), parts(r, c)),
          rd_kafka_DeleteConsumerGroupOffsets_destroy));
    rd_kafka_DeleteConsumerGroupOffsets(client.raw(), v.data(), v.size(),
                                        options, q);
  } else if (op == 11) {
    rd_kafka_ListOffsets(client.raw(), parts(requests[0], c), options, q);
  } else if (op == 12) {
    std::vector<const char *> names;
    for (const auto &r : requests)
      names.push_back(c.text(r.name));
    auto *p = c.own(
        rd_kafka_TopicCollection_of_topic_names(names.data(), names.size()),
        rd_kafka_TopicCollection_destroy);
    rd_kafka_DescribeTopics(client.raw(), p, options, q);
  } else if (op == 13) {
    rd_kafka_DescribeCluster(client.raw(), options, q);
  } else if (op >= 14 && op <= 16) {
    std::vector<rd_kafka_AclBinding_t *> v;
    for (const auto &r : requests) {
      auto make =
          op == 14 ? rd_kafka_AclBinding_new : rd_kafka_AclBindingFilter_new;
      auto str = [&c](const rust::String &s) {
        return s.empty() ? nullptr : c.text(s);
      };
      auto *p = make(static_cast<rd_kafka_ResourceType_t>(r.resource_type),
                     str(r.name),
                     static_cast<rd_kafka_ResourcePatternType_t>(r.pattern),
                     str(r.principal), str(r.host),
                     static_cast<rd_kafka_AclOperation_t>(r.operation),
                     static_cast<rd_kafka_AclPermissionType_t>(r.permission),
                     err, sizeof(err));
      if (!p)
        throw std::invalid_argument(err);
      v.push_back(c.own(p, rd_kafka_AclBinding_destroy));
    }
    if (op == 14)
      rd_kafka_CreateAcls(client.raw(), v.data(), v.size(), options, q);
    if (op == 15)
      rd_kafka_DescribeAcls(client.raw(), v[0], options, q);
    if (op == 16)
      rd_kafka_DeleteAcls(client.raw(), v.data(), v.size(), options, q);
  } else
    throw std::invalid_argument("unknown native admin operation");
  auto *event = rd_kafka_queue_poll(q, timeout);
  if (!event) {
    AdminResponse out{};
    out.code = RD_KAFKA_RESP_ERR__TIMED_OUT;
    out.message = "administrative request timed out";
    return out;
  }
  c.own(event, rd_kafka_event_destroy);
  auto result = admin_result(event, op);
  if (op == 14) {
    for (size_t i = 0; i < result.resources.size() && i < requests.size();
         ++i) {
      auto &r = result.resources[i];
      const auto &a = requests[i];
      r.name = a.name;
      r.resource_type = a.resource_type;
      r.principal = a.principal;
      r.host = a.host;
      r.pattern = a.pattern;
      r.operation = a.operation;
      r.permission = a.permission;
    }
  }
  return result;
}
} // namespace kafka_bridge

#include "rdkafka-sys/src/lib.rs.h"
#include "src/shim.h"
#include <stdexcept>
#include <cstdlib>
#include <cstring>
namespace kafka_bridge {
static void checked(rd_kafka_resp_err_t e) {
  if (e)
    throw std::runtime_error(std::to_string(e) + ":" + rd_kafka_err2str(e));
}
static int32_t produce_impl(Client &c, const Topic *handle, rust::Str topic, int32_t partition,
                rust::Slice<const uint8_t> key, bool has_key,
                rust::Slice<const uint8_t> payload, bool has_payload,
                rust::Slice<const NativeHeader> headers, int64_t timestamp,
                uint64_t token) {
  std::string name(topic);
  if (c.kind != 0)
    return RD_KAFKA_RESP_ERR__INVALID_ARG;
  auto *h = rd_kafka_headers_new(headers.size());
  for (const auto &x : headers) {
    auto e = rd_kafka_header_add(h, x.name.data(), x.name.size(),
                                 x.has_value ? x.value.data() : nullptr,
                                 x.has_value ? x.value.size() : 0);
    if (e) {
      rd_kafka_headers_destroy(h);
      return e;
    }
  }
  static uint8_t empty = 0;
  auto *data =
      has_payload
          ? (payload.empty() ? &empty : const_cast<uint8_t *>(payload.data()))
          : nullptr;
  const void *k = has_key ? (key.empty() ? &empty : key.data()) : nullptr;
  auto e = handle ? rd_kafka_producev(
      c.raw(), RD_KAFKA_V_RKT(handle->value), RD_KAFKA_V_PARTITION(partition),
      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_KEY(k, key.size()),
      RD_KAFKA_V_VALUE(data, payload.size()), RD_KAFKA_V_HEADERS(h),
      RD_KAFKA_V_TIMESTAMP(timestamp),
      RD_KAFKA_V_OPAQUE(reinterpret_cast<void *>(static_cast<uintptr_t>(token))),
      RD_KAFKA_V_END) : rd_kafka_producev(
      c.raw(), RD_KAFKA_V_TOPIC(name.c_str()), RD_KAFKA_V_PARTITION(partition),
      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_KEY(k, key.size()),
      RD_KAFKA_V_VALUE(data, payload.size()), RD_KAFKA_V_HEADERS(h),
      RD_KAFKA_V_TIMESTAMP(timestamp),
      RD_KAFKA_V_OPAQUE(
          reinterpret_cast<void *>(static_cast<uintptr_t>(token))),
      RD_KAFKA_V_END);
  if (e)
    rd_kafka_headers_destroy(h);
  return e;
}
int32_t produce(Client &c, rust::Str topic, int32_t partition,
                rust::Slice<const uint8_t> key, bool has_key,
                rust::Slice<const uint8_t> payload, bool has_payload,
                rust::Slice<const NativeHeader> headers, int64_t timestamp,
                uint64_t token) {
  return produce_impl(c, nullptr, topic, partition, key, has_key, payload,
                      has_payload, headers, timestamp, token);
}
int32_t produce_topic(Client &c, const Topic &topic, int32_t partition,
                      rust::Slice<const uint8_t> key, bool has_key,
                      rust::Slice<const uint8_t> payload, bool has_payload,
                      rust::Slice<const NativeHeader> headers, int64_t timestamp,
                      uint64_t token) {
  if (topic.owner != c.state) return RD_KAFKA_RESP_ERR__INVALID_ARG;
  return produce_impl(c, &topic, rd_kafka_topic_name(topic.value), partition,
                      key, has_key, payload, has_payload, headers, timestamp, token);
}
rust::String client_id(Client &c, int32_t kind, int32_t timeout) {
  char *value = kind == 0 ? rd_kafka_clusterid(c.raw(), timeout)
                          : rd_kafka_memberid(c.raw());
  if (!value)
    return "";
  auto release = [&](char *text) { rd_kafka_mem_free(c.raw(), text); };
  std::unique_ptr<char, decltype(release)> owner(value, release);
  rust::String result(value);
  return result;
}
int32_t client_controller(Client &c, int32_t timeout) {
  return rd_kafka_controllerid(c.raw(), timeout);
}
NativeEvent client_fatal(Client &c) {
  char text[512];
  auto e = rd_kafka_fatal_error(c.raw(), text, sizeof(text));
  return {1, e, rust::String(text), "", -1, -1, 0, {}};
}
NativeMetadata client_metadata(Client &c, rust::Str topic, int32_t timeout) {
  const rd_kafka_metadata_t *md = nullptr;
  std::string name(topic);
  rd_kafka_topic_t *t =
      name.empty() ? nullptr
                   : rd_kafka_topic_new(c.raw(), name.c_str(), nullptr);
  std::unique_ptr<rd_kafka_topic_t, decltype(&rd_kafka_topic_destroy)>
      topic_owner(t, rd_kafka_topic_destroy);
  if (!name.empty() && !t) checked(rd_kafka_last_error());
  auto e = rd_kafka_metadata(c.raw(), name.empty(), t, &md, timeout);
  std::unique_ptr<const rd_kafka_metadata_t, decltype(&rd_kafka_metadata_destroy)>
      metadata_owner(md, rd_kafka_metadata_destroy);
  checked(e);
  if (!md) throw std::runtime_error("native metadata unavailable");
  NativeMetadata out;
  for (int i = 0; i < md->broker_cnt; ++i) {
    out.broker_ids.push_back(md->brokers[i].id);
    out.broker_hosts.push_back(rust::String(md->brokers[i].host));
    out.broker_ports.push_back(md->brokers[i].port);
  }
  for (int i = 0; i < md->topic_cnt; ++i) {
    auto &x = md->topics[i];
    NativeTopicMetadata topic{
        rust::String(x.topic), x.err, x.partition_cnt, {}};
    for (int j = 0; j < x.partition_cnt; ++j) {
      auto &p = x.partitions[j];
      NativePartitionMetadata part{p.id, p.err, p.leader, {}, {}};
      for (int k = 0; k < p.replica_cnt; ++k)
        part.replicas.push_back(p.replicas[k]);
      for (int k = 0; k < p.isr_cnt; ++k)
        part.isr.push_back(p.isrs[k]);
      topic.partition_metadata.push_back(std::move(part));
    }
    out.topics.push_back(std::move(topic));
  }
  return out;
}
rust::Vec<int64_t> client_watermarks(Client &c, rust::Str topic,
                                     int32_t partition, int32_t timeout,
                                     bool cached) {
  int64_t lo = 0, hi = 0;
  std::string name(topic);
  checked(cached ? rd_kafka_get_watermark_offsets(c.raw(), name.c_str(),
                                                  partition, &lo, &hi)
                 : rd_kafka_query_watermark_offsets(
                       c.raw(), name.c_str(), partition, &lo, &hi, timeout));
  rust::Vec<int64_t> out;
  out.push_back(lo);
  out.push_back(hi);
  return out;
}
NativePartition partition_from(const rd_kafka_topic_partition_t &p) {
  NativePartition out{rust::String(p.topic), p.partition, p.offset, p.err,
                      rd_kafka_topic_partition_get_leader_epoch(&p), {}};
  auto *data = static_cast<const uint8_t *>(p.metadata);
  for (size_t i = 0; i < p.metadata_size; ++i) out.metadata.push_back(data[i]);
  return out;
}
void partition_to(rd_kafka_topic_partition_t &item, const NativePartition &p) {
  item.offset = p.offset;
  rd_kafka_topic_partition_set_leader_epoch(&item, p.leader_epoch);
  if (!p.metadata.empty()) {
    item.metadata = std::malloc(p.metadata.size());
    if (!item.metadata) throw std::bad_alloc();
    std::memcpy(item.metadata, p.metadata.data(), p.metadata.size());
    item.metadata_size = p.metadata.size();
  }
}
rust::Vec<NativePartition>
client_partitions(Client &c, int32_t op,
                  rust::Slice<const NativePartition> input, int32_t timeout) {
  auto *list = rd_kafka_topic_partition_list_new(input.size());
  std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)>
      owner(list, rd_kafka_topic_partition_list_destroy);
  if (!list) throw std::bad_alloc();
  for (const auto &p : input) {
    std::string t(p.topic);
    auto *item =
        rd_kafka_topic_partition_list_add(list, t.c_str(), p.partition);
    partition_to(*item, p);
  }
  rd_kafka_resp_err_t e = RD_KAFKA_RESP_ERR_NO_ERROR;
  std::string rich_error;
  switch (op) {
  case 0:
    e = rd_kafka_assign(c.raw(), list);
    if (!e) c.state->callbacks.rebalance_pending = false;
    break;
  case 1:
    e = rd_kafka_assign(c.raw(), nullptr);
    if (!e) c.state->callbacks.rebalance_pending = false;
    break;
  case 2:
    e = rd_kafka_commit(c.raw(), input.empty() ? nullptr : list, 0);
    break;
  case 3:
    e = rd_kafka_commit(c.raw(), input.empty() ? nullptr : list, 1);
    break;
  case 4:
    e = rd_kafka_offsets_store(c.raw(), list);
    break;
  case 5:
    e = rd_kafka_committed(c.raw(), list, timeout);
    break;
  case 6:
    e = rd_kafka_position(c.raw(), list);
    break;
  case 7:
    e = rd_kafka_offsets_for_times(c.raw(), list, timeout);
    break;
  case 8:
    e = rd_kafka_pause_partitions(c.raw(), list);
    break;
  case 9:
    e = rd_kafka_resume_partitions(c.raw(), list);
    break;
  case 10: {
    auto *error = rd_kafka_seek_partitions(c.raw(), list, timeout);
    if (error) {
      e = rd_kafka_error_code(error);
      unsigned flags = (rd_kafka_error_is_retriable(error) ? 1 : 0) |
                       (rd_kafka_error_is_fatal(error) ? 2 : 0) |
                       (rd_kafka_error_txn_requires_abort(error) ? 4 : 0);
      rich_error = std::to_string(e) + ":flags=" + std::to_string(flags) + ":" +
                   rd_kafka_error_string(error);
      rd_kafka_error_destroy(error);
    }
    break;
  }
  default:
    e = RD_KAFKA_RESP_ERR__INVALID_ARG;
  }
  rust::Vec<NativePartition> out;
  for (int i = 0; i < list->cnt; ++i) {
    auto &p = list->elems[i];
    out.push_back(partition_from(p));
  }
  if (!rich_error.empty())
    throw std::runtime_error(rich_error);
  checked(e);
  return out;
}
Topic::~Topic() {
  if (value)
    rd_kafka_topic_destroy(value);
}
std::unique_ptr<Topic> topic_new(Client &c, rust::Str name, const Conf &conf) {
  if (!conf.topic)
    throw std::runtime_error("topic configuration required");
  auto cfg = std::unique_ptr<RdKafka::Conf>(
      RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
  std::string e;
  for (const auto &p : conf.properties)
    if (cfg->set(p.first, p.second, e) != RdKafka::Conf::CONF_OK)
      throw std::runtime_error(e);
  auto *native = rd_kafka_topic_conf_dup(cfg->c_ptr_topic());
  if (!native) throw std::bad_alloc();
  std::unique_ptr<rd_kafka_topic_conf_t, decltype(&rd_kafka_topic_conf_destroy)>
      native_owner(native, rd_kafka_topic_conf_destroy);
  if (c.state->callbacks.has_partitioner) {
    rd_kafka_topic_conf_set_opaque(native, &c.state->callbacks);
    rd_kafka_topic_conf_set_partitioner_cb(native, partitioner_string);
  }
  auto topic = std::make_unique<Topic>();
  topic->owner = c.state;
  std::string topic_name(name);
  topic->value = rd_kafka_topic_new(c.raw(), topic_name.c_str(), native_owner.release());
  if (!topic->value)
    throw std::runtime_error(std::to_string(rd_kafka_last_error()) + ":" +
                             rd_kafka_err2str(rd_kafka_last_error()));
  return topic;
}
int32_t topic_murmur2_partition(const Topic &t, rust::Slice<const uint8_t> key,
                                 int32_t count) {
  if (count <= 0) return RD_KAFKA_PARTITION_UA;
  return rd_kafka_msg_partitioner_murmur2_random(t.value, key.data(), key.size(),
                                                count, nullptr, nullptr);
}
rust::String topic_name(const Topic &t) { return rd_kafka_topic_name(t.value); }
bool topic_available(const Topic &t, int32_t p) {
  return rd_kafka_topic_partition_available(t.value, p);
}
} // namespace kafka_bridge

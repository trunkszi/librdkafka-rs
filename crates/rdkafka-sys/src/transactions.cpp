#include "rdkafka-sys/src/lib.rs.h"
#include "rdkafka-sys/src/transactions.rs.h"
#include "rdkafka-sys/src/transactions.h"
#include <stdexcept>
namespace kafka_bridge {
GroupMetadata::~GroupMetadata() {
  if (value)
    rd_kafka_consumer_group_metadata_destroy(value);
}
static void checked(rd_kafka_error_t *error) {
  if (!error)
    return;
  int flags = (rd_kafka_error_is_retriable(error) ? 1 : 0) |
              (rd_kafka_error_is_fatal(error) ? 2 : 0) |
              (rd_kafka_error_txn_requires_abort(error) ? 4 : 0);
  std::string message = std::to_string(rd_kafka_error_code(error)) +
                        ":flags=" + std::to_string(flags) + ":" +
                        rd_kafka_error_string(error);
  rd_kafka_error_destroy(error);
  throw std::runtime_error(message);
}
std::unique_ptr<GroupMetadata> group_metadata(const Client &client) {
  auto result = std::make_unique<GroupMetadata>();
  auto *metadata = rd_kafka_consumer_group_metadata(client.raw());
  if (!metadata)
    throw std::runtime_error("-172:consumer group metadata unavailable");
  result->value = metadata;
  return result;
}
void transaction(Client &client, int32_t operation, int32_t timeout) {
  switch (operation) {
  case 0: checked(rd_kafka_init_transactions(client.raw(), timeout)); break;
  case 1: checked(rd_kafka_begin_transaction(client.raw())); break;
  case 2: checked(rd_kafka_commit_transaction(client.raw(), timeout)); break;
  case 3: checked(rd_kafka_abort_transaction(client.raw(), timeout)); break;
  default: throw std::runtime_error("-186:invalid transaction operation");
  }
}
using Partitions = std::unique_ptr<rd_kafka_topic_partition_list_t,
                                  decltype(&rd_kafka_topic_partition_list_destroy)>;
static Partitions partition_list(rust::Slice<const NativePartition> source) {
  Partitions result(rd_kafka_topic_partition_list_new(source.size()),
                    rd_kafka_topic_partition_list_destroy);
  if (!result) throw std::bad_alloc();
  for (const auto &p : source) {
    auto *target = rd_kafka_topic_partition_list_add(result.get(), std::string(p.topic).c_str(),
                                                    p.partition);
    partition_to(*target, p);
  }
  return result;
}
void transaction_offsets(Client &client,
                         rust::Slice<const NativePartition> source,
                         const GroupMetadata &metadata, int32_t timeout) {
  auto parts = partition_list(source);
  checked(rd_kafka_send_offsets_to_transaction(client.raw(), parts.get(),
                                               metadata.value, timeout));
}
int32_t transaction_purge(Client &client, int32_t flags) {
  return rd_kafka_purge(client.raw(), flags);
}
void consumer_incremental(Client &client,
                          rust::Slice<const NativePartition> source,
                          bool assign, bool complete_rebalance) {
  rd_kafka_topic_partition_list_t *current = nullptr;
  auto code = rd_kafka_assignment(client.raw(), &current);
  Partitions owner(current, rd_kafka_topic_partition_list_destroy);
  if (code)
    throw std::runtime_error(std::to_string(code) + ":" + rd_kafka_err2str(code));
  if (!current) throw std::runtime_error("native assignment unavailable");
  auto selected = partition_list(source);
  // Repeated assignment deltas do not add or remove a partition twice.
  for (int i = selected->cnt - 1; i >= 0; --i) {
    const auto &p = selected->elems[i];
    bool present = rd_kafka_topic_partition_list_find(current, p.topic, p.partition);
    if (present == assign)
      rd_kafka_topic_partition_list_del_by_idx(selected.get(), i);
  }
  if (!selected->cnt && !complete_rebalance)
    return;
  checked(assign ? rd_kafka_incremental_assign(client.raw(), selected.get())
                 : rd_kafka_incremental_unassign(client.raw(), selected.get()));
}
bool consumer_assignment_lost(const Client &client) {
  return rd_kafka_assignment_lost(client.raw());
}
rust::String consumer_rebalance_protocol(const Client &client) {
  const char *protocol = rd_kafka_rebalance_protocol(client.raw());
  return protocol ? protocol : "NONE";
}
}

#include "librdkafka-rs-sys/src/lib.rs.h"
#include "librdkafka-rs-sys/src/transactions.h"
#include <stdexcept>
namespace kafka_bridge {
GroupMetadata::~GroupMetadata() {
  if (value)
    rd_kafka_consumer_group_metadata_destroy(value);
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
  case 0: check_native_error(rd_kafka_init_transactions(client.raw(), timeout)); break;
  case 1: check_native_error(rd_kafka_begin_transaction(client.raw())); break;
  case 2: check_native_error(rd_kafka_commit_transaction(client.raw(), timeout)); break;
  case 3: check_native_error(rd_kafka_abort_transaction(client.raw(), timeout)); break;
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
  check_native_error(rd_kafka_send_offsets_to_transaction(client.raw(), parts.get(),
                                               metadata.value, timeout));
}

}

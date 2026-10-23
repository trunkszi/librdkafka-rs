#pragma once
#include "librdkafka-rs-sys/src/shim.h"
namespace kafka_bridge {
struct GroupMetadata {
  rd_kafka_consumer_group_metadata_t *value = nullptr;
  ~GroupMetadata();
};
std::unique_ptr<GroupMetadata> group_metadata(const Client &);
void transaction(Client &, int32_t, int32_t);
void transaction_offsets(Client &, rust::Slice<const NativePartition>,
                         const GroupMetadata &, int32_t);
}

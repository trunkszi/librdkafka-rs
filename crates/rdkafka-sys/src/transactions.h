#pragma once
#include "rdkafka-sys/src/shim.h"
#include "rdkafka-sys/src/lib.rs.h"
namespace kafka_bridge {
struct GroupMetadata {
  rd_kafka_consumer_group_metadata_t *value = nullptr;
  ~GroupMetadata();
};
std::unique_ptr<GroupMetadata> group_metadata(const Client &);
void transaction(Client &, int32_t, int32_t);
void transaction_offsets(Client &, rust::Slice<const NativePartition>,
                         const GroupMetadata &, int32_t);
int32_t transaction_purge(Client &, int32_t);
void consumer_incremental(Client &, rust::Slice<const NativePartition>, bool, bool);
bool consumer_assignment_lost(const Client &);
rust::String consumer_rebalance_protocol(const Client &);
}

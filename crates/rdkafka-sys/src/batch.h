#pragma once
#include "rdkafka-sys/src/shim.h"
#include "rdkafka-sys/src/lib.rs.h"
namespace kafka_bridge {
struct BatchRecord;
struct BatchMessages {
  std::vector<std::unique_ptr<Message>> entries;
};
rust::Vec<int32_t> produce_records(Client &, rust::Slice<const BatchRecord>);
std::unique_ptr<BatchMessages> consume_records(Client &, size_t, int32_t);
size_t batch_count(const BatchMessages &);
std::unique_ptr<Message> batch_take(BatchMessages &, size_t);
} // namespace kafka_bridge

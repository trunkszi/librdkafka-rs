#include "rdkafka-sys/src/batch.h"
#include "rdkafka-sys/src/batch.rs.h"
#include "rdkafka-sys/src/lib.rs.h"
#include <limits>
#include <stdexcept>
namespace kafka_bridge {
rust::Vec<int32_t> produce_records(Client &client,
                                   rust::Slice<const BatchRecord> records) {
  rust::Vec<int32_t> results;
  for (const auto &record : records) {
    if (client.kind != 0) { results.push_back(RD_KAFKA_RESP_ERR__INVALID_ARG); continue; }
    std::string topic(record.topic);
    auto *headers = rd_kafka_headers_new(record.headers.size());
    rd_kafka_resp_err_t header_error = RD_KAFKA_RESP_ERR_NO_ERROR;
    for (const auto &header : record.headers) {
      header_error = rd_kafka_header_add(
          headers, header.name.data(), header.name.size(),
          header.has_value ? static_cast<const void *>(header.value.data()) : nullptr,
          header.value.size());
      if (header_error) break;
    }
    if (header_error) {
      rd_kafka_headers_destroy(headers);
      results.push_back(header_error);
      continue;
    }
    // produceva is one message per call; the Rust/native transition batches all
    // descriptors while preserving independent enqueue results and ownership.
    rd_kafka_vu_t args[7]{};
    args[0].vtype = RD_KAFKA_VTYPE_TOPIC;
    args[0].u.cstr = topic.c_str();
    args[1].vtype = RD_KAFKA_VTYPE_PARTITION;
    args[1].u.i32 = record.partition;
    args[2].vtype = RD_KAFKA_VTYPE_MSGFLAGS;
    args[2].u.i = RD_KAFKA_MSG_F_COPY;
    args[3].vtype = RD_KAFKA_VTYPE_KEY;
    args[3].u.mem.ptr = record.has_key ? const_cast<uint8_t *>(record.key.data()) : nullptr;
    args[3].u.mem.size = record.key.size();
    args[4].vtype = RD_KAFKA_VTYPE_VALUE;
    args[4].u.mem.ptr = record.has_payload ? const_cast<uint8_t *>(record.payload.data()) : nullptr;
    args[4].u.mem.size = record.payload.size();
    args[5].vtype = RD_KAFKA_VTYPE_TIMESTAMP;
    args[5].u.i64 = record.timestamp;
    args[6].vtype = RD_KAFKA_VTYPE_HEADERS;
    args[6].u.headers = headers;
    auto *error = rd_kafka_produceva(client.raw(), args, 7);
    auto code = error ? rd_kafka_error_code(error) : RD_KAFKA_RESP_ERR_NO_ERROR;
    if (error) {
      rd_kafka_headers_destroy(headers);
      rd_kafka_error_destroy(error);
    }
    results.push_back(code);
  }
  return results;
}
std::unique_ptr<BatchMessages> consume_records(Client &client, size_t maximum,
                                               int32_t timeout) {
  if (client.kind != 1) throw std::runtime_error("batch receive requires a group consumer");
  if (maximum > static_cast<size_t>(std::numeric_limits<int>::max()))
    throw std::runtime_error("batch size exceeds native receive limit");
  auto batch = std::make_unique<BatchMessages>();
  if (maximum == 0) return batch;
  auto *queue = rd_kafka_queue_get_consumer(client.raw());
  if (!queue) throw std::runtime_error("consumer group queue unavailable");
  std::unique_ptr<rd_kafka_queue_t, decltype(&rd_kafka_queue_destroy)> owner(
      queue, rd_kafka_queue_destroy);
  struct RawMessages {
    std::vector<rd_kafka_message_t *> values;
    ~RawMessages() { for (auto *message : values) if (message) rd_kafka_message_destroy(message); }
  } raw{std::vector<rd_kafka_message_t *>(maximum)};
  batch->entries.reserve(maximum);
  auto count = rd_kafka_consume_batch_queue(queue, timeout, raw.values.data(), maximum);
  if (count < 0) throw std::runtime_error(rd_kafka_err2str(rd_kafka_last_error()));
  for (decltype(count) i = 0; i < count; ++i) {
    auto message = std::make_unique<Message>();
    message->owner = client.state;
    message->value = raw.values[i];
    raw.values[i] = nullptr;
    batch->entries.push_back(std::move(message));
  }
  return batch;
}
size_t batch_count(const BatchMessages &messages) { return messages.entries.size(); }
std::unique_ptr<Message> batch_take(BatchMessages &messages, size_t index) {
  if (index >= messages.entries.size()) return nullptr;
  return std::move(messages.entries[index]);
}
} // namespace kafka_bridge

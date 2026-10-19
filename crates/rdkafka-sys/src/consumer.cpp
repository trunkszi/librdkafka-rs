#include "rdkafka-sys/src/lib.rs.h"
#include "src/shim.h"
#include <stdexcept>
namespace kafka_bridge {
Message::~Message() {
  if (value)
    rd_kafka_message_destroy(value);
}
Queue::~Queue() {
  if (value)
    rd_kafka_queue_destroy(value);
}
static std::unique_ptr<Message> wrap(rd_kafka_message_t *p,
                                     std::shared_ptr<State> owner) {
  if (!p)
    return nullptr;
  auto out = std::make_unique<Message>();
  out->owner = std::move(owner);
  out->value = p;
  return out;
}
int32_t subscribe(Client &c, rust::Slice<const rust::String> names) {
  auto *list = rd_kafka_topic_partition_list_new(names.size());
  for (const auto &n : names) {
    std::string name(n);
    rd_kafka_topic_partition_list_add(list, name.c_str(),
                                      RD_KAFKA_PARTITION_UA);
  }
  auto e = rd_kafka_subscribe(c.raw(), list);
  rd_kafka_topic_partition_list_destroy(list);
  return e;
}
int32_t unsubscribe(Client &c) { return rd_kafka_unsubscribe(c.raw()); }
rust::Vec<NativePartition> consumer_list(Client &c, bool assignment) {
  rd_kafka_topic_partition_list_t *list = nullptr;
  auto e = assignment ? rd_kafka_assignment(c.raw(), &list)
                      : rd_kafka_subscription(c.raw(), &list);
  if (e)
    throw std::runtime_error(std::to_string(e) + ":" + rd_kafka_err2str(e));
  rust::Vec<NativePartition> out;
  for (int i = 0; i < list->cnt; ++i) {
    auto &p = list->elems[i];
    out.push_back({rust::String(p.topic), p.partition, p.offset, p.err});
  }
  rd_kafka_topic_partition_list_destroy(list);
  return out;
}
std::unique_ptr<Message> consume(Client &c, int32_t timeout) {
  return wrap(rd_kafka_consumer_poll(c.raw(), timeout), c.state);
}
int32_t consumer_close(Client &c) { return rd_kafka_consumer_close(c.raw()); }
int32_t simple_start(Client &, const Topic &t, int32_t p, int64_t o) {
  return rd_kafka_consume_start(t.value, p, o) == 0 ? 0 : rd_kafka_last_error();
}
int32_t simple_stop(Client &, const Topic &t, int32_t p) {
  return rd_kafka_consume_stop(t.value, p) == 0 ? 0 : rd_kafka_last_error();
}
int32_t simple_seek(Client &, const Topic &t, int32_t p, int64_t o,
                    int32_t timeout) {
  return rd_kafka_seek(t.value, p, o, timeout);
}
std::unique_ptr<Message> simple_consume(Client &c, const Topic &t, int32_t p,
                                        int32_t timeout) {
  return wrap(rd_kafka_consume(t.value, p, timeout), c.state);
}
std::unique_ptr<Queue> queue_new(Client &c, rust::Str topic,
                                 int32_t partition) {
  auto q = std::make_unique<Queue>();
  q->owner = c.state;
  std::string name(topic);
  q->value = name.empty() ? rd_kafka_queue_new(c.raw())
                          : rd_kafka_queue_get_partition(c.raw(), name.c_str(),
                                                         partition);
  if (!q->value)
    throw std::runtime_error("partition queue unavailable");
  return q;
}
void queue_forward(Queue &q, const Queue &d) {
  rd_kafka_queue_forward(q.value, d.value);
  q.forwarded_owner = d.owner;
}
void queue_unforward(Queue &q) { rd_kafka_queue_forward(q.value, nullptr); }
int32_t queue_poll(Queue &q, int32_t timeout) {
  return rd_kafka_queue_poll_callback(q.value, timeout);
}
std::unique_ptr<Message> queue_consume(Queue &q, int32_t timeout) {
  return wrap(rd_kafka_consume_queue(q.value, timeout), q.owner);
}
void queue_io_event(Queue &q, int32_t fd, rust::Slice<const uint8_t> payload) {
  rd_kafka_queue_io_event_enable(q.value, fd, payload.data(), payload.size());
}
int32_t simple_start_queue(Client &, const Topic &t, int32_t p, int64_t o,
                           const Queue &q) {
  return rd_kafka_consume_start_queue(t.value, p, o, q.value) == 0
             ? 0
             : rd_kafka_last_error();
}
int32_t message_error(const Message &m) { return m.value->err; }
rust::String message_topic(const Message &m) {
  return m.value->rkt ? rd_kafka_topic_name(m.value->rkt) : "";
}
int32_t message_partition(const Message &m) { return m.value->partition; }
int64_t message_offset(const Message &m) { return m.value->offset; }
int64_t message_timestamp(const Message &m) {
  return rd_kafka_message_timestamp(m.value, nullptr);
}
bool message_has_key(const Message &m) { return m.value->key != nullptr; }
rust::Slice<const uint8_t> message_key(const Message &m) {
  return {static_cast<const uint8_t *>(m.value->key), m.value->key_len};
}
bool message_has_payload(const Message &m) {
  return m.value->payload != nullptr;
}
rust::Slice<const uint8_t> message_payload(const Message &m) {
  return {static_cast<const uint8_t *>(m.value->payload), m.value->len};
}
rust::Vec<NativeHeader> message_headers(const Message &m) {
  rust::Vec<NativeHeader> out;
  rd_kafka_headers_t *h = nullptr;
  if (rd_kafka_message_headers(m.value, &h))
    return out;
  for (size_t i = 0;; ++i) {
    const char *name;
    const void *value;
    size_t size;
    if (rd_kafka_header_get_all(h, i, &name, &value, &size))
      break;
    NativeHeader x;
    x.name = rust::String(name);
    x.has_value = value != nullptr;
    auto *bytes = static_cast<const uint8_t *>(value);
    for (size_t j = 0; j < size; ++j)
      x.value.push_back(bytes[j]);
    out.push_back(std::move(x));
  }
  return out;
}
} // namespace kafka_bridge

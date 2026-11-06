#include "librdkafka-rs-sys/src/lib.rs.h"
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
  std::unique_ptr<rd_kafka_message_t, decltype(&rd_kafka_message_destroy)>
      owner_message(p, rd_kafka_message_destroy);
  auto out = std::make_unique<Message>();
  out->owner = std::move(owner);
  out->value = owner_message.release();
  return out;
}
int32_t subscribe(Client &c, rust::Slice<const rust::String> names) {
  auto *list = rd_kafka_topic_partition_list_new(names.size());
  std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)>
      owner(list, rd_kafka_topic_partition_list_destroy);
  if (!list) throw std::bad_alloc();
  for (const auto &n : names) {
    std::string name(n);
    rd_kafka_topic_partition_list_add(list, name.c_str(),
                                      RD_KAFKA_PARTITION_UA);
  }
  auto e = rd_kafka_subscribe(c.raw(), list);
  return e;
}
int32_t unsubscribe(Client &c) { return rd_kafka_unsubscribe(c.raw()); }
rust::Vec<NativePartition> consumer_list(Client &c, bool assignment) {
  rd_kafka_topic_partition_list_t *list = nullptr;
  auto e = assignment ? rd_kafka_assignment(c.raw(), &list)
                      : rd_kafka_subscription(c.raw(), &list);
  std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)>
      owner(list, rd_kafka_topic_partition_list_destroy);
  // The final cooperative unassign can retire the group queue during close.
  // A terminated group has no assignment even if its query reports teardown.
  if (assignment &&
      (e == RD_KAFKA_RESP_ERR__DESTROY || e == RD_KAFKA_RESP_ERR__UNKNOWN_GROUP) &&
      (c.state->close_started || rd_kafka_consumer_closed(c.raw())))
    return rust::Vec<NativePartition>();
  if (e)
    throw std::runtime_error(std::to_string(e) + ":" + rd_kafka_err2str(e));
  if (!list) throw std::runtime_error("native partition list unavailable");
  rust::Vec<NativePartition> out;
  for (int i = 0; i < list->cnt; ++i) {
    auto &p = list->elems[i];
    out.push_back(partition_from(p));
  }
  return out;
}
void consumer_incremental(Client &client,
                          rust::Slice<const NativePartition> source,
                          bool assign, bool complete_rebalance) {
  rd_kafka_topic_partition_list_t *current = nullptr;
  auto code = rd_kafka_assignment(client.raw(), &current);
  using Partitions = std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)>;
  Partitions owner(current, rd_kafka_topic_partition_list_destroy);
  if (code)
    throw std::runtime_error(std::to_string(code) + ":" + rd_kafka_err2str(code));
  if (!current) throw std::runtime_error("native assignment unavailable");
  Partitions selected(rd_kafka_topic_partition_list_new(source.size()), rd_kafka_topic_partition_list_destroy);
  if (!selected) throw std::bad_alloc();
  for (const auto &p : source) {
    auto *target = rd_kafka_topic_partition_list_add(selected.get(), std::string(p.topic).c_str(), p.partition);
    partition_to(*target, p);
  }
  // Repeated assignment deltas do not add or remove a partition twice.
  for (int i = selected->cnt - 1; i >= 0; --i) {
    const auto &p = selected->elems[i];
    bool present = rd_kafka_topic_partition_list_find(current, p.topic, p.partition);
    if (present == assign)
      rd_kafka_topic_partition_list_del_by_idx(selected.get(), i);
  }
  if (!selected->cnt && !complete_rebalance)
    return;
  check_native_error(assign ? rd_kafka_incremental_assign(client.raw(), selected.get())
                 : rd_kafka_incremental_unassign(client.raw(), selected.get()));
  client.state->callbacks.rebalance_pending = false;
}
bool consumer_assignment_lost(const Client &client) {
  return rd_kafka_assignment_lost(client.raw());
}
rust::String consumer_rebalance_protocol(const Client &client) {
  const char *protocol = rd_kafka_rebalance_protocol(client.raw());
  return protocol ? protocol : "NONE";
}
std::unique_ptr<Message> consume(Client &c, int32_t timeout) {
  return wrap(rd_kafka_consumer_poll(c.raw(), timeout), c.state);
}
void consumer_close_begin(Client &c) {
  if (c.close_queue || rd_kafka_consumer_closed(c.raw())) return;
  c.close_queue.reset(rd_kafka_queue_new(c.raw()));
  if (!c.close_queue) throw std::bad_alloc();
  abort_pending_rebalance(*c.state);
  auto *error = rd_kafka_consumer_close_queue(c.raw(), c.close_queue.get());
  if (error) {
    std::string message = std::to_string(rd_kafka_error_code(error)) + ":" + rd_kafka_error_string(error);
    rd_kafka_error_destroy(error);
    c.close_queue.reset();
    throw std::runtime_error(message);
  }
  c.state->close_started = true;
}
bool consumer_close_poll(Client &c, int32_t timeout) {
  if (c.close_queue) {
    auto *message = rd_kafka_consume_queue(c.close_queue.get(), timeout);
    if (message) rd_kafka_message_destroy(message);
  }
  return rd_kafka_consumer_closed(c.raw());
}
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
bool queue_retains_forwarded_owner(const Queue &q) {
  return static_cast<bool>(q.forwarded_owner);
}
void queue_unforward(Queue &q) {
  rd_kafka_queue_forward(q.value, nullptr);
  q.forwarded_owner.reset();
}
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
  static const uint8_t empty = 0;
  return {m.value->key_len ? static_cast<const uint8_t *>(m.value->key) : &empty, m.value->key_len};
}
bool message_has_payload(const Message &m) {
  return m.value->payload != nullptr;
}
rust::Slice<const uint8_t> message_payload(const Message &m) {
  static const uint8_t empty = 0;
  return {m.value->len ? static_cast<const uint8_t *>(m.value->payload) : &empty, m.value->len};
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

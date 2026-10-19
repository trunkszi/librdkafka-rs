#include "src/shim.h"
#include "rdkafka-sys/src/tls_callbacks.h"
#include "rdkafka-sys/src/lib.rs.h"
#include <array>
#include <fcntl.h>
#include <rdkafka_mock.h>
#include <stdexcept>
#include <sys/socket.h>
namespace kafka_bridge {
// Sequence-tagged slots prevent producers from publishing partially written
// events.
struct EventQueue {
  static constexpr size_t capacity = 4096;
  struct Slot {
    std::atomic<size_t> sequence;
    NativeEvent value;
  };
  std::array<Slot, capacity> slots;
  std::atomic<size_t> enqueue{0};
  size_t dequeue = 0;
  std::atomic<bool> overflow{false};
  EventQueue() {
    for (size_t i = 0; i < capacity; ++i)
      slots[i].sequence.store(i, std::memory_order_relaxed);
  }
  void push(NativeEvent event) {
    size_t pos = enqueue.load(std::memory_order_relaxed);
    for (;;) {
      auto &slot = slots[pos % capacity];
      auto seq = slot.sequence.load(std::memory_order_acquire);
      auto difference = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
      if (difference == 0) {
        if (enqueue.compare_exchange_weak(pos, pos + 1,
                                          std::memory_order_relaxed)) {
          slot.value = std::move(event);
          slot.sequence.store(pos + 1, std::memory_order_release);
          return;
        }
      } else if (difference < 0) {
        overflow.store(true, std::memory_order_release);
        return;
      } else
        pos = enqueue.load(std::memory_order_relaxed);
    }
  }
  bool pop(NativeEvent &event) {
    auto &slot = slots[dequeue % capacity];
    if (slot.sequence.load(std::memory_order_acquire) != dequeue + 1)
      return false;
    event = std::move(slot.value);
    slot.sequence.store(dequeue + capacity, std::memory_order_release);
    ++dequeue;
    return true;
  }
};
Callbacks::Callbacks()
    : hooks(hooks_empty()), events(std::make_unique<EventQueue>()) {}
Callbacks::~Callbacks() = default;
State::~State() {
  callbacks.manual_rebalance = false;
  if (auto *consumer = dynamic_cast<RdKafka::KafkaConsumer *>(handle.get())) {
    if (!rd_kafka_consumer_closed(consumer->c_ptr())) consumer->close();
  }
}
Client::~Client() {
  if (!state) return;
  state->callbacks.manual_rebalance = false;
  if (close_queue) {
    while (!rd_kafka_consumer_closed(raw())) {
      auto *message = rd_kafka_consume_queue(close_queue.get(), 100);
      if (message) rd_kafka_message_destroy(message);
    }
  }
}
Conf::~Conf() = default;
static std::string str(rust::Str s) { return std::string(s); }
void Callbacks::record(int kind, int code, std::string text, std::string topic,
                       int partition, int64_t offset, uint64_t token) {
  events->push(NativeEvent{kind,
                           code,
                           rust::String(text),
                           rust::String(topic),
                           partition,
                           offset,
                           token,
                           {}});
}
rust::Vec<NativeEvent> Callbacks::drain() {
  rust::Vec<NativeEvent> out;
  NativeEvent event;
  while (events->pop(event))
    out.push_back(std::move(event));
  if (events->overflow.exchange(false, std::memory_order_acq_rel))
    out.push_back({12,
                   RD_KAFKA_RESP_ERR__QUEUE_FULL,
                   "NATIVE_OBSERVER_QUEUE_OVERFLOW",
                   "",
                   -1,
                   -1,
                   0,
                   {}});
  return out;
}
void Callbacks::dr_cb(RdKafka::Message &m) {
  NativeEvent event{};
  event.kind = 0;
  event.code = m.err();
  event.message = m.errstr();
  event.topic = m.topic_name();
  event.partition = m.partition();
  event.offset = m.offset();
  event.token = reinterpret_cast<uintptr_t>(m.msg_opaque());
  event.status = static_cast<int32_t>(m.status());
  event.latency_us = m.latency();
  events->push(std::move(event));
}
void Callbacks::event_cb(RdKafka::Event &e) {
  NativeEvent event{};
  event.kind = 1 + static_cast<int>(e.type());
  event.code = e.err();
  event.message = e.str();
  event.broker_name = e.broker_name();
  event.broker_id = e.broker_id();
  event.throttle_ms = e.throttle_time();
  event.log_level = static_cast<int32_t>(e.severity());
  event.log_facility = e.fac();
  event.fatal = e.fatal();
  events->push(std::move(event));
}
void Callbacks::consume_cb(RdKafka::Message &m, void *) {
  NativeEvent event{};
  event.kind = 10;
  event.code = m.err();
  event.message = m.errstr();
  event.topic = m.topic_name();
  event.partition = m.partition();
  event.offset = m.offset();
  auto timestamp = m.timestamp();
  event.timestamp = timestamp.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE
                        ? -1 : timestamp.timestamp;
  auto *raw = m.c_ptr();
  event.has_key = raw->key != nullptr;
  event.has_payload = raw->payload != nullptr;
  auto *key = static_cast<const uint8_t *>(raw->key);
  auto *payload = static_cast<const uint8_t *>(raw->payload);
  for (size_t i = 0; i < raw->key_len; ++i) event.key.push_back(key[i]);
  if (!raw->err)
    for (size_t i = 0; i < raw->len; ++i) event.payload.push_back(payload[i]);
  rd_kafka_headers_t *headers = nullptr;
  if (!rd_kafka_message_headers(raw, &headers)) {
    for (size_t i = 0;; ++i) {
      const char *name; const void *value; size_t size;
      if (rd_kafka_header_get_all(headers, i, &name, &value, &size)) break;
      NativeHeader header{};
      header.name = name;
      header.has_value = value != nullptr;
      auto *bytes = static_cast<const uint8_t *>(value);
      for (size_t n = 0; n < size; ++n) header.value.push_back(bytes[n]);
      event.headers.push_back(std::move(header));
    }
  }
  events->push(std::move(event));
}
static NativeEvent
partition_event(int kind, RdKafka::ErrorCode code,
                std::vector<RdKafka::TopicPartition *> &partitions) {
  NativeEvent event{kind, code, "", "", -1, -1, 0, {}};
  for (auto *p : partitions) {
    NativePartition part{rust::String(p->topic()), p->partition(), p->offset(), p->err(),
                         p->get_leader_epoch(), {}};
    for (auto byte : p->get_metadata()) part.metadata.push_back(byte);
    event.partitions.push_back(std::move(part));
  }
  return event;
}
void Callbacks::rebalance_cb(RdKafka::KafkaConsumer *c, RdKafka::ErrorCode e,
                             std::vector<RdKafka::TopicPartition *> &p) {
  auto event = partition_event(5, e, p);
  event.assignment_lost = c->assignment_lost();
  event.rebalance_protocol = c->rebalance_protocol();
  events->push(std::move(event));
  if (manual_rebalance) return;
  if (c->rebalance_protocol() == "COOPERATIVE") {
    auto *error = e == RdKafka::ERR__ASSIGN_PARTITIONS
                      ? c->incremental_assign(p)
                      : c->incremental_unassign(p);
    if (error) {
      record(1, error->code(), error->str());
      delete error;
    }
  } else if (e == RdKafka::ERR__ASSIGN_PARTITIONS)
    c->assign(p);
  else
    c->unassign();
}
void Callbacks::offset_commit_cb(RdKafka::ErrorCode e,
                                 std::vector<RdKafka::TopicPartition *> &p) {
  events->push(partition_event(6, e, p));
}
void Callbacks::oauthbearer_token_refresh_cb(RdKafka::Handle *,
                                             const std::string &config) {
  record(7, 0, config);
}
int32_t Callbacks::partitioner_cb(const RdKafka::Topic *t,
                                  const std::string *key, int32_t n,
                                  void *opaque) {
  return partitioner_cb(t, key ? key->data() : nullptr, key ? key->size() : 0,
                        n, opaque);
}
int32_t Callbacks::partitioner_cb(const RdKafka::Topic *t, const void *key,
                                  size_t len, int32_t n, void *) {
  record(11, 0, "native partitioner", t->name());
  return rd_kafka_msg_partitioner_consistent_random(
      const_cast<RdKafka::Topic *>(t)->c_ptr(), key, len, n, nullptr, nullptr);
}
int Callbacks::socket_cb(int domain, int type, int protocol) {
  record(8, domain, "socket", "", type, protocol);
  auto result = hooks_socket(*hooks, domain, type, protocol);
  auto fd = result == -2 ? ::socket(domain, type, protocol) : result;
  tls_track_socket(fd, *this);
  return fd;
}
int Callbacks::open_cb(const std::string &path, int flags, int mode) {
  record(9, flags, path, "", mode);
  auto result = hooks_open(*hooks, path, flags, mode);
  return result == -2 ? ::open(path.c_str(), flags, mode) : result;
}
// The adapter borrows a native topic only while librdkafka owns the callback.
// Its opaque pointer is callback state, never a destructible C++ Topic wrapper.
class BorrowedTopic final : public RdKafka::Topic {
  rd_kafka_topic_t *raw_;

public:
  explicit BorrowedTopic(const rd_kafka_topic_t *topic)
      : raw_(const_cast<rd_kafka_topic_t *>(topic)) {}
  std::string name() const override { return rd_kafka_topic_name(raw_); }
  bool partition_available(int32_t p) const override {
    return rd_kafka_topic_partition_available(raw_, p);
  }
  RdKafka::ErrorCode offset_store(int32_t p, int64_t o) override {
    return static_cast<RdKafka::ErrorCode>(rd_kafka_offset_store(raw_, p, o));
  }
  rd_kafka_topic_t *c_ptr() override { return raw_; }
};
int32_t partitioner_pointer(const rd_kafka_topic_t *topic, const void *key,
                            size_t len, int32_t n, void *opaque, void *token) {
  BorrowedTopic borrowed(topic);
  return static_cast<Callbacks *>(opaque)->partitioner_cb(&borrowed, key, len,
                                                          n, token);
}
int32_t partitioner_string(const rd_kafka_topic_t *topic, const void *key,
                           size_t len, int32_t n, void *opaque, void *token) {
  BorrowedTopic borrowed(topic);
  std::string text;
  if (key)
    text.assign(static_cast<const char *>(key), len);
  return static_cast<Callbacks *>(opaque)->partitioner_cb(
      &borrowed, key ? &text : nullptr, n, token);
}
Conf::Conf(bool topic)
    : topic(topic),
      value(RdKafka::Conf::create(topic ? RdKafka::Conf::CONF_TOPIC
                                        : RdKafka::Conf::CONF_GLOBAL)),
      hooks(hooks_empty()) {}
std::unique_ptr<Conf> conf_new(bool topic) {
  return std::make_unique<Conf>(topic);
}
void conf_set(Conf &c, rust::Str key, rust::Str value) {
  std::string e;
  if (c.value->set(str(key), str(value), e) != RdKafka::Conf::CONF_OK)
    throw std::runtime_error(e);
  c.properties.emplace_back(str(key), str(value));
}
rust::String conf_get(const Conf &c, rust::Str key) {
  std::string value;
  if (c.value->get(str(key), value) != RdKafka::Conf::CONF_OK)
    throw std::runtime_error("unknown configuration: " + str(key));
  return value;
}
rust::Vec<NativeProperty> conf_dump(const Conf &c) {
  std::unique_ptr<std::list<std::string>> values(c.value->dump());
  rust::Vec<NativeProperty> out;
  for (auto i = values->begin(); i != values->end();) {
    auto key = *i++;
    auto value = *i++;
    out.push_back({rust::String(key), rust::String(value)});
  }
  return out;
}
std::unique_ptr<Client> client_new(const Conf &c, int32_t kind) {
  if (c.topic || kind < 0 || kind > 2)
    throw std::runtime_error(
        "global configuration and valid client kind required");
  auto out = std::make_unique<Client>();
  out->state = std::make_shared<State>();
  out->kind = kind;
  auto cfg = std::unique_ptr<RdKafka::Conf>(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  std::string e;
  for (const auto &p : c.properties)
    if (cfg->set(p.first, p.second, e) != RdKafka::Conf::CONF_OK)
      throw std::runtime_error(e);
  auto &cb = out->state->callbacks;
  cb.hooks = hooks_clone(*c.hooks);
  cb.manual_rebalance = c.rebalance_observer;
  if (hooks_tls_enabled(*cb.hooks)) tls_install_callbacks(cfg->c_ptr_global());
  auto check = [&](auto ptr, const char *name) {
    if (cfg->set(name, ptr, e) != RdKafka::Conf::CONF_OK)
      throw std::runtime_error(e);
  };
  check(static_cast<RdKafka::DeliveryReportCb *>(&cb), "dr_cb");
  check(static_cast<RdKafka::EventCb *>(&cb), "event_cb");
  if (c.consume_observer)
    check(static_cast<RdKafka::ConsumeCb *>(&cb), "consume_cb");
  check(static_cast<RdKafka::SocketCb *>(&cb), "socket_cb");
  check(static_cast<RdKafka::OpenCb *>(&cb), "open_cb");
  check(static_cast<RdKafka::OAuthBearerTokenRefreshCb *>(&cb),
        "oauthbearer_token_refresh_cb");
  if (kind == 1) {
    check(static_cast<RdKafka::RebalanceCb *>(&cb), "rebalance_cb");
    check(static_cast<RdKafka::OffsetCommitCb *>(&cb), "offset_commit_cb");
  }
  if (cfg->set("log.queue", "true", e) != RdKafka::Conf::CONF_OK)
    throw std::runtime_error(e);
  if (kind == 0) {
    auto *topic_conf =
        rd_kafka_conf_get_default_topic_conf(cfg->c_ptr_global());
    auto *copy = topic_conf ? rd_kafka_topic_conf_dup(topic_conf)
                            : rd_kafka_topic_conf_new();
    rd_kafka_topic_conf_set_opaque(copy, &cb);
    rd_kafka_topic_conf_set_partitioner_cb(copy, partitioner_pointer);
    rd_kafka_conf_set_default_topic_conf(cfg->c_ptr_global(), copy);
  }
  if (kind == 0)
    out->state->handle.reset(RdKafka::Producer::create(cfg.get(), e));
  else if (kind == 1)
    out->state->handle.reset(RdKafka::KafkaConsumer::create(cfg.get(), e));
  else
    out->state->handle.reset(RdKafka::Consumer::create(cfg.get(), e));
  if (!out->state->handle)
    throw std::runtime_error(e);
  rd_kafka_set_log_queue(out->raw(), nullptr);
  if (kind == 1)
    rd_kafka_poll_set_consumer(out->raw());
  return out;
}
rust::Vec<NativeEvent> client_poll(Client &c, int32_t timeout) {
  if (c.kind != 1)
    c.state->handle->poll(timeout);
  auto &callbacks = c.state->callbacks;
  for (auto &message : hooks_tls_errors(*callbacks.hooks))
    callbacks.record(13, RD_KAFKA_RESP_ERR__SSL, std::string(message));
  return callbacks.drain();
}
int32_t client_flush(Client &c, int32_t timeout) {
  return rd_kafka_flush(c.raw(), timeout);
}
int32_t client_outq(const Client &c) { return rd_kafka_outq_len(c.raw()); }
rust::String client_name(const Client &c) { return c.state->handle->name(); }
rust::String error_name(int32_t c) {
  return rd_kafka_err2name(static_cast<rd_kafka_resp_err_t>(c));
}
rust::String error_message(int32_t c) {
  return rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(c));
}
uint8_t error_flags(int32_t code) {
  // Transaction-abort flags belong to operation errors, never code-only
  // objects.
  switch (code) {
  case RD_KAFKA_RESP_ERR__QUEUE_FULL:
  case RD_KAFKA_RESP_ERR__TIMED_OUT:
  case RD_KAFKA_RESP_ERR__TRANSPORT:
  case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
  case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
  case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
  case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
  case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
  case RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION:
    return 1;
  case RD_KAFKA_RESP_ERR__FATAL:
    return 2;
  default:
    return 0;
  }
}
} // namespace kafka_bridge

namespace kafka_bridge {
void conf_set_observers(Conf &c, bool consume, bool rebalance) {
  c.consume_observer = consume;
  c.rebalance_observer = rebalance;
}
void conf_set_hooks(Conf &c, rust::Box<NativeHooks> hooks) {
  c.hooks = std::move(hooks);
}
rust::String mock_bootstraps(const Client &c) {
  auto *cluster = rd_kafka_handle_mock_cluster(c.raw());
  return cluster ? rd_kafka_mock_cluster_bootstraps(cluster) : "";
}
void oauth_set_token(Client &c, rust::Str token, int64_t expiry,
                     rust::Str principal,
                     rust::Slice<const rust::String> extensions) {
  std::string t(token), p(principal);
  std::vector<std::string> owned;
  for (const auto &e : extensions)
    owned.emplace_back(e);
  std::vector<const char *> ext;
  for (const auto &e : owned)
    ext.push_back(e.c_str());
  char error[512];
  auto code = rd_kafka_oauthbearer_set_token(c.raw(), t.c_str(), expiry,
                                             p.c_str(), ext.data(), ext.size(),
                                             error, sizeof(error));
  if (code)
    throw std::runtime_error(std::to_string(code) + ":" + error);
}
int32_t oauth_set_failure(Client &c, rust::Str reason) {
  std::string r(reason);
  return rd_kafka_oauthbearer_set_token_failure(c.raw(), r.c_str());
}
} // namespace kafka_bridge

#include "src/shim.h"
#include "rdkafka-sys/src/tls_callbacks.h"
#include "rdkafka-sys/src/lib.rs.h"
#include <cerrno>
#include <chrono>
#include <unistd.h>
#include <thread>
#include <array>
#include <fcntl.h>
#include <rdkafka_mock.h>
#include <stdexcept>
#include <sys/socket.h>
namespace kafka_bridge {
// Producers publish into a linked MPSC FIFO; only the caller thread drains it.
// Native handles stop their callback producers before this queue is destroyed.
struct EventQueue {
  static constexpr size_t capacity = 4096;
  struct Node {
    std::atomic<Node *> next{nullptr};
    NativeEvent value{};
    Node() = default;
    explicit Node(NativeEvent event) : value(std::move(event)) {}
  };
  Node *head = new Node();
  std::atomic<Node *> tail{head};
  std::atomic<size_t> logs{0};
  ~EventQueue() {
    while (head) {
      auto *next = head->next.load(std::memory_order_relaxed);
      delete head;
      head = next;
    }
  }
  void push(NativeEvent event) {
    auto node = std::make_unique<Node>(std::move(event));
    const bool log = node->value.kind == 1 + RdKafka::Event::EVENT_LOG;
    if (log && logs.fetch_add(1, std::memory_order_relaxed) >= capacity) {
      logs.fetch_sub(1, std::memory_order_relaxed);
      return;
    }
    auto *next = node.release();
    // Exchange orders producers; release-link publishes the initialized node.
    auto *previous = tail.exchange(next, std::memory_order_acq_rel);
    previous->next.store(next, std::memory_order_release);
  }
  bool pop(NativeEvent &event) {
    auto *next = head->next.load(std::memory_order_acquire);
    if (!next) return false;
    // Acquiring the link also proves its producer has finished using head.
    event = std::move(next->value);
    delete head;
    head = next;
    if (event.kind == 1 + RdKafka::Event::EVENT_LOG)
      logs.fetch_sub(1, std::memory_order_relaxed);
    return true;
  }
};
Callbacks::Callbacks()
    : hooks(hooks_empty()), events(std::make_unique<EventQueue>()) {}
Callbacks::~Callbacks() {
  // State destroys the native handle first, while its close hooks can use us.
  tls_untrack_callbacks(this);
}
static void finish_close(State &state, rd_kafka_queue_t *queue) {
  auto *raw = state.handle->c_ptr();
  // Failed Rust rebalance callbacks cannot leave native revocation pending.
  const char *protocol = rd_kafka_rebalance_protocol(raw);
  if (protocol && std::string(protocol) == "COOPERATIVE") {
    rd_kafka_topic_partition_list_t *parts = nullptr;
    auto code = rd_kafka_assignment(raw, &parts);
    std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)>
        owner(parts, rd_kafka_topic_partition_list_destroy);
    if (!code && parts) {
      auto *error = rd_kafka_incremental_unassign(raw, parts);
      if (error) rd_kafka_error_destroy(error);
    }
  } else rd_kafka_assign(raw, nullptr);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!rd_kafka_consumer_closed(raw) && std::chrono::steady_clock::now() < deadline) {
    auto *message = rd_kafka_consume_queue(queue, 100);
    if (message) rd_kafka_message_destroy(message);
  }
}
State::~State() {
  callbacks.manual_rebalance = false;
  auto *consumer = dynamic_cast<RdKafka::KafkaConsumer *>(handle.get());
  if (!consumer || close_started || rd_kafka_consumer_closed(consumer->c_ptr())) return;
  std::unique_ptr<rd_kafka_queue_t, decltype(&rd_kafka_queue_destroy)>
      queue(rd_kafka_queue_new(consumer->c_ptr()), rd_kafka_queue_destroy);
  if (!queue) return;
  auto *error = rd_kafka_consumer_close_queue(consumer->c_ptr(), queue.get());
  if (error) { rd_kafka_error_destroy(error); return; }
  close_started = true;
  finish_close(*this, queue.get());
  // KafkaConsumerImpl's destructor uses NO_CONSUMER_CLOSE if the deadline expires.
}
Client::~Client() {
  if (!state) return;
  state->callbacks.manual_rebalance = false;
  if (close_queue) finish_close(*state, close_queue.get());
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
  return rd_kafka_msg_partitioner_consistent_random(
      const_cast<RdKafka::Topic *>(t)->c_ptr(), key, len, n, nullptr, nullptr);
}
static int cloexec(int fd) {
  if (fd < 0) return fd;
  const int flags = fcntl(fd, F_GETFD);
  if (flags >= 0 && fcntl(fd, F_SETFD, flags | FD_CLOEXEC) >= 0) return fd;
  const int error = errno;
  ::close(fd);
  errno = error;
  return -1;
}
static int fallback_socket(int domain, int type, int protocol) {
#ifdef SOCK_CLOEXEC
  int fd = ::socket(domain, type | SOCK_CLOEXEC, protocol);
  if (fd >= 0 || errno != EINVAL) return fd;
#endif
  return cloexec(::socket(domain, type, protocol));
}
static int fallback_open(const std::string &path, int flags, int mode) {
#ifdef O_CLOEXEC
  int fd = ::open(path.c_str(), flags | O_CLOEXEC, mode);
  if (fd >= 0 || errno != EINVAL) return fd;
#endif
  return cloexec(::open(path.c_str(), flags, mode));
}
bool native_callback_contract_test() {
  Callbacks callbacks;
  for (int kind : {0, 1, 5, 6, 7, 10, 13})
    for (uint64_t i = 0; i < 5000; ++i)
      callbacks.record(kind, 0, "", "", -1, -1, i);
  auto events = callbacks.drain();
  if (events.size() != 35000) return false;
  for (size_t i = 0; i < events.size(); ++i)
    if (events[i].token != i % 5000) return false;
  std::atomic<int> remaining{4};
  std::vector<std::thread> producers;
  for (int producer = 0; producer < 4; ++producer) {
    producers.emplace_back([&, producer] {
      for (uint64_t i = 0; i < 5000; ++i)
        callbacks.record(0, producer, "", "", -1, -1, i);
      remaining.fetch_sub(1, std::memory_order_release);
    });
  }
  std::array<uint64_t, 4> received{};
  bool ordered = true;
  auto consume = [&] {
    for (const auto &event : callbacks.drain()) {
      if (event.code < 0 || event.code >= 4) { ordered = false; continue; }
      if (event.token != received[event.code]++) ordered = false;
    }
  };
  while (remaining.load(std::memory_order_acquire)) {
    consume();
    std::this_thread::yield();
  }
  for (auto &producer : producers) producer.join();
  consume();
  if (!ordered) return false;
  for (auto count : received) if (count != 5000) return false;
  for (int i = 0; i < 6000; ++i)
    callbacks.record(1 + RdKafka::Event::EVENT_LOG, 0, "log");
  callbacks.record(13, 0, "TLS failure");
  auto bounded = callbacks.drain();
  if (bounded.size() != 4097 || bounded[bounded.size() - 1].kind != 13) return false;
  const int socket = callbacks.socket_cb(AF_UNIX, SOCK_STREAM, 0);
  const int file = callbacks.open_cb("/dev/null", O_RDONLY, 0);
  const bool valid = socket >= 0 && file >= 0 &&
      (fcntl(socket, F_GETFD) & FD_CLOEXEC) &&
      (fcntl(file, F_GETFD) & FD_CLOEXEC);
  if (socket >= 0) ::close(socket);
  if (file >= 0) ::close(file);
  rd_kafka_message_t raw{};
  Message message{};
  message.value = &raw;
  const bool empty = message_key(message).empty() && message_key(message).data() &&
                     message_payload(message).empty() && message_payload(message).data();
  message.value = nullptr;
  return valid && empty;
}
int Callbacks::socket_cb(int domain, int type, int protocol) {
  record(8, domain, "socket", "", type, protocol);
  auto result = hooks_socket(*hooks, domain, type, protocol);
  auto fd = result == -2 ? fallback_socket(domain, type, protocol) : result;
  tls_track_socket(fd, *this);
  return fd;
}
int Callbacks::open_cb(const std::string &path, int flags, int mode) {
  record(9, flags, path, "", mode);
  auto result = hooks_open(*hooks, path, flags, mode);
  return result == -2 ? fallback_open(path, flags, mode) : result;
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
  return client_new_with_partitioner(c, kind, false);
}
std::unique_ptr<Client> client_new_with_partitioner(const Conf &c, int32_t kind,
                                                     bool has_partitioner) {
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
  cb.has_partitioner = has_partitioner;
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
  if (kind == 0 && has_partitioner) {
    auto *topic_conf =
        rd_kafka_conf_get_default_topic_conf(cfg->c_ptr_global());
    auto *copy = topic_conf ? rd_kafka_topic_conf_dup(topic_conf)
                            : rd_kafka_topic_conf_new();
    if (!copy) throw std::bad_alloc();
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

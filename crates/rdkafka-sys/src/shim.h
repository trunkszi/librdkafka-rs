#pragma once
#include "rust/cxx.h"
#include <atomic>
#include <memory>
#include <rdkafka.h>
#include <rdkafkacpp.h>
#include <utility>
#include <vector>
namespace kafka_bridge {
struct NativeProperty;
struct NativeHeader;
struct NativePartition;
struct NativeEvent;
struct NativeMetadata;
struct NativeHooks;
struct EventQueue;
class Callbacks : public RdKafka::DeliveryReportCb,
                  public RdKafka::EventCb,
                  public RdKafka::ConsumeCb,
                  public RdKafka::RebalanceCb,
                  public RdKafka::OffsetCommitCb,
                  public RdKafka::OAuthBearerTokenRefreshCb,
                  public RdKafka::PartitionerCb,
                  public RdKafka::PartitionerKeyPointerCb,
                  public RdKafka::SocketCb,
                  public RdKafka::OpenCb {
public:
  Callbacks();
  ~Callbacks();
  rust::Box<NativeHooks> hooks;
  bool manual_rebalance = false;
  bool has_partitioner = false;
  std::unique_ptr<EventQueue> events;
  rust::Vec<NativeEvent> drain();
  void record(int kind, int code, std::string text, std::string topic = "",
              int partition = -1, int64_t offset = -1, uint64_t token = 0);
  void dr_cb(RdKafka::Message &) override;
  void event_cb(RdKafka::Event &) override;
  void consume_cb(RdKafka::Message &, void *) override;
  void rebalance_cb(RdKafka::KafkaConsumer *, RdKafka::ErrorCode,
                    std::vector<RdKafka::TopicPartition *> &) override;
  void offset_commit_cb(RdKafka::ErrorCode,
                        std::vector<RdKafka::TopicPartition *> &) override;
  void oauthbearer_token_refresh_cb(RdKafka::Handle *,
                                    const std::string &) override;
  int32_t partitioner_cb(const RdKafka::Topic *, const std::string *, int32_t,
                         void *) override;
  int32_t partitioner_cb(const RdKafka::Topic *, const void *, size_t, int32_t,
                         void *) override;
  int socket_cb(int, int, int) override;
  int open_cb(const std::string &, int, int) override;
};
struct State {
  ~State();
  Callbacks callbacks;
  std::unique_ptr<RdKafka::Handle> handle;
};
struct Conf {
  bool topic;
  bool consume_observer = false;
  bool rebalance_observer = false;
  std::vector<std::pair<std::string, std::string>> properties;
  std::unique_ptr<RdKafka::Conf> value;
  rust::Box<NativeHooks> hooks;
  explicit Conf(bool);
  ~Conf();
};
struct Client {
  ~Client();
  std::shared_ptr<State> state;
  int kind;
  std::unique_ptr<rd_kafka_queue_t, decltype(&rd_kafka_queue_destroy)> close_queue{nullptr, rd_kafka_queue_destroy};
  rd_kafka_t *raw() const { return state->handle->c_ptr(); }
};
struct Topic {
  std::shared_ptr<State> owner;
  rd_kafka_topic_t *value;
  ~Topic();
};
struct Queue {
  std::shared_ptr<State> owner;
  std::shared_ptr<State> forwarded_owner;
  rd_kafka_queue_t *value;
  ~Queue();
};
struct Message {
  std::shared_ptr<State> owner;
  rd_kafka_message_t *value;
  ~Message();
};
int32_t partitioner_pointer(const rd_kafka_topic_t *, const void *, size_t,
                            int32_t, void *, void *);
int32_t partitioner_string(const rd_kafka_topic_t *, const void *, size_t,
                           int32_t, void *, void *);
bool native_callback_contract_test();
bool queue_retains_forwarded_owner(const Queue &);
int32_t topic_murmur2_partition(const Topic &, rust::Slice<const uint8_t>, int32_t);
std::unique_ptr<Conf> conf_new(bool);
void conf_set_hooks(Conf &, rust::Box<NativeHooks>);
void conf_set_observers(Conf &, bool, bool);
rust::String mock_bootstraps(const Client &);
void oauth_set_token(Client &, rust::Str, int64_t, rust::Str,
                     rust::Slice<const rust::String>);
int32_t oauth_set_failure(Client &, rust::Str);
void conf_set(Conf &, rust::Str, rust::Str);
rust::String conf_get(const Conf &, rust::Str);
rust::Vec<NativeProperty> conf_dump(const Conf &);
std::unique_ptr<Client> client_new(const Conf &, int32_t);
std::unique_ptr<Client> client_new_with_partitioner(const Conf &, int32_t, bool);
rust::Vec<NativeEvent> client_poll(Client &, int32_t);
int32_t client_flush(Client &, int32_t);
int32_t client_outq(const Client &);
rust::String client_name(const Client &);
rust::String client_id(Client &, int32_t, int32_t);
int32_t client_controller(Client &, int32_t);
NativeEvent client_fatal(Client &);
int32_t produce(Client &, rust::Str, int32_t, rust::Slice<const uint8_t>, bool,
                rust::Slice<const uint8_t>, bool,
                rust::Slice<const NativeHeader>, int64_t, uint64_t);
int32_t produce_topic(Client &, const Topic &, int32_t, rust::Slice<const uint8_t>, bool,
                      rust::Slice<const uint8_t>, bool,
                      rust::Slice<const NativeHeader>, int64_t, uint64_t);
NativeMetadata client_metadata(Client &, rust::Str, int32_t);
rust::Vec<int64_t> client_watermarks(Client &, rust::Str, int32_t, int32_t,
                                     bool);
rust::Vec<NativePartition> client_partitions(Client &, int32_t,
                                             rust::Slice<const NativePartition>,
                                             int32_t);
NativePartition partition_from(const rd_kafka_topic_partition_t &);
void partition_to(rd_kafka_topic_partition_t &, const NativePartition &);
int32_t subscribe(Client &, rust::Slice<const rust::String>);
int32_t unsubscribe(Client &);
rust::Vec<NativePartition> consumer_list(Client &, bool);
std::unique_ptr<Message> consume(Client &, int32_t);
void consumer_close_begin(Client &);
bool consumer_close_poll(Client &, int32_t);
std::unique_ptr<Topic> topic_new(Client &, rust::Str, const Conf &);
rust::String topic_name(const Topic &);
bool topic_available(const Topic &, int32_t);
int32_t simple_start(Client &, const Topic &, int32_t, int64_t);
int32_t simple_stop(Client &, const Topic &, int32_t);
int32_t simple_seek(Client &, const Topic &, int32_t, int64_t, int32_t);
std::unique_ptr<Message> simple_consume(Client &, const Topic &, int32_t,
                                        int32_t);
std::unique_ptr<Queue> queue_new(Client &, rust::Str, int32_t);
void queue_forward(Queue &, const Queue &);
void queue_unforward(Queue &);
int32_t queue_poll(Queue &, int32_t);
std::unique_ptr<Message> queue_consume(Queue &, int32_t);
void queue_io_event(Queue &, int32_t, rust::Slice<const uint8_t>);
int32_t simple_start_queue(Client &, const Topic &, int32_t, int64_t,
                           const Queue &);
int32_t message_error(const Message &);
rust::String message_topic(const Message &);
int32_t message_partition(const Message &);
int64_t message_offset(const Message &);
int64_t message_timestamp(const Message &);
bool message_has_key(const Message &);
rust::Slice<const uint8_t> message_key(const Message &);
bool message_has_payload(const Message &);
rust::Slice<const uint8_t> message_payload(const Message &);
rust::Vec<NativeHeader> message_headers(const Message &);
rust::String error_name(int32_t);
rust::String error_message(int32_t);
uint8_t error_flags(int32_t);
} // namespace kafka_bridge

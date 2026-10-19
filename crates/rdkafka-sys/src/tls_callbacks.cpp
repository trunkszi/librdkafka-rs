#include "rdkafka-sys/src/tls_callbacks.h"
#include "rdkafka-sys/src/lib.rs.h"
#include "rdkafka-sys/src/shim.h"
#include <cerrno>
#include <cstdlib>
#include <fcntl.h>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unordered_map>
#include <unistd.h>
namespace kafka_bridge {
// Only connection creation and teardown use this registry. TLS data is pumped
// independently without holding a registry lock. The C++ SDK owns native opaque.
static std::mutex registry_lock;
static std::unordered_map<int, Callbacks *> registry;
void tls_track_socket(int fd, Callbacks &callbacks) {
  if (fd < 0 || !hooks_tls_enabled(*callbacks.hooks)) return;
  std::lock_guard<std::mutex> guard(registry_lock);
  registry[fd] = &callbacks;
}
void tls_untrack_callbacks(Callbacks *callbacks) {
  std::lock_guard<std::mutex> guard(registry_lock);
  for (auto entry = registry.begin(); entry != registry.end();) {
    if (entry->second == callbacks)
      entry = registry.erase(entry);
    else
      ++entry;
  }
}
static Callbacks *find_callbacks(int fd, bool remove) {
  std::lock_guard<std::mutex> guard(registry_lock);
  auto found = registry.find(fd);
  if (found == registry.end()) return nullptr;
  auto *callbacks = found->second;
  if (remove) registry.erase(found);
  return callbacks;
}
static int tls_connect(int fd, const sockaddr *address, int size,
                       const char *name, void *) {
  auto *callbacks = find_callbacks(fd, false);
  if (!callbacks) return EINVAL;
  char host[NI_MAXHOST], service[NI_MAXSERV];
  if (getnameinfo(address, static_cast<socklen_t>(size), host, sizeof(host),
                  service, sizeof(service), NI_NUMERICHOST | NI_NUMERICSERV))
    return EINVAL;
  std::string peer = name ? name : host;
  auto port = static_cast<uint16_t>(std::strtoul(service, nullptr, 10));
  return hooks_tls_connect(*callbacks->hooks, fd, host, port, peer);
}
static int tls_close(int fd, void *) {
  auto *callbacks = find_callbacks(fd, true);
  return callbacks ? hooks_tls_close(*callbacks->hooks, fd) : ::close(fd);
}
void tls_install_callbacks(rd_kafka_conf_t *conf) {
  rd_kafka_conf_set_connect_cb(conf, tls_connect);
  rd_kafka_conf_set_closesocket_cb(conf, tls_close);
}
int32_t tls_callback_peer_name_test(rust::Box<NativeHooks> hooks, rust::Str name) {
  Callbacks callbacks;
  callbacks.hooks = std::move(hooks);
  int fd = callbacks.socket_cb(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return errno;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(9092);
  std::string peer(name);
  int result = tls_connect(fd, reinterpret_cast<const sockaddr *>(&address),
                            sizeof(address), peer.c_str(), nullptr);
  tls_untrack_callbacks(&callbacks);
  ::close(fd);
  return result;
}
bool tls_callback_registry_teardown_test(rust::Box<NativeHooks> hooks) {
  struct Descriptor {
    int value;
    ~Descriptor() { if (value >= 0) ::close(value); }
  };
  auto callbacks = std::make_unique<Callbacks>();
  callbacks->hooks = hooks_clone(*hooks);
  Descriptor first{callbacks->socket_cb(AF_INET, SOCK_STREAM, 0)};
  Descriptor second{callbacks->socket_cb(AF_INET, SOCK_STREAM, 0)};
  Callbacks other;
  other.hooks = std::move(hooks);
  Descriptor unrelated{other.socket_cb(AF_INET, SOCK_STREAM, 0)};
  if (first.value < 0 || second.value < 0 || unrelated.value < 0 ||
      find_callbacks(first.value, false) != callbacks.get() ||
      find_callbacks(second.value, false) != callbacks.get())
    return false;
  callbacks.reset();
  if (find_callbacks(first.value, false) ||
      find_callbacks(second.value, false) ||
      find_callbacks(unrelated.value, false) != &other ||
      ::fcntl(first.value, F_GETFD) < 0 ||
      ::fcntl(second.value, F_GETFD) < 0)
    return false;
  // Replace an owned descriptor atomically so another thread cannot claim it.
  if (::dup2(unrelated.value, first.value) != first.value) return false;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  if (tls_connect(first.value, reinterpret_cast<const sockaddr *>(&address),
                  sizeof(address), "localhost:9092", nullptr) != EINVAL)
    return false;
  tls_track_socket(first.value, other);
  return find_callbacks(first.value, false) == &other;
}
}

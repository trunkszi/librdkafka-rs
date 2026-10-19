#include "rdkafka-sys/src/tls_callbacks.h"
#include "rdkafka-sys/src/lib.rs.h"
#include "rdkafka-sys/src/shim.h"
#include <cerrno>
#include <cstdlib>
#include <mutex>
#include <netdb.h>
#include <string>
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
  if (!peer.empty() && peer.front() == '[') {
    auto end = peer.find(']');
    if (end != std::string::npos) peer = peer.substr(1, end - 1);
  } else {
    auto colon = peer.rfind(':');
    if (colon != std::string::npos) peer.resize(colon);
  }
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
}

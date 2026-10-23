#pragma once
#include "rust/cxx.h"
#include <rdkafka.h>
namespace kafka_bridge {
class Callbacks;
struct NativeHooks;
void tls_track_socket(int fd, Callbacks &callbacks);
void tls_untrack_callbacks(Callbacks *callbacks);
void tls_install_callbacks(rd_kafka_conf_t *conf);
int32_t tls_callback_peer_name_test(rust::Box<NativeHooks>, rust::Str);
bool tls_callback_registry_teardown_test(rust::Box<NativeHooks> hooks);
}

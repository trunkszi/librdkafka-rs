#pragma once
#include <rdkafka.h>
namespace kafka_bridge {
class Callbacks;
void tls_track_socket(int fd, Callbacks &callbacks);
void tls_install_callbacks(rd_kafka_conf_t *conf);
}

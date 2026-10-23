#pragma once
#include "librdkafka-rs-sys/src/shim.h"
namespace kafka_bridge {
struct AdminRequest;
struct AdminResponse;
AdminResponse admin_request(const Client &, int32_t,
                            rust::Slice<const AdminRequest>, int32_t);
AdminResponse admin_result(rd_kafka_event_t *, int32_t);
} // namespace kafka_bridge

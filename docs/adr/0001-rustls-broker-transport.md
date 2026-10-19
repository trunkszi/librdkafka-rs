# Rustls Broker Transport via `connect_cb` Socketpair Bridge

We build `librdkafka` with `WITH_SSL=OFF` and forbid OpenSSL across the workspace, routing all `SSL` and `SASL_SSL` broker connections through a `rustls` session bridged to `librdkafka` via `rd_kafka_conf_set_connect_cb`, `rd_kafka_conf_set_closesocket_cb`, and a local `AF_UNIX` `socketpair`. This preserves `librdkafka`'s native non-blocking `fd` state machine and SASL handshakes (`PLAIN`, `SCRAM`, `OAUTHBEARER`) without patching `librdkafka`'s private `rdkafka_ssl.c` internals or linking OpenSSL.

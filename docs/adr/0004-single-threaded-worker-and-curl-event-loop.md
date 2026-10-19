# Thread-Affine Workers and `curl-rust` EventLoop HTTP

`Producer`, `Consumer`, `SimpleConsumer`, `AdminClient`, `Queue`, `HttpTransport`,
`SchemaRegistryClient`, `OAuthBearerHttpClient`, and `WorkerSlotLease` are
`!Send + !Sync`. Caller-thread record processing and local callback dispatch use
thread-affine state without client mutexes. Shareable `ThreadLocal*` and
`Worker*` managers create or lease these local clients on the calling OS thread.

`librdkafka` builds with `WITH_CURL=OFF`. HTTP/2 operations for OIDC token refresh
and Schema Registry run on the calling thread through `curl-rust`
(`feat/event-loop-ws`) with `rustls`.

Broker TLS follows ADR-0001: `connect_cb` / `closesocket_cb` bridge native broker
sockets through a local `AF_UNIX` socketpair to a per-connection
`rustls::ClientConnection` pump thread. Native broker I/O and these private pump
threads progress independently of caller-thread polling. The native socket
callback registry and Rust session registry use mutexes for connection ownership.
TLS failure reporting also uses a mutex-protected queue, including when polling
drains diagnostics on the caller thread. Successful pump iterations do not lock
these registries or the failure queue. Local `FnMut` callbacks remain on the
calling thread; teardown wakes and joins each connection's pump.

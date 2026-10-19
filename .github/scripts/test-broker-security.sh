#!/usr/bin/env bash
# Real broker TLS, mutual TLS, and SASL record delivery/readback matrix.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
docker info >/dev/null
security_dir=$(mktemp -d)
container="librdkafka-security-$(python3 -c 'import secrets; print(secrets.token_hex(8))')"
created=false
cleanup() {
  result=$?
  if [ "$created" = true ] && docker inspect "$container" >/dev/null 2>&1; then
    if [ "$result" -ne 0 ]; then docker logs --tail 100 "$container" >&2 || true; fi
    if ! docker rm --force "$container" >/dev/null; then
      echo "::error::cannot remove owned security container $container" >&2
      result=1
    fi
  fi
  rm -rf "$security_dir"
  exit "$result"
}
trap cleanup EXIT
export RDKAFKA_SECURITY_CERTS="$security_dir"
cargo test -p rdkafka --features tls --test sasl_live \
  integration_scenario_security_generate_certificates -- --ignored --exact
read -r tls_port mtls_port sasl_port sasl_tls_port < <(python3 - "$security_dir" <<'PY'
import json
import socket
import sys
listeners = [socket.socket() for _ in range(4)]
for listener in listeners:
    listener.bind(('127.0.0.1', 0))
ports = [listener.getsockname()[1] for listener in listeners]
names = ['tls', 'mtls', 'sasl', 'sasl_tls']
config = {'redpanda': {
    'data_directory': '/var/lib/redpanda/data', 'node_id': 0,
    'rpc_server': {'address': '0.0.0.0', 'port': 33145},
    'advertised_rpc_api': {'address': '127.0.0.1', 'port': 33145},
    'admin': [{'address': '0.0.0.0', 'port': 9644}],
    'kafka_api': [{'name': name, 'address': '0.0.0.0', 'port': 9092 + i,
                   'authentication_method': 'sasl' if name.startswith('sasl') else 'none'}
                  for i, name in enumerate(names)],
    'advertised_kafka_api': [{'name': name, 'address': '127.0.0.1', 'port': port}
                             for name, port in zip(names, ports)],
    'kafka_api_tls': [{'name': name, 'enabled': True,
                      'require_client_auth': name == 'mtls',
                      'cert_file': '/security/server.pem',
                      'key_file': '/security/server.key',
                      'truststore_file': '/security/ca.pem'}
                     for name in ['tls', 'mtls', 'sasl_tls']],
}}
with open(sys.argv[1] + '/redpanda.yaml', 'w') as target:
    json.dump(config, target)
print(*ports)
PY
)
# Redpanda runs as an unprivileged user inside the container.
chmod 755 "$security_dir"
chmod 644 "$security_dir"/*
created=true
docker run --detach --entrypoint /bin/sh --name "$container" --label librdkafka-rs.security-test=true \
  -v "$security_dir:/security:ro" \
  -p "127.0.0.1:$tls_port:9092" -p "127.0.0.1:$mtls_port:9093" \
  -p "127.0.0.1:$sasl_port:9094" -p "127.0.0.1:$sasl_tls_port:9095" \
  "${RDKAFKA_SECURITY_IMAGE:-docker.redpanda.com/redpandadata/redpanda:v25.3.1}" \
  -c 'cp /security/redpanda.yaml /tmp/redpanda.yaml; exec /usr/bin/rpk redpanda start --config /tmp/redpanda.yaml "$@"' \
  security-broker --overprovisioned --smp 1 \
  --memory 1G --reserve-memory 0M --check=false \
  --set enable_sasl=true --set 'sasl_mechanisms=["SCRAM","PLAIN"]' \
  --set kafka_enable_authorization=false >/dev/null
ready=false
for _ in $(seq 1 60); do
  if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]; then
    echo '::error::security broker exited during startup' >&2
    exit 1
  fi
  if docker exec "$container" rpk cluster health -X admin.hosts=127.0.0.1:9644 >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
if [ "$ready" != true ]; then echo '::error::security broker readiness deadline' >&2; exit 1; fi
docker inspect "$container" --format 'Security broker image: {{.Image}}'
docker exec "$container" rpk cluster config set sasl_mechanisms '["SCRAM","PLAIN"]' \
  -X admin.hosts=127.0.0.1:9644
docker exec "$container" rpk cluster config get sasl_mechanisms \
  -X admin.hosts=127.0.0.1:9644
run_case() {
  printf 'Security case: protocol=%s mechanism=%s mutual_tls=%s\n' "$1" "${RDKAFKA_SASL_MECHANISM:-none}" "${RDKAFKA_SECURITY_MTLS:-false}"
  export RDKAFKA_SECURITY_MODE="$1"
  export RDKAFKA_TEST_BROKERS="127.0.0.1:$2"
  timeout "${RDKAFKA_SECURITY_TEST_TIMEOUT:-120}" cargo test -p rdkafka --features tls \
    --test sasl_live e2e_lifecycle_scenario_security_delivery_and_readback \
    -- --ignored --exact --nocapture
}
run_case SSL "$tls_port"
export RDKAFKA_SECURITY_MTLS=1
run_case SSL "$mtls_port"
unset RDKAFKA_SECURITY_MTLS
docker exec "$container" rpk security user create security-user \
  --password security-password --mechanism SCRAM-SHA-256 -X admin.hosts=127.0.0.1:9644
for mechanism in PLAIN SCRAM-SHA-256 SCRAM-SHA-512; do
  if [ "$mechanism" = SCRAM-SHA-512 ]; then
    docker exec "$container" rpk security user update security-user \
      --new-password security-password --mechanism SCRAM-SHA-512 -X admin.hosts=127.0.0.1:9644 \
      -X user=security-user -X pass=security-password
  fi
  export RDKAFKA_SASL_MECHANISM="$mechanism"
  run_case SASL_PLAINTEXT "$sasl_port"
  run_case SASL_SSL "$sasl_tls_port"
done

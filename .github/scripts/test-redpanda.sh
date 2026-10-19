#!/usr/bin/env bash
# Exercise the live broker contract and remove only the container this process creates.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
if [ -z "${RDKAFKA_TEST_BROKERS:-}" ]; then
  docker info >/dev/null
  read -r container port < <(python3 - <<'PY'
import secrets
import socket
with socket.socket() as listener:
    listener.bind(('127.0.0.1', 0))
    print('librdkafka-rs-ci-' + secrets.token_hex(8), listener.getsockname()[1])
PY
)
  cleanup() {
    result=$?
    if [ "$result" -ne 0 ]; then
      docker logs --tail 80 "$container" >&2 || true
    fi
    if docker inspect "$container" >/dev/null 2>&1; then
      if ! docker rm --force "$container" >/dev/null; then
        echo "::error::cannot remove task container $container" >&2
        return 1
      fi
    fi
    return "$result"
  }
  trap cleanup EXIT
  docker run --detach --name "$container" --label librdkafka-rs.ci=true \
    -p "127.0.0.1:$port:9092" docker.redpanda.com/redpandadata/redpanda:latest \
    redpanda start --overprovisioned --smp 1 --memory 512M --reserve-memory 0M \
    --node-id 0 --check=false --kafka-addr plaintext://0.0.0.0:9092 \
    --advertise-kafka-addr "plaintext://127.0.0.1:$port" \
    --set kafka_enable_authorization=true --set 'superusers=["ANONYMOUS"]' >/dev/null
  ready=false
  for _ in $(seq 1 60); do
    if docker exec "$container" rpk cluster info -X brokers=127.0.0.1:9092 >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [ "$ready" != true ]; then
    echo '::error::Redpanda did not become ready within 60 attempts' >&2
    exit 1
  fi
  export RDKAFKA_TEST_BROKERS="127.0.0.1:$port"
  docker inspect "$container" --format 'Live broker image: {{.Image}}'
fi
timeout "${RDKAFKA_LIVE_TEST_TIMEOUT:-600}" cargo test -p rdkafka _live_ \
  -- --ignored --test-threads 1

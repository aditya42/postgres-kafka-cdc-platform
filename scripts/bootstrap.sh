#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

wait_http() {
  local url="$1" name="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "✓ $name ready"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: $name did not become ready: $url" >&2
  return 1
}

wait_http http://localhost:8081/subjects "Schema Registry"
wait_http http://localhost:8083/connectors "Kafka Connect"

curl -fsS -X PUT http://localhost:8081/config \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"compatibility":"BACKWARD_TRANSITIVE"}' >/dev/null

docker compose exec -T kafka /kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic cdc.dlq --partitions 1 --replication-factor 1 >/dev/null

curl -fsS -X PUT http://localhost:8083/connectors/postgres-cdc/config \
  -H 'Content-Type: application/json' \
  --data-binary @infra/connect/connectors/postgres-source.json >/dev/null

echo "✓ registered postgres-cdc"

curl -fsS -X PUT http://localhost:8083/connectors/s3-cdc-sink/config \
  -H 'Content-Type: application/json' \
  --data-binary @infra/connect/connectors/s3-sink.json >/dev/null

echo "✓ registered s3-cdc-sink"

echo
curl -fsS http://localhost:8083/connectors/postgres-cdc/status | python3 -m json.tool
curl -fsS http://localhost:8083/connectors/s3-cdc-sink/status | python3 -m json.tool

echo
echo "Platform ready. Run: make demo"

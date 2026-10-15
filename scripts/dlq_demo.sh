#!/usr/bin/env bash
set -euo pipefail

printf '{"malformed":"intentionally-not-avro"}\n' | \
  docker compose exec -T kafka /kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:29092 --topic cdc.public.orders >/dev/null

echo "Injected one malformed non-Avro record into cdc.public.orders."
echo "Waiting for sink error handling..."
sleep 5

docker compose exec -T kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic cdc.dlq --from-beginning \
  --max-messages 1 --timeout-ms 10000 --property print.headers=true || true

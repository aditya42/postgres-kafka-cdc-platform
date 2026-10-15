.PHONY: help build up bootstrap down reset seed load evolve schemas wait-raw spark reconcile health dlq-demo show-dlq replay test lint integration demo logs

COMPOSE=docker compose
PYTHON?=python3

help:
	@printf "%-18s %s\n" \
	  "make up" "Build/start infrastructure and register connectors" \
	  "make demo" "Run seed -> CDC -> Spark -> reconciliation demo" \
	  "make seed" "Generate deterministic insert/update/delete changes" \
	  "make load N=1000" "Generate configurable CDC load" \
	  "make evolve" "Apply a backward-compatible schema evolution" \
	  "make schemas" "Show Schema Registry subjects and versions" \
	  "make wait-raw" "Wait until raw CDC objects land in S3" \
	  "make spark" "Run incremental Spark analytics job" \
	  "make reconcile" "Compare PostgreSQL current state with CDC-rebuilt state" \
	  "make dlq-demo" "Inject malformed Kafka data and verify DLQ routing" \
	  "make replay TOPIC=cdc.public.orders START=0 END=50" "Replay raw Kafka offsets" \
	  "make health" "Print platform health/status" \
	  "make test" "Run unit tests" \
	  "make integration" "Run Docker-backed integration tests" \
	  "make reset" "Destroy volumes and recreate the platform"

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d --build postgres kafka schema-registry s3 s3-init connect
	./scripts/bootstrap.sh

bootstrap:
	./scripts/bootstrap.sh

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v --remove-orphans
	$(MAKE) up

seed:
	$(PYTHON) -m scripts.seed_changes

load:
	$(PYTHON) -m scripts.generate_load --events $${N:-1000}

evolve:
	./scripts/evolve_schema.sh

schemas:
	$(PYTHON) -m scripts.schema_versions

wait-raw:
	$(PYTHON) -m scripts.wait_for_raw

spark:
	$(COMPOSE) run --rm spark-runner

reconcile:
	$(PYTHON) -m scripts.reconcile

health:
	$(PYTHON) -m scripts.healthcheck

dlq-demo:
	./scripts/dlq_demo.sh

show-dlq:
	$(COMPOSE) exec -T kafka /kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka:29092 --topic cdc.dlq --from-beginning \
	  --timeout-ms 5000 --property print.headers=true || true

replay:
	$(PYTHON) -m scripts.replay --topic $${TOPIC:-cdc.public.orders} \
	  --start $${START:-0} --end $${END:-100}

test:
	$(PYTHON) -m pytest tests/unit

lint:
	ruff check cdc_platform scripts tests

integration:
	$(PYTHON) -m pytest -m integration tests/integration

demo: seed
	$(MAKE) wait-raw
	$(MAKE) spark
	$(MAKE) reconcile

logs:
	$(COMPOSE) logs -f --tail=200 connect

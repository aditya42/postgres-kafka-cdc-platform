# Operations runbook

## Day-one commands

```bash
cp .env.example .env
make up
make demo
make health
```

Optional Kafka UI:

```bash
docker compose --profile ui up -d kafka-ui
# http://localhost:8080
```

## Inspect connectors

```bash
curl -s localhost:8083/connectors/postgres-cdc/status | jq
curl -s localhost:8083/connectors/s3-cdc-sink/status | jq
```

## Inspect schemas

```bash
make schemas
curl -s localhost:8081/subjects | jq
```

## Validate recovery

Restart Connect and confirm offsets resume without replaying the complete database:

```bash
docker compose restart connect
make health
make seed
```

## Reset all state

```bash
make reset
```

This removes PostgreSQL and Kafka volumes and recreates the local S3 emulator. It also resets the Debezium replication slot and Connect offsets.

## Production hardening path

Replace local components with RDS/Aurora PostgreSQL, MSK/Confluent Cloud, managed Schema Registry, AWS S3, and Spark on EMR/Glue. Run at least three Kafka brokers, multiple Connect workers, RF=3 internal topics, TLS/SASL, ACLs, IAM roles, Secrets Manager, private networking, autoscaling, centralized logs/metrics, lag alerts, WAL-retention alerts, schema-compatibility gates, S3 lifecycle policies, and a table format such as Iceberg for transactional analytics tables.

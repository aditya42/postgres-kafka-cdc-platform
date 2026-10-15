# Resume and interview positioning

## Resume bullets — safe to use after you have run the project

- Built an end-to-end PostgreSQL CDC data platform using Debezium, Kafka/Kafka Connect, Avro Schema Registry, S3-compatible object storage, and PySpark to transform WAL changes into analytics-ready Parquet datasets.
- Enabled Kafka Connect exactly-once source transactions for Debezium PostgreSQL and designed downstream idempotency using topic/partition/offset event identity, deterministic object naming, de-duplication, and checkpoint-on-success processing.
- Implemented backward-transitive Avro schema evolution, Kafka Connect dead-letter queues, raw-message replay, Spark checkpoints, and source-to-lake checksum reconciliation to improve recoverability and data correctness.
- Containerized the full platform with Docker Compose and added automated unit/config validation plus an on-demand GitHub Actions integration pipeline covering connector health, schema registration, Spark processing, and reconciliation.

## Do not invent scale numbers

Run `make load N=10000` or a larger workload on your machine and record measured event throughput, S3 landing latency, Spark duration, and reconciliation time before adding numerical claims. A defensible bullet is stronger than an impressive but unverified metric.

## Senior-level interview discussion

Be ready to explain:

- Why a logical replication slot can retain WAL and become an operational risk when the connector is down.
- Why Kafka Connect EOS on the source does not automatically imply exactly-once behavior all the way through S3 and Spark.
- Why schema compatibility policy belongs in CI/governance rather than being left to producer convention.
- How DLQ, replay, and reconciliation solve different classes of failure.
- When you would replace raw Parquet snapshots with Apache Iceberg/Delta/Hudi.
- How you would scale Connect workers, Kafka partitions, S3 object sizes, Spark jobs, and replication-factor settings in production.

# Architecture and design decisions

## Data flow

1. PostgreSQL writes row changes into WAL with `wal_level=logical`.
2. Debezium PostgreSQL captures the `pgoutput` logical stream through a replication slot.
3. Kafka Connect writes change envelopes to one Kafka topic per captured table.
4. Keys and values use Avro with Schema Registry and `BACKWARD_TRANSITIVE` compatibility.
5. An S3 Kafka Connect sink writes decoded JSONL changelog files to the raw data-lake prefix.
6. Spark incrementally processes newly landed objects into bronze Parquet, rebuilds silver current-state snapshots, and publishes gold aggregates.
7. The Spark checkpoint is committed only after all outputs succeed. Reprocessing uses deterministic object/run identities, making retries idempotent.
8. Reconciliation independently rebuilds source-table state from the raw CDC log and compares row-level checksums to PostgreSQL.

## Delivery semantics

The source connector uses Kafka Connect's exactly-once source support (`exactly.once.source.support=enabled`, connector `exactly.once.support=required`). This protects the PostgreSQL-to-Kafka source path with Kafka transactions.

The project intentionally does **not** market the whole distributed pipeline as magically exactly-once. The S3 and Spark layers use a separate idempotency strategy: Kafka topic/partition/offset identifies an event, S3 object names contain source offsets, Spark de-duplicates event IDs, and the checkpoint is updated after outputs succeed.

## Schema evolution

Schema Registry is set to `BACKWARD_TRANSITIVE`. `make evolve` adds a nullable `sales_channel` field to `orders`, triggers new Debezium events, and therefore a new Avro schema version. `make schemas` exposes the subject/version history.

## Failure handling

The S3 sink uses Kafka Connect DLQ handling. `make dlq-demo` deliberately bypasses Avro serialization and injects a malformed record into a CDC topic. The converter failure is tolerated and routed to `cdc.dlq` with error-context headers.

## Replay

`scripts/replay.py` reads raw Kafka bytes with `isolation.level=read_committed` and republishes an offset range to a dedicated replay topic without decoding/re-encoding Avro. This preserves the original wire representation and schema IDs.

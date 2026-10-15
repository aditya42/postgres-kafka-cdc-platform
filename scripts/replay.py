from __future__ import annotations

import argparse

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay raw Kafka records without deserializing Avro"
    )
    parser.add_argument("--topic", required=True)
    parser.add_argument("--partition", type=int, default=0)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=100)
    parser.add_argument("--destination")
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    destination = args.destination or f"replay.{args.topic}"
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"cdc-replay-{args.topic}-{args.partition}-{args.start}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True})
    consumer.assign([TopicPartition(args.topic, args.partition, args.start)])

    delivery_errors: list[KafkaException] = []

    def on_delivery(error: object, _: object) -> None:
        if error is not None:
            delivery_errors.append(KafkaException(error))

    count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())
            if msg.offset() >= args.end:
                break
            producer.produce(
                destination,
                key=msg.key(),
                value=msg.value(),
                headers=msg.headers(),
                on_delivery=on_delivery,
            )
            count += 1
        remaining = producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} replay records were not delivered before timeout")
        if delivery_errors:
            raise delivery_errors[0]
    finally:
        consumer.close()
    print(f"Replayed {count} raw records to {destination}")


if __name__ == "__main__":
    main()

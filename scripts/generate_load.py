from __future__ import annotations

import argparse
import os
import random
import time

import psycopg

DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/commerce")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()

    random.seed(42)
    started = time.perf_counter()
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM customers ORDER BY id")
        customers = [row[0] for row in cur.fetchall()]
        if not customers:
            raise SystemExit("No customers exist; run make seed first")

        for index in range(args.events):
            customer_id = random.choice(customers)
            amount = round(random.uniform(10, 500), 2)
            cur.execute(
                """INSERT INTO orders(customer_id,status,amount,currency)
                   VALUES (%s,'PENDING',%s,'USD') RETURNING id""",
                (customer_id, amount),
            )
            order_id = cur.fetchone()[0]
            cur.execute(
                """INSERT INTO order_items(order_id,sku,quantity,unit_price)
                   VALUES (%s,%s,1,%s)""",
                (order_id, f"LOAD-{index:08d}", amount),
            )
            if index % 2 == 0:
                cur.execute(
                    "UPDATE orders SET status='PAID', updated_at=now() WHERE id=%s",
                    (order_id,),
                )
            if index % 5 == 0:
                cur.execute(
                    "UPDATE orders SET status='SHIPPED', updated_at=now() WHERE id=%s",
                    (order_id,),
                )
            if (index + 1) % args.batch_size == 0:
                conn.commit()
        conn.commit()

    elapsed = time.perf_counter() - started
    print(f"Generated {args.events} order transactions in {elapsed:.2f}s")
    print("Use observed Kafka/S3/Spark metrics before adding throughput numbers to your resume.")


if __name__ == "__main__":
    main()

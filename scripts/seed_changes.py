from __future__ import annotations

import os

import psycopg

DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://postgres:postgres@localhost:5432/commerce",
)


def main() -> None:
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO customers(email, full_name, country_code)
            VALUES ('demo.customer@example.com', 'Demo Customer', 'US')
            ON CONFLICT (email) DO UPDATE SET
              full_name = EXCLUDED.full_name,
              updated_at = now()
            RETURNING id
            """
        )
        customer_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO orders(customer_id, status, amount, currency)
            VALUES (%s, 'PENDING', 249.95, 'USD')
            RETURNING id
            """,
            (customer_id,),
        )
        order_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO order_items(order_id, sku, quantity, unit_price)
            VALUES (%s, 'CDC-DEMO-001', 1, 249.95)
            """,
            (order_id,),
        )
        cur.execute(
            "UPDATE orders SET status='PAID', updated_at=now() WHERE id=%s",
            (order_id,),
        )
        cur.execute(
            "UPDATE orders SET status='SHIPPED', updated_at=now() WHERE id=%s",
            (order_id,),
        )
        cur.execute(
            """INSERT INTO orders(customer_id, status, amount, currency)
               VALUES (%s, 'CANCELLED', 1.00, 'USD') RETURNING id""",
            (customer_id,),
        )
        transient_order_id = cur.fetchone()[0]
        cur.execute("DELETE FROM orders WHERE id=%s", (transient_order_id,))
        conn.commit()
    print(
        f"Generated CDC lifecycle for order_id={order_id}; "
        f"insert/delete demo order_id={transient_order_id}"
    )


if __name__ == "__main__":
    main()

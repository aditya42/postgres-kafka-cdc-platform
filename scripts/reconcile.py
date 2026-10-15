from __future__ import annotations

import hashlib
import json
import os
import re
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg

from cdc_platform.debezium import CDCEvent, parse_s3_record, rebuild_current_state
from scripts.common import bucket, s3_client

DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/commerce")
TOPICS = {
    "cdc.public.customers": "customers",
    "cdc.public.orders": "orders",
    "cdc.public.order_items": "order_items",
}
FIELDS = {
    "customers": ["id", "email", "full_name", "country_code"],
    "orders": ["id", "customer_id", "status", "amount", "currency", "sales_channel"],
    "order_items": ["id", "order_id", "sku", "quantity", "unit_price"],
}
DECIMAL_FIELDS = {"amount", "unit_price"}
PARTITION_RE = re.compile(r"-(\d{10})-(\d{20})\.jsonl$")


def normalize_scalar(field: str, value: Any) -> Any:
    if value is None:
        return None
    if field in DECIMAL_FIELDS:
        try:
            return format(Decimal(str(value)), "f")
        except (InvalidOperation, ValueError):
            return str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def canonicalize(table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = FIELDS[table]
    canonical: list[dict[str, Any]] = []
    for row in rows:
        canonical.append(
            {field: normalize_scalar(field, row.get(field)) for field in fields if field in row}
        )
    return sorted(canonical, key=lambda row: row.get("id", -1))


def checksum(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def postgres_state(table: str) -> list[dict[str, Any]]:
    requested = FIELDS[table]
    with psycopg.connect(DSN, row_factory=psycopg.rows.dict_row) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema='public' AND table_name=%s""",
            (table,),
        )
        existing = {row["column_name"] for row in cur.fetchall()}
        selected = [field for field in requested if field in existing]
        columns = ", ".join(selected)
        cur.execute(f"SELECT {columns} FROM public.{table} ORDER BY id")  # controlled allowlist
        return canonicalize(table, [dict(row) for row in cur.fetchall()])


def cdc_state(topic: str, table: str) -> list[dict[str, Any]]:
    client = s3_client()
    events: list[CDCEvent] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket(), "Prefix": f"raw/{topic}/"}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            match = PARTITION_RE.search(key)
            partition = int(match.group(1)) if match else 0
            body = client.get_object(Bucket=bucket(), Key=key)["Body"].read().decode()
            for line in body.splitlines():
                if line.strip():
                    events.append(parse_s3_record(json.loads(line), topic, partition))
        if not response.get("IsTruncated"):
            break
        token = response["NextContinuationToken"]
    state = rebuild_current_state(events)
    return canonicalize(table, [state[key] for key in sorted(state)])


def main() -> None:
    failures = 0
    for topic, table in TOPICS.items():
        source = postgres_state(table)
        rebuilt = cdc_state(topic, table)
        source_sum = checksum(source)
        rebuilt_sum = checksum(rebuilt)
        ok = source_sum == rebuilt_sum
        print(
            f"{table:12s} source_rows={len(source):5d} cdc_rows={len(rebuilt):5d} "
            f"checksum_match={ok}"
        )
        if not ok:
            print(f"  source_sha256={source_sum}")
            print(f"  cdc_sha256   ={rebuilt_sum}")
            failures += 1
    if failures:
        raise SystemExit(f"Reconciliation failed for {failures} table(s)")
    print("Reconciliation PASSED")


if __name__ == "__main__":
    main()

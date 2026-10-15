from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import boto3
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ENDPOINT = os.getenv("S3_ENDPOINT", "http://s3:5000")
BUCKET = os.getenv("S3_BUCKET", "cdc-lake")
CHECKPOINT_KEY = "checkpoints/spark/processed-raw-objects.json"
TABLES = ["customers", "orders", "order_items"]


def s3() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def list_keys(client: Any, prefix: str) -> list[str]:
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": BUCKET, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        keys.extend(obj["Key"] for obj in response.get("Contents", []))
        if not response.get("IsTruncated"):
            return keys
        token = response["NextContinuationToken"]


def load_checkpoint(client: Any) -> set[str]:
    try:
        body = client.get_object(Bucket=BUCKET, Key=CHECKPOINT_KEY)["Body"].read()
        return set(json.loads(body).get("processed", []))
    except client.exceptions.NoSuchKey:
        return set()
    except Exception as exc:  # boto3 maps some S3-compatible misses to ClientError
        if "NoSuchKey" in str(exc) or "404" in str(exc):
            return set()
        raise


def save_checkpoint(client: Any, processed: set[str]) -> None:
    client.put_object(
        Bucket=BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps({"processed": sorted(processed)}, indent=2).encode(),
        ContentType="application/json",
    )


def download(client: Any, keys: list[str], base: Path) -> list[str]:
    paths: list[str] = []
    for key in keys:
        path = base / key
        path.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(BUCKET, key, str(path))
        paths.append(str(path))
    return paths


def upload_tree(client: Any, local: Path, prefix: str) -> None:
    for path in local.rglob("*"):
        if path.is_file():
            key = f"{prefix}/{path.relative_to(local).as_posix()}"
            client.upload_file(str(path), BUCKET, key)


def replace_prefix(client: Any, prefix: str) -> None:
    keys = list_keys(client, prefix)
    if not keys:
        return
    for start in range(0, len(keys), 1000):
        client.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": key} for key in keys[start : start + 1000]]},
        )


def main() -> None:
    client = s3()
    raw_keys = sorted(list_keys(client, "raw/"))
    processed = load_checkpoint(client)
    new_keys = [key for key in raw_keys if key not in processed and key.endswith(".jsonl")]
    if not new_keys:
        print("No new raw CDC objects. Analytics tables are already current.")
        return

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("postgres-kafka-cdc-analytics")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    work = Path(tempfile.mkdtemp(prefix="cdc-spark-"))
    try:
        incremental_dir = work / "incremental"
        all_raw_dir = work / "all-raw"
        bronze_dir = work / "bronze"
        silver_dir = work / "silver"
        gold_dir = work / "gold"

        incremental_paths = download(client, new_keys, incremental_dir)
        incremental = spark.read.option("multiLine", "false").json(incremental_paths)
        incremental = incremental.withColumn("_source_file", F.input_file_name())
        incremental = incremental.withColumn(
            "_topic",
            F.regexp_extract("_source_file", r"raw/(cdc\.public\.[^/]+)/", 1),
        )
        incremental = incremental.withColumn(
            "_partition",
            F.regexp_extract("_source_file", r"-(\d{10})-\d{20}\.jsonl$", 1).cast("int"),
        ).withColumn(
            "_event_id",
            F.concat_ws(
                ":",
                "_topic",
                F.col("_partition").cast("string"),
                F.col("offset").cast("string"),
            ),
        ).dropDuplicates(["_event_id"])
        incremental.write.mode("overwrite").partitionBy("_topic").parquet(str(bronze_dir))

        run_id = hashlib.sha256("|".join(new_keys).encode()).hexdigest()[:16]
        bronze_prefix = f"analytics/bronze/run={run_id}/"
        replace_prefix(client, bronze_prefix)
        upload_tree(client, bronze_dir, bronze_prefix.rstrip("/"))

        all_paths = download(client, [k for k in raw_keys if k.endswith(".jsonl")], all_raw_dir)
        history = spark.read.option("multiLine", "false").json(all_paths)
        history = history.withColumn("_source_file", F.input_file_name()).withColumn(
            "_topic",
            F.regexp_extract("_source_file", r"raw/(cdc\.public\.[^/]+)/", 1),
        )

        snapshots: dict[str, Any] = {}
        for table in TABLES:
            topic = f"cdc.public.{table}"
            events = history.filter(F.col("_topic") == topic)
            rows = events.withColumn(
                "_pk",
                F.coalesce(F.col("value.after.id"), F.col("value.before.id")),
            )
            w = Window.partitionBy("_pk").orderBy(
                F.col("value.source.lsn").desc_nulls_last(), F.col("offset").desc()
            )
            latest = rows.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1)
            current = latest.filter(F.col("value.op") != "d").select("value.after.*")
            snapshots[table] = current
            out = silver_dir / table
            current.write.mode("overwrite").parquet(str(out))
            replace_prefix(client, f"analytics/silver/{table}/current/")
            upload_tree(client, out, f"analytics/silver/{table}/current")

        orders = snapshots["orders"]
        metrics = (
            orders.groupBy("status")
            .agg(
                F.count("*").alias("order_count"),
                F.round(F.sum("amount"), 2).alias("gross_order_value"),
                F.round(F.avg("amount"), 2).alias("avg_order_value"),
            )
            .orderBy("status")
        )
        metrics.write.mode("overwrite").parquet(str(gold_dir / "orders_by_status"))
        replace_prefix(client, "analytics/gold/orders_by_status/current/")
        upload_tree(
            client,
            gold_dir / "orders_by_status",
            "analytics/gold/orders_by_status/current",
        )

        save_checkpoint(client, processed | set(new_keys))
        print(f"Processed {len(new_keys)} new S3 objects")
        print(f"Bronze run id: {run_id}")
        print("Updated silver current-state tables and gold order metrics")
    finally:
        spark.stop()
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()

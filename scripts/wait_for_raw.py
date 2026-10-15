from __future__ import annotations

import argparse
import time

from scripts.common import bucket, s3_client

TOPIC_PREFIXES = (
    "raw/cdc.public.customers/",
    "raw/cdc.public.orders/",
    "raw/cdc.public.order_items/",
)


def prefix_has_object(prefix: str) -> bool:
    response = s3_client().list_objects_v2(Bucket=bucket(), Prefix=prefix, MaxKeys=1)
    return bool(response.get("Contents"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wait until CDC objects are visible in the raw lake"
    )
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        missing = [prefix for prefix in TOPIC_PREFIXES if not prefix_has_object(prefix)]
        if not missing:
            print("Raw CDC objects are available for all captured tables")
            return
        print("Waiting for raw CDC objects: " + ", ".join(missing))
        time.sleep(args.interval)

    raise SystemExit(f"Timed out after {args.timeout}s waiting for raw CDC objects")


if __name__ == "__main__":
    main()

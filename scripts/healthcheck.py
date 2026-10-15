from __future__ import annotations

import json
import socket
from typing import Any

import requests


def check_http(name: str, url: str) -> tuple[str, str]:
    try:
        response = requests.get(url, timeout=3)
        response.raise_for_status()
        return name, "UP"
    except Exception as exc:  # noqa: BLE001
        return name, f"DOWN ({exc})"


def connector(name: str) -> dict[str, Any]:
    try:
        r = requests.get(f"http://localhost:8083/connectors/{name}/status", timeout=3)
        r.raise_for_status()
        payload = r.json()
        tasks = ",".join(task["state"] for task in payload.get("tasks", []))
        return {"connector": payload["connector"]["state"], "tasks": tasks or "none"}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def main() -> None:
    rows = [
        check_http("Schema Registry", "http://localhost:8081/subjects"),
        check_http("Kafka Connect", "http://localhost:8083/connectors"),
        check_http("S3 emulator", "http://localhost:9000/"),
    ]
    try:
        with socket.create_connection(("localhost", 5432), timeout=3):
            rows.append(("PostgreSQL", "UP"))
    except OSError as exc:
        rows.append(("PostgreSQL", f"DOWN ({exc})"))

    for name, state in rows:
        print(f"{name:20s} {state}")
    print("postgres-cdc       " + json.dumps(connector("postgres-cdc")))
    print("s3-cdc-sink        " + json.dumps(connector("s3-cdc-sink")))


if __name__ == "__main__":
    main()

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CDCEvent:
    topic: str
    partition: int
    offset: int
    op: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    lsn: int | None
    ts_ms: int | None

    @property
    def identity(self) -> str:
        """Stable idempotency key for one Kafka record."""
        return f"{self.topic}:{self.partition}:{self.offset}"

    @property
    def row(self) -> dict[str, Any] | None:
        return self.after if self.op != "d" else self.before

    @property
    def primary_key(self) -> Any:
        row = self.row or {}
        return row.get("id")


def parse_s3_record(record: dict[str, Any], topic: str, partition: int = 0) -> CDCEvent:
    value = record.get("value") or {}
    source = value.get("source") or {}
    return CDCEvent(
        topic=topic,
        partition=partition,
        offset=int(record.get("offset", -1)),
        op=value.get("op", "?"),
        before=value.get("before"),
        after=value.get("after"),
        lsn=source.get("lsn"),
        ts_ms=value.get("ts_ms"),
    )


def rebuild_current_state(events: list[CDCEvent]) -> dict[Any, dict[str, Any]]:
    """Rebuild a table state from ordered Debezium events.

    The caller should provide records in Kafka partition/offset order. Replayed
    duplicates are harmless because identity is de-duplicated before applying.
    """
    state: dict[Any, dict[str, Any]] = {}
    seen: set[str] = set()
    for event in sorted(events, key=lambda e: (e.partition, e.offset)):
        if event.identity in seen:
            continue
        seen.add(event.identity)
        pk = event.primary_key
        if pk is None:
            continue
        if event.op == "d":
            state.pop(pk, None)
        elif event.after is not None:
            state[pk] = event.after
    return state

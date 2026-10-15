from cdc_platform.debezium import CDCEvent, parse_s3_record, rebuild_current_state


def event(offset: int, op: str, before=None, after=None) -> CDCEvent:
    return CDCEvent(
        topic="cdc.public.orders",
        partition=0,
        offset=offset,
        op=op,
        before=before,
        after=after,
        lsn=offset * 10,
        ts_ms=offset * 1000,
    )


def test_parse_s3_record() -> None:
    record = {
        "offset": 7,
        "value": {
            "op": "u",
            "before": {"id": 1, "status": "PENDING"},
            "after": {"id": 1, "status": "PAID"},
            "source": {"lsn": 9001},
            "ts_ms": 123456,
        },
    }
    parsed = parse_s3_record(record, "cdc.public.orders")
    assert parsed.identity == "cdc.public.orders:0:7"
    assert parsed.primary_key == 1
    assert parsed.lsn == 9001


def test_rebuild_current_state_handles_updates_deletes_and_duplicates() -> None:
    events = [
        event(0, "c", after={"id": 1, "status": "PENDING"}),
        event(1, "u", before={"id": 1}, after={"id": 1, "status": "PAID"}),
        event(1, "u", before={"id": 1}, after={"id": 1, "status": "PAID"}),
        event(2, "c", after={"id": 2, "status": "PENDING"}),
        event(3, "d", before={"id": 2, "status": "PENDING"}),
    ]
    assert rebuild_current_state(events) == {1: {"id": 1, "status": "PAID"}}

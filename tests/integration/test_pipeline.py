from __future__ import annotations

import os

import pytest
import requests

pytestmark = pytest.mark.integration


def test_connectors_are_running() -> None:
    for connector in ("postgres-cdc", "s3-cdc-sink"):
        payload = requests.get(
            f"http://localhost:8083/connectors/{connector}/status", timeout=5
        ).json()
        assert payload["connector"]["state"] == "RUNNING"
        assert all(task["state"] == "RUNNING" for task in payload["tasks"])


def test_schema_registry_has_cdc_subjects() -> None:
    subjects = requests.get("http://localhost:8081/subjects", timeout=5).json()
    assert any(subject.startswith("cdc.public.orders") for subject in subjects)


def test_s3_emulator_is_healthy() -> None:
    response = requests.get("http://localhost:9000/", timeout=5)
    assert response.ok

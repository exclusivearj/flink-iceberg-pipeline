"""Tests for the pure quality-gate functions and DLQ record builder.

These run without Flink installed (the ProcessFunction itself is exercised
inside the container; here we test the pure logic for fast feedback).
"""

from __future__ import annotations

import json
import time

import pytest

from flink_job.quality_gates import (
    FRESHNESS_FUTURE_TOL_MS,
    FRESHNESS_MAX_LAG_MS,
    build_dlq_record,
    check_device_type,
    check_event_type,
    check_freshness,
    check_null_fields,
    check_schema,
    evaluate_event,
)
from flink_job.schemas import PageEvent


def _evt(**overrides) -> PageEvent:
    now_ms = int(time.time() * 1000)
    base = dict(
        event_id="e1",
        user_id="u1",
        session_id="s1",
        event_type="click",
        page_url="https://example.com",
        country_code="US",
        device_type="desktop",
        timestamp_ms=now_ms,
        duration_ms=100,
        extra={},
    )
    base.update(overrides)
    return PageEvent(**base)


def test_happy_path_passes_all_gates():
    res = evaluate_event(_evt())
    assert res.passed
    assert res.reason == "ok"


def test_check_null_fields_fail_on_null_user_id():
    res = check_null_fields(_evt(user_id=None))
    assert not res.passed
    assert "user_id" in res.reason


def test_check_schema_fail_on_string_duration():
    res = check_schema(_evt(duration_ms="oops"))  # type: ignore[arg-type]
    assert not res.passed
    assert "duration_ms" in res.reason


def test_check_schema_fail_on_negative_duration():
    res = check_schema(_evt(duration_ms=-1))
    assert not res.passed


def test_check_schema_allows_null_duration():
    res = check_schema(_evt(duration_ms=None))
    assert res.passed


def test_check_event_type_fail_on_unknown():
    res = check_event_type(_evt(event_type="unknown_type"))
    assert not res.passed


def test_check_device_type_fail_on_unknown():
    res = check_device_type(_evt(device_type="hologram"))
    assert not res.passed


def test_check_freshness_fail_on_stale():
    stale_ms = int(time.time() * 1000) - (FRESHNESS_MAX_LAG_MS + 10_000)
    res = check_freshness(_evt(timestamp_ms=stale_ms))
    assert not res.passed
    assert "check_freshness" in res.reason


def test_check_freshness_fail_on_future():
    future_ms = int(time.time() * 1000) + (FRESHNESS_FUTURE_TOL_MS + 10_000)
    res = check_freshness(_evt(timestamp_ms=future_ms))
    assert not res.passed


def test_check_freshness_allows_recent_past():
    recent_ms = int(time.time() * 1000) - 5_000
    res = check_freshness(_evt(timestamp_ms=recent_ms))
    assert res.passed


def test_evaluate_short_circuits_on_first_failure():
    # Both null user_id AND bad event_type — should report null first
    res = evaluate_event(_evt(user_id=None, event_type="unknown_type"))
    assert not res.passed
    assert res.gate == "check_null_fields"


def test_build_dlq_record_has_reason_and_timestamp():
    record = build_dlq_record('{"foo": 1}', "check_null_fields:user_id")
    data = json.loads(record)
    assert data["reason"].startswith("check_null_fields")
    assert "failed_at" in data
    assert data["event"] == '{"foo": 1}'


def test_dlq_rate_approximation(monkeypatch):
    """Simulate 100 events with 10% bad user_id and verify ~10 DLQ records."""
    dlq_count = 0
    for i in range(100):
        # 10% of events have null user_id
        event = _evt(user_id=None if i % 10 == 0 else f"u{i}")
        result = evaluate_event(event)
        if not result.passed:
            dlq_count += 1
    assert 7 <= dlq_count <= 13  # tolerance for any timing-related flakes

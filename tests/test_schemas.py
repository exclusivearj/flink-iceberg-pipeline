"""Tests for PageEvent (de)serialization."""

from __future__ import annotations

import json
import time

import pytest

from flink_job.schemas import (
    ALLOWED_DEVICE_TYPES,
    ALLOWED_EVENT_TYPES,
    DeserializationError,
    PageEvent,
    deserialize_event,
    serialize_event,
)


def _good_event(**overrides) -> dict:
    event = {
        "event_id": "evt-1",
        "user_id": "user-1",
        "session_id": "sess-1",
        "event_type": "click",
        "page_url": "https://example.com/x",
        "country_code": "US",
        "device_type": "desktop",
        "timestamp_ms": int(time.time() * 1000),
        "duration_ms": 1500,
        "extra": {"k": "v"},
    }
    event.update(overrides)
    return event


def test_roundtrip():
    event = PageEvent(**_good_event())
    out = deserialize_event(serialize_event(event))
    assert out == event


def test_allowed_types_sets():
    assert "click" in ALLOWED_EVENT_TYPES
    assert "desktop" in ALLOWED_DEVICE_TYPES


def test_deserialize_invalid_json():
    with pytest.raises(DeserializationError):
        deserialize_event("not json")


def test_deserialize_missing_required():
    raw = json.dumps({"event_id": "x"})
    with pytest.raises(DeserializationError) as exc:
        deserialize_event(raw)
    assert "missing required fields" in str(exc.value)


def test_deserialize_null_user_id_allowed():
    raw = json.dumps(_good_event(user_id=None))
    event = deserialize_event(raw)
    assert event.user_id is None


def test_deserialize_bytes_input():
    raw = json.dumps(_good_event()).encode("utf-8")
    event = deserialize_event(raw)
    assert event.event_type == "click"


def test_deserialize_non_object_payload():
    with pytest.raises(DeserializationError):
        deserialize_event("[1, 2, 3]")

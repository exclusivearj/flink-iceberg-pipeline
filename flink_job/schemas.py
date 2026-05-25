"""PageEvent schema definitions: dataclass + JSON Schema + (de)serializers."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any


ALLOWED_EVENT_TYPES: set[str] = {
    "page_view", "click", "search", "play", "pause", "seek", "error",
}

ALLOWED_DEVICE_TYPES: set[str] = {"desktop", "mobile", "tablet", "tv"}


@dataclass
class PageEvent:
    event_id: str
    user_id: str | None        # nullable on purpose to exercise null checks
    session_id: str
    event_type: str
    page_url: str
    country_code: str
    device_type: str
    timestamp_ms: int
    duration_ms: int | None = None
    extra: dict = field(default_factory=dict)


PAGE_EVENT_JSON_SCHEMA: dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "PageEvent",
    "type": "object",
    "required": [
        "event_id", "session_id", "event_type", "page_url",
        "country_code", "device_type", "timestamp_ms",
    ],
    "properties": {
        "event_id": {"type": "string"},
        "user_id": {"type": ["string", "null"]},
        "session_id": {"type": "string"},
        "event_type": {"type": "string", "enum": sorted(ALLOWED_EVENT_TYPES)},
        "page_url": {"type": "string"},
        "country_code": {"type": "string", "minLength": 2, "maxLength": 2},
        "device_type": {"type": "string", "enum": sorted(ALLOWED_DEVICE_TYPES)},
        "timestamp_ms": {"type": "integer", "minimum": 0},
        "duration_ms": {"type": ["integer", "null"], "minimum": 0},
        "extra": {"type": "object"},
    },
    "additionalProperties": True,
}


class DeserializationError(ValueError):
    """Raised when a Kafka payload can't be parsed into PageEvent."""


def serialize_event(event: PageEvent) -> str:
    return json.dumps(asdict(event), separators=(",", ":"), ensure_ascii=False)


_REQUIRED = {
    "event_id", "session_id", "event_type", "page_url",
    "country_code", "device_type", "timestamp_ms",
}


def deserialize_event(raw: str | bytes) -> PageEvent:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise DeserializationError(f"invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise DeserializationError(f"payload must be a JSON object, got {type(data).__name__}")
    missing = _REQUIRED - set(data)
    if missing:
        raise DeserializationError(f"missing required fields: {sorted(missing)}")
    try:
        return PageEvent(
            event_id=str(data["event_id"]),
            user_id=data.get("user_id"),
            session_id=str(data["session_id"]),
            event_type=str(data["event_type"]),
            page_url=str(data["page_url"]),
            country_code=str(data["country_code"]),
            device_type=str(data["device_type"]),
            timestamp_ms=int(data["timestamp_ms"]),
            duration_ms=data.get("duration_ms"),
            extra=dict(data.get("extra") or {}),
        )
    except (TypeError, ValueError) as e:
        raise DeserializationError(f"type coercion failed: {e}") from e

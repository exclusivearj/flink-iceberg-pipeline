"""Quality gates for the Flink streaming pipeline.

Implements `QualityGateProcessor`, a Flink ProcessFunction that runs
each gate in order against a PageEvent. On the first failure, it emits
a JSON DLQ record to the DLQ_TAG side output. Passing events flow
downstream via the main output. Every evaluation emits a structured
metric to METRICS_TAG for downstream observability.

The gates are also exposed as plain functions so they can be unit-tested
without Flink (used by `tests/test_quality_gates.py`).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

from flink_job.schemas import (
    ALLOWED_DEVICE_TYPES,
    ALLOWED_EVENT_TYPES,
    DeserializationError,
    PageEvent,
    deserialize_event,
    serialize_event,
)

# These OutputTag imports are only needed when running inside Flink. The
# unit-test path doesn't construct Flink contexts.
try:
    from pyflink.common.typeinfo import Types  # type: ignore
    from pyflink.datastream.functions import ProcessFunction  # type: ignore
    from pyflink.datastream.output_tag import OutputTag  # type: ignore

    DLQ_TAG = OutputTag("dlq", Types.STRING())
    METRICS_TAG = OutputTag("metrics", Types.STRING())
    _HAS_FLINK = True
except Exception:  # pragma: no cover - tests use the bare functions
    ProcessFunction = object  # type: ignore[misc,assignment]
    DLQ_TAG = "dlq"  # type: ignore[assignment]
    METRICS_TAG = "metrics"  # type: ignore[assignment]
    _HAS_FLINK = False


@dataclass
class GateResult:
    passed: bool
    reason: str   # "ok" or "<gate_name>:<detail>"
    gate: str


FRESHNESS_MAX_LAG_MS = 24 * 60 * 60 * 1000   # 24h
FRESHNESS_FUTURE_TOL_MS = 60 * 1000          # 60s into the future is OK


# ----- pure gate functions (testable without Flink) -----

def check_null_fields(event: PageEvent) -> GateResult:
    if not event.user_id:
        return GateResult(False, "check_null_fields:user_id is null/empty", "check_null_fields")
    if not event.event_type:
        return GateResult(False, "check_null_fields:event_type is null/empty", "check_null_fields")
    if event.timestamp_ms is None:
        return GateResult(False, "check_null_fields:timestamp_ms is null", "check_null_fields")
    return GateResult(True, "ok", "check_null_fields")


def check_schema(event: PageEvent) -> GateResult:
    if not isinstance(event.timestamp_ms, int):
        return GateResult(
            False,
            f"check_schema:timestamp_ms must be int, got {type(event.timestamp_ms).__name__}",
            "check_schema",
        )
    if event.duration_ms is not None:
        if not isinstance(event.duration_ms, int):
            return GateResult(
                False,
                f"check_schema:duration_ms must be int, got {type(event.duration_ms).__name__}",
                "check_schema",
            )
        if event.duration_ms < 0:
            return GateResult(
                False,
                f"check_schema:duration_ms must be >= 0, got {event.duration_ms}",
                "check_schema",
            )
    return GateResult(True, "ok", "check_schema")


def check_event_type(event: PageEvent) -> GateResult:
    if event.event_type not in ALLOWED_EVENT_TYPES:
        return GateResult(
            False,
            f"check_event_type:'{event.event_type}' not in allowed set",
            "check_event_type",
        )
    return GateResult(True, "ok", "check_event_type")


def check_device_type(event: PageEvent) -> GateResult:
    if event.device_type not in ALLOWED_DEVICE_TYPES:
        return GateResult(
            False,
            f"check_device_type:'{event.device_type}' not in allowed set",
            "check_device_type",
        )
    return GateResult(True, "ok", "check_device_type")


def check_freshness(event: PageEvent, now_ms: Optional[int] = None) -> GateResult:
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    lag = now_ms - event.timestamp_ms
    if lag > FRESHNESS_MAX_LAG_MS:
        return GateResult(
            False,
            f"check_freshness:lag_ms={lag} > {FRESHNESS_MAX_LAG_MS}",
            "check_freshness",
        )
    if lag < -FRESHNESS_FUTURE_TOL_MS:
        return GateResult(
            False,
            f"check_freshness:timestamp in future by {-lag}ms",
            "check_freshness",
        )
    return GateResult(True, "ok", "check_freshness")


GATES: list[Callable[[PageEvent], GateResult]] = [
    check_null_fields,
    check_schema,
    check_event_type,
    check_device_type,
    check_freshness,
]


def evaluate_event(event: PageEvent) -> GateResult:
    """Run all gates in order; short-circuit on first failure."""
    for gate in GATES:
        res = gate(event)
        if not res.passed:
            return res
    return GateResult(True, "ok", "all")


def build_dlq_record(raw_payload: str, reason: str) -> str:
    return json.dumps(
        {
            "event": raw_payload,
            "reason": reason,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        },
        separators=(",", ":"),
    )


def build_metric_record(result: GateResult) -> str:
    return json.dumps(
        {
            "gate_result": "pass" if result.passed else "fail",
            "reason": result.gate if result.passed else result.reason,
            "ts": int(time.time() * 1000),
        },
        separators=(",", ":"),
    )


# ----- Flink ProcessFunction (used by job.py) -----

class QualityGateProcessor(ProcessFunction):  # type: ignore[misc,valid-type]
    """ProcessFunction with side outputs for DLQ and metrics."""

    def processElement(self, value, ctx):
        # value comes in as a serialized JSON string from the Kafka source
        raw = value if isinstance(value, str) else str(value)
        try:
            event = deserialize_event(raw)
        except DeserializationError as e:
            ctx.output(DLQ_TAG, build_dlq_record(raw, f"deserialize:{e}"))
            ctx.output(
                METRICS_TAG,
                build_metric_record(GateResult(False, f"deserialize:{e}", "deserialize")),
            )
            return

        result = evaluate_event(event)
        ctx.output(METRICS_TAG, build_metric_record(result))
        if not result.passed:
            ctx.output(DLQ_TAG, build_dlq_record(raw, result.reason))
            return
        yield serialize_event(event)

"""Synthetic clickstream generator → Kafka.

Reads config from env vars; produces PageEvent JSON to TOPIC at
EVENTS_PER_SECOND. Injects configured rates of null_user_id,
schema_drift, late_event, and bad_event_type defects.
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from faker import Faker


EVENT_TYPES = ["page_view", "click", "search", "play", "pause", "seek", "error"]
DEVICE_TYPES = ["desktop", "mobile", "tablet", "tv"]
COUNTRIES = ["US", "IN", "GB", "DE", "BR", "JP", "FR", "CA", "AU", "MX"]


def env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def build_event(fake: Faker) -> dict:
    now_ms = int(time.time() * 1000)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "page_url": fake.url(),
        "country_code": random.choice(COUNTRIES),
        "device_type": random.choice(DEVICE_TYPES),
        "timestamp_ms": now_ms,
        "duration_ms": random.randint(0, 60_000) if random.random() < 0.8 else None,
        "extra": {"ua": fake.user_agent()[:80]},
    }


def inject_defects(event: dict, rates: dict, counters: Counter) -> dict:
    if random.random() < rates["null"]:
        event["user_id"] = None
        counters["null_user_id"] += 1
    if random.random() < rates["drift"]:
        event["duration_ms"] = "not-a-number"  # type drift
        counters["schema_drift"] += 1
    if random.random() < rates["late"]:
        lag = random.uniform(2.0, 26.0)
        event["timestamp_ms"] = int(
            (datetime.now(timezone.utc) - timedelta(hours=lag)).timestamp() * 1000
        )
        counters["late_event"] += 1
    if random.random() < rates["bad_type"]:
        event["event_type"] = "unknown_type"
        counters["bad_event_type"] += 1
    return event


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    log = logging.getLogger("generator")

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.environ.get("TOPIC", "page_events")
    eps = env_int("EVENTS_PER_SECOND", 500)
    rates = {
        "null": env_float("NULL_RATE", 0.03),
        "drift": env_float("SCHEMA_DRIFT_RATE", 0.01),
        "late": env_float("LATE_EVENT_RATE", 0.02),
        "bad_type": env_float("BAD_EVENT_TYPE_RATE", 0.005),
    }
    log.info(
        "Starting generator → %s @ %s topic %s rate %d/s defects %s",
        bootstrap, topic, topic, eps, rates,
    )

    producer = Producer(
        {"bootstrap.servers": bootstrap, "linger.ms": 5, "acks": "1"}
    )
    fake = Faker()
    counters: Counter = Counter()
    sent = 0
    interval = 1.0 / eps if eps > 0 else 0.1
    next_report = time.monotonic() + 10
    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True
        log.info("Shutdown requested.")

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while not stop:
        loop_start = time.monotonic()
        for _ in range(max(1, eps // 10)):  # batch ~100ms worth
            event = inject_defects(build_event(fake), rates, counters)
            try:
                producer.produce(topic, json.dumps(event).encode("utf-8"))
                sent += 1
                counters["sent"] += 1
            except BufferError:
                producer.poll(0.05)
            if stop:
                break

        producer.poll(0)
        now = time.monotonic()
        if now >= next_report:
            log.info(
                "Generator stats — sent=%d defects=%s",
                sent,
                {k: counters[k] for k in ("null_user_id", "schema_drift", "late_event", "bad_event_type")},
            )
            next_report = now + 10

        elapsed = time.monotonic() - loop_start
        sleep_for = max(0.0, 0.1 - elapsed)  # roughly maintain 10Hz batching
        time.sleep(sleep_for)

    log.info("Flushing producer...")
    producer.flush(10)
    log.info("Done. Total sent=%d, defects=%s", sent, dict(counters))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Generator crashed")
        sys.exit(1)

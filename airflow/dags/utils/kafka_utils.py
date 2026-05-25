"""Kafka admin and consumer-lag helpers."""

from __future__ import annotations

import json
import logging
import os
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

from airflow.hooks.base import BaseHook
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient

log = logging.getLogger(__name__)


def _bootstrap() -> str:
    try:
        conn = BaseHook.get_connection("kafka_default")
        extra = json.loads(conn.extra or "{}")
        return extra.get("bootstrap.servers", "kafka:29092")
    except Exception:
        return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def get_admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": _bootstrap()})


def topic_exists(topic_name: str) -> bool:
    md = get_admin_client().list_topics(timeout=10)
    return topic_name in md.topics


def get_consumer_group_lag(group_id: str, topic: str) -> dict:
    consumer = Consumer({
        "bootstrap.servers": _bootstrap(),
        "group.id": group_id,
        "enable.auto.commit": False,
    })
    try:
        md = consumer.list_topics(topic, timeout=10)
        if topic not in md.topics:
            return {"total_lag": 0, "partitions": {}}
        partitions = [
            TopicPartition(topic, p.id) for p in md.topics[topic].partitions.values()
        ]
        committed = consumer.committed(partitions, timeout=10)
        result: dict[int, int] = {}
        total = 0
        for tp in committed:
            low, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            committed_offset = tp.offset if tp.offset >= 0 else low
            lag = max(0, high - committed_offset)
            result[tp.partition] = lag
            total += lag
        return {"total_lag": total, "partitions": result}
    finally:
        consumer.close()


def sample_dlq_messages(
    topic: str,
    max_messages: int = 100,
    timeout_seconds: int = 10,
) -> list[dict]:
    group = f"airflow-dlq-sampler-{uuid.uuid4().hex[:8]}"
    consumer = Consumer({
        "bootstrap.servers": _bootstrap(),
        "group.id": group,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    messages: list[dict] = []
    deadline = datetime.now(timezone.utc).timestamp() + timeout_seconds
    try:
        while len(messages) < max_messages and datetime.now(timezone.utc).timestamp() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.warning("DLQ poll error: %s", msg.error())
                continue
            try:
                messages.append(json.loads(msg.value().decode("utf-8")))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.warning("Skipping malformed DLQ message: %s", e)
    finally:
        consumer.close()
    return messages


def count_dlq_reasons(messages: list[dict]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for m in messages:
        reason = m.get("reason", "unknown")
        prefix = reason.split(":", 1)[0]
        counter[prefix] += 1
    return dict(counter)

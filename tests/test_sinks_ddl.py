"""Smoke tests for sink helpers — verify DDL strings have the right shape."""

from __future__ import annotations

from flink_job.sinks import ICEBERG_TABLE_DDL, create_dlq_kafka_sink_ddl


def test_iceberg_ddl_contains_partitioning():
    ddl = ICEBERG_TABLE_DDL.format(catalog="iceberg_catalog")
    assert "page_events_aggregated" in ddl
    assert "PARTITIONED BY (event_type, event_date)" in ddl
    assert "format-version" in ddl


def test_dlq_kafka_ddl_uses_topic_and_bootstrap():
    ddl = create_dlq_kafka_sink_ddl("kafka:29092", topic="dlq_events")
    assert "kafka:29092" in ddl
    assert "dlq_events" in ddl
    assert "'connector' = 'kafka'" in ddl

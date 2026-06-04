"""Iceberg sink registration and Kafka DLQ sink helpers (PyFlink Table API)."""

from __future__ import annotations

import os
from typing import Any

# These imports execute inside the Flink image at runtime. The module is
# also importable in a plain Python venv (no pyflink) — tests only touch
# the helper SQL strings, not the Flink wiring.
try:
    from pyflink.table import TableEnvironment  # type: ignore  # noqa: F401
    _HAS_FLINK = True
except Exception:  # pragma: no cover
    _HAS_FLINK = False


ICEBERG_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.`default`.page_events_aggregated (
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3),
    event_type      STRING,
    country_code    STRING,
    event_count     BIGINT,
    unique_users    BIGINT,
    processed_at    TIMESTAMP_LTZ(3),
    event_date      DATE
)
PARTITIONED BY (event_type, event_date)
WITH (
    'format-version' = '2'
)
"""


def create_iceberg_sink(
    table_env: Any,
    catalog_name: str = "iceberg_catalog",
    catalog_uri: str | None = None,
    warehouse: str = "s3://warehouse/",
    s3_endpoint: str | None = None,
) -> str:
    """Register an Iceberg REST catalog in Flink's TableEnvironment.

    Creates the `page_events_aggregated` table if it doesn't already exist.
    Returns the fully-qualified table name for downstream INSERT INTO.
    """
    catalog_uri = catalog_uri or os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
    s3_endpoint = s3_endpoint or os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

    create_catalog = f"""
        CREATE CATALOG {catalog_name} WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
            'uri' = '{catalog_uri}',
            'warehouse' = '{warehouse}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = '{s3_endpoint}',
            's3.access-key-id' = 'minioadmin',
            's3.secret-access-key' = 'minioadmin',
            's3.path-style-access' = 'true'
        )
    """
    table_env.execute_sql(create_catalog)
    table_env.execute_sql(f"USE CATALOG {catalog_name}")
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS `default`")
    table_env.execute_sql(ICEBERG_TABLE_DDL.format(catalog=catalog_name))
    return f"{catalog_name}.`default`.page_events_aggregated"


def create_dlq_kafka_sink_ddl(
    bootstrap_servers: str,
    table_name: str = "dlq_kafka_sink",
    topic: str = "dlq_events",
) -> str:
    """Returns a Flink SQL CREATE TABLE statement for a Kafka DLQ sink.

    The sink writes raw JSON strings (the payload column) to the DLQ topic.
    Use with the Table API after registering the default catalog.
    """
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        payload STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'format' = 'raw'
    )
    """

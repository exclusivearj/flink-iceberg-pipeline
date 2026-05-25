"""Bootstrap the Iceberg REST catalog and create the raw page_events table.

Run once after the iceberg-rest service is healthy:
    python iceberg/init_catalog.py

Idempotent: safe to re-run.
"""

from __future__ import annotations

import logging
import os
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    IntegerType,
    LongType,
    MapType,
    NestedField,
    StringType,
    TimestampType,
)


NAMESPACE = "default"
TABLE_NAME = "page_events_raw"


def _build_schema() -> Schema:
    return Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "user_id", StringType(), required=False),
        NestedField(3, "session_id", StringType(), required=True),
        NestedField(4, "event_type", StringType(), required=True),
        NestedField(5, "page_url", StringType(), required=True),
        NestedField(6, "country_code", StringType(), required=True),
        NestedField(7, "device_type", StringType(), required=True),
        NestedField(8, "timestamp_ms", LongType(), required=True),
        NestedField(9, "duration_ms", IntegerType(), required=False),
        NestedField(
            10,
            "extra",
            MapType(
                key_id=11,
                key_type=StringType(),
                value_id=12,
                value_type=StringType(),
                value_required=False,
            ),
            required=False,
        ),
        NestedField(13, "ingested_at", TimestampType(), required=True),
    )


def _build_partition_spec(schema: Schema) -> PartitionSpec:
    return PartitionSpec(
        PartitionField(
            source_id=13,
            field_id=1000,
            transform=DayTransform(),
            name="ingested_day",
        ),
        PartitionField(
            source_id=4,
            field_id=1001,
            transform=IdentityTransform(),
            name="event_type",
        ),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("init_catalog")
    uri = os.environ.get("ICEBERG_CATALOG_URI", "http://localhost:8181")
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")

    log.info("Connecting to Iceberg REST catalog at %s", uri)
    catalog = load_catalog(
        "rest",
        **{
            "uri": uri,
            "warehouse": "s3://warehouse/",
            "s3.endpoint": endpoint,
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
        },
    )

    existing_ns = [".".join(n) for n in catalog.list_namespaces()]
    if NAMESPACE not in existing_ns:
        catalog.create_namespace(NAMESPACE)
        log.info("Created namespace %s", NAMESPACE)
    else:
        log.info("Namespace %s already exists", NAMESPACE)

    fq = (NAMESPACE, TABLE_NAME)
    existing_tables = [t for t in catalog.list_tables(NAMESPACE)]
    if any(t == fq for t in existing_tables):
        log.info("Table %s.%s already exists; nothing to do.", *fq)
        return

    schema = _build_schema()
    partition = _build_partition_spec(schema)
    catalog.create_table(fq, schema=schema, partition_spec=partition)
    log.info("Created table %s.%s with %d partition fields", *fq, len(partition.fields))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Catalog init failed")
        sys.exit(1)

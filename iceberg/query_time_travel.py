"""Iceberg time-travel demo.

1. Lists the last 5 snapshots of page_events_aggregated.
2. Reads row counts at the latest snapshot AND the previous one.
3. Shows top-5 event_type counts at the latest snapshot.

Run after the Flink job has been writing for a few minutes so multiple
snapshots exist:
    python iceberg/query_time_travel.py
"""

from __future__ import annotations

import logging
import os
import sys
from collections import Counter
from datetime import datetime, timezone

import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog


NAMESPACE = "default"
TABLE_NAME = "page_events_aggregated"


def _connect():
    uri = os.environ.get("ICEBERG_CATALOG_URI", "http://localhost:8181")
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    return load_catalog(
        "rest",
        **{
            "uri": uri,
            "warehouse": "s3://warehouse/",
            "s3.endpoint": endpoint,
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("time-travel")
    catalog = _connect()
    table = catalog.load_table((NAMESPACE, TABLE_NAME))

    # pyiceberg 0.5.1 has no Table.snapshots() method; snapshots are a list on
    # the table metadata.
    snapshots = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms)
    if len(snapshots) < 2:
        log.warning(
            "Only %d snapshot(s); time-travel needs at least 2. Wait for the "
            "Flink job to write a couple of windows.", len(snapshots),
        )

    print("\n=== Last 5 snapshots ===")
    for snap in snapshots[-5:]:
        ts = datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc).isoformat()
        op = snap.summary.operation if snap.summary else "?"
        print(f"  snapshot_id={snap.snapshot_id}  ts={ts}  op={op}")

    print("\n=== Row counts ===")
    latest_count = table.scan().to_arrow().num_rows
    print(f"  latest:      {latest_count:,}")

    if len(snapshots) >= 2:
        prev = snapshots[-2]
        prev_scan = table.scan(snapshot_id=prev.snapshot_id).to_arrow()
        print(f"  prev snap:   {prev_scan.num_rows:,}")
        print(f"  delta:       {latest_count - prev_scan.num_rows:+,}")

    print("\n=== Top 5 event_type at latest snapshot ===")
    arrow = table.scan(selected_fields=("event_type", "event_count")).to_arrow()
    counts: Counter[str] = Counter()
    if arrow.num_rows:
        for etype, n in zip(arrow["event_type"].to_pylist(), arrow["event_count"].to_pylist()):
            if etype is None:
                continue
            counts[etype] += int(n or 0)
    for et, n in counts.most_common(5):
        print(f"  {et:<12} {n:>10,}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("query_time_travel failed")
        sys.exit(1)

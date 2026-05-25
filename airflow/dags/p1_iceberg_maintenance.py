"""p1_iceberg_maintenance — nightly Iceberg compaction + snapshot expiry + QA."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from utils.flink_client import get_job_status
from utils.iceberg_ops import (
    compact_table,
    expire_snapshots,
    get_latest_snapshot_info,
    get_table_row_count,
)

log = logging.getLogger(__name__)


@dag(
    dag_id="p1_iceberg_maintenance",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["project1", "iceberg", "maintenance"],
    description="Daily Iceberg compaction + snapshot expiry + quality validation.",
)
def p1_iceberg_maintenance():
    @task.sensor(poke_interval=30, timeout=120, mode="reschedule")
    def check_flink_job_alive() -> bool:
        job_id = Variable.get("p1_current_flink_job_id", default_var=None)
        if not job_id:
            raise AirflowSkipException("No p1_current_flink_job_id Variable; skipping maintenance.")
        status = get_job_status(job_id)
        if status != "RUNNING":
            raise AirflowSkipException(
                f"Flink job {job_id} status={status} — skipping maintenance to avoid write conflicts."
            )
        return True

    @task
    def compact_page_events_table() -> dict:
        result = compact_table("default", "page_events_aggregated")
        log.info(
            "Compaction complete: rewrote %d files into %d files",
            result["rewritten_files"], result["added_files"],
        )
        return result

    @task
    def expire_old_snapshots() -> dict:
        return expire_snapshots("default", "page_events_aggregated", older_than_days=7)

    @task
    def validate_latest_snapshot() -> dict:
        from sentinel.checks import RowCountCheck

        row_count = get_table_row_count("default", "page_events_aggregated")
        snap = get_latest_snapshot_info("default", "page_events_aggregated")

        # RowCountCheck
        rc_check = RowCountCheck(min=0, max=500_000_000)
        # We pass a tiny synthetic DataFrame just to leverage the check API
        import pandas as pd

        df = pd.DataFrame({"row_count": [row_count]})
        # Direct call on length:
        rc_result = rc_check.evaluate(df)
        status = "pass" if rc_result.status.value == "pass" else "fail"

        # Freshness check: snapshot must be within 2 hours
        if snap.get("timestamp_ms"):
            age_hours = (
                datetime.now(timezone.utc).timestamp() * 1000 - snap["timestamp_ms"]
            ) / 3_600_000
            if age_hours > 2.0:
                status = "fail"
                log.error(
                    "Latest snapshot is %.2fh old (>2h threshold). Iceberg writer may be stuck.",
                    age_hours,
                )

        log.info(
            "Validation: rows=%d snapshot=%s status=%s",
            row_count, snap.get("snapshot_id"), status,
        )
        if status == "fail":
            # Best-effort Slack alert; never raise from a maintenance task
            try:
                webhook = Variable.get("slack_webhook_token", default_var="")
                if webhook and webhook != "REPLACE_ME":
                    requests.post(
                        f"https://hooks.slack.com/services/{webhook}",
                        json={"text": f":warning: P1 Iceberg validation failed (rows={row_count})"},
                        timeout=5,
                    )
            except Exception:
                log.exception("Slack alert failed (continuing).")

        return {"row_count": row_count, "snapshot_id": snap.get("snapshot_id"), "status": status}

    @task
    def annotate_grafana(maintenance_result: dict) -> None:
        try:
            requests.post(
                "http://grafana:3000/api/annotations",
                json={
                    "text": f"Iceberg maintenance complete (rows={maintenance_result['row_count']})",
                    "tags": ["maintenance", "project1"],
                },
                auth=("admin", "admin"),
                timeout=5,
            )
        except Exception:
            log.exception("Grafana annotation failed (non-fatal).")

    alive = check_flink_job_alive()
    compacted = compact_page_events_table()
    expired = expire_old_snapshots()
    validated = validate_latest_snapshot()
    alive >> compacted >> expired >> validated >> annotate_grafana(validated)


dag = p1_iceberg_maintenance()

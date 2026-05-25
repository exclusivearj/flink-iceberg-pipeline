"""p1_health_report — daily pipeline health summary.

Aggregates Flink, Kafka, and Iceberg metrics into a structured report. Logged
to the scheduler logs (and optionally posted to Slack).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.models import Variable

from utils.flink_client import get_checkpoint_stats, get_job_metrics, get_job_status
from utils.iceberg_ops import get_latest_snapshot_info, get_table_row_count
from utils.kafka_utils import get_consumer_group_lag

log = logging.getLogger(__name__)


@dag(
    dag_id="p1_health_report",
    start_date=datetime(2024, 1, 1),
    schedule="30 8 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["project1", "reporting"],
    description="Daily Flink + Kafka + Iceberg health summary.",
)
def p1_health_report():
    @task
    def collect_flink_metrics() -> dict:
        job_id = Variable.get("p1_current_flink_job_id", default_var=None)
        if not job_id:
            return {"status": "no_job"}
        status = get_job_status(job_id)
        metrics = get_job_metrics(
            job_id,
            [
                "numRecordsIn",
                "numRecordsOut",
                "numberOfFailedCheckpoints",
                "lastCheckpointDuration",
                "currentInputWatermark",
            ],
        )
        ckpt = get_checkpoint_stats(job_id)
        return {"status": status, "job_id": job_id, **metrics, **{"checkpoint": ckpt}}

    @task
    def collect_kafka_metrics() -> dict:
        page = get_consumer_group_lag("flink-quality-pipeline", "page_events")
        dlq = get_consumer_group_lag("flink-quality-pipeline", "dlq_events")
        return {
            "page_events_lag": page["total_lag"],
            "dlq_events_lag": dlq["total_lag"],
        }

    @task
    def collect_iceberg_metrics() -> dict:
        try:
            rows = get_table_row_count("default", "page_events_aggregated")
            snap = get_latest_snapshot_info("default", "page_events_aggregated")
            age_h = None
            if snap.get("timestamp_ms"):
                age_h = (
                    datetime.now(timezone.utc).timestamp() * 1000 - snap["timestamp_ms"]
                ) / 3_600_000
            return {
                "row_count": rows,
                "last_snapshot_age_hours": round(age_h, 2) if age_h is not None else None,
                "snapshot_id": snap.get("snapshot_id"),
            }
        except Exception as e:
            log.warning("Iceberg metrics unavailable: %s", e)
            return {"row_count": None, "last_snapshot_age_hours": None, "snapshot_id": None}

    @task
    def build_and_send_report(flink: dict, kafka: dict, iceberg: dict) -> None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        lines = [
            "=========================================",
            "Project 1 — Daily Pipeline Health Report",
            f"Date: {date}",
            "=========================================",
            "FLINK",
            f"  Status:          {flink.get('status', 'unknown')}",
            f"  Records In:      {flink.get('numRecordsIn', 'n/a')}",
            f"  Records Out:     {flink.get('numRecordsOut', 'n/a')}",
            f"  Failed Checkpts: {flink.get('numberOfFailedCheckpoints', 'n/a')}",
            f"  Last Ckpt (ms):  {flink.get('lastCheckpointDuration', 'n/a')}",
            "",
            "KAFKA",
            f"  page_events lag: {kafka.get('page_events_lag', 'n/a')}",
            f"  dlq_events lag:  {kafka.get('dlq_events_lag', 'n/a')}",
            "",
            "ICEBERG",
            f"  Total rows:      {iceberg.get('row_count', 'n/a')}",
            f"  Last snapshot:   {iceberg.get('last_snapshot_age_hours', 'n/a')}h ago",
            f"  Snapshot ID:     {iceberg.get('snapshot_id', 'n/a')}",
            "=========================================",
        ]
        report = "\n".join(lines)
        log.info("\n%s", report)

    flink = collect_flink_metrics()
    kafka = collect_kafka_metrics()
    iceberg = collect_iceberg_metrics()
    build_and_send_report(flink, kafka, iceberg)


dag = p1_health_report()

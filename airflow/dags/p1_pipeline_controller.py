"""p1_pipeline_controller — boot + Flink lifecycle.

Manually triggered DAG that:
  1. Waits for Kafka topic, MinIO health, Flink JobManager
  2. Initializes Iceberg namespace + page_events_aggregated table
  3. Submits the Flink job (skips if already RUNNING)
  4. Waits for the job to reach RUNNING state
  5. Stores the job_id as Airflow Variable `p1_current_flink_job_id`
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.models import Variable

from utils.flink_client import (
    get_job_status,
    get_running_job_id,
    submit_pyflink_job,
)
from utils.kafka_utils import topic_exists

log = logging.getLogger(__name__)


@dag(
    dag_id="p1_pipeline_controller",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["project1", "flink", "lifecycle"],
    description="Boots P1 infra and submits the Flink quality-pipeline job.",
)
def p1_pipeline_controller():
    @task.sensor(poke_interval=15, timeout=300, mode="reschedule")
    def wait_for_kafka() -> bool:
        return topic_exists("page_events")

    @task.sensor(poke_interval=15, timeout=300, mode="reschedule")
    def wait_for_minio() -> bool:
        try:
            r = requests.get("http://minio:9000/minio/health/live", timeout=5)
            return r.status_code == 200
        except requests.RequestException:
            return False

    @task
    def init_iceberg_catalog() -> dict:
        from utils.iceberg_ops import get_catalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType, LongType, TimestampType, IntegerType, DateType

        catalog = get_catalog()
        namespace_created = False
        if "default" not in [".".join(n) for n in catalog.list_namespaces()]:
            catalog.create_namespace("default")
            namespace_created = True

        table_id = ("default", "page_events_aggregated")
        existing = [(ns, tn) for ns, tn in catalog.list_tables("default")]
        table_created = False
        if table_id not in existing:
            schema = Schema(
                NestedField(1, "window_start", TimestampType()),
                NestedField(2, "window_end", TimestampType()),
                NestedField(3, "event_type", StringType()),
                NestedField(4, "country_code", StringType()),
                NestedField(5, "event_count", LongType()),
                NestedField(6, "unique_users", LongType()),
                NestedField(7, "processed_at", TimestampType()),
                NestedField(8, "event_date", DateType()),
            )
            catalog.create_table(table_id, schema=schema)
            table_created = True
        return {"namespace_created": namespace_created, "table_created": table_created}

    @task.sensor(poke_interval=20, timeout=600, mode="reschedule")
    def wait_for_flink_jobmanager() -> bool:
        try:
            r = requests.get("http://flink-jobmanager:8081/overview", timeout=5)
            return r.status_code == 200 and "flink-version" in r.json()
        except requests.RequestException:
            return False

    @task
    def check_if_job_already_running() -> dict:
        jid = get_running_job_id()
        return {"already_running": bool(jid), "job_id": jid}

    @task
    def submit_flink_job(existing: dict) -> dict:
        if existing["already_running"]:
            log.info("Job already running (%s); skipping submission.", existing["job_id"])
            return {"job_id": existing["job_id"], "submitted": False}
        script_path = Variable.get("flink_job_jar_path", default_var="/opt/flink_job/job.py")
        log.info("Submitting Flink job from %s", script_path)
        jid = submit_pyflink_job(script_path)
        return {"job_id": jid, "submitted": True}

    @task.sensor(poke_interval=10, timeout=180, mode="reschedule")
    def wait_for_job_running(job_info: dict) -> bool:
        return get_job_status(job_info["job_id"]) == "RUNNING"

    @task
    def store_job_id(job_info: dict) -> None:
        Variable.set("p1_current_flink_job_id", job_info["job_id"])
        log.info("Stored p1_current_flink_job_id=%s", job_info["job_id"])

    kafka_ready = wait_for_kafka()
    minio_ready = wait_for_minio()
    flink_ready = wait_for_flink_jobmanager()
    catalog_init = init_iceberg_catalog()
    existing = check_if_job_already_running()
    submitted = submit_flink_job(existing)
    running = wait_for_job_running(submitted)
    stored = store_job_id(submitted)

    [kafka_ready, minio_ready] >> catalog_init
    [catalog_init, flink_ready] >> existing
    existing >> submitted >> running >> stored


dag = p1_pipeline_controller()

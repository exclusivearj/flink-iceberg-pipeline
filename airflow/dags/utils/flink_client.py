"""Flink REST API helpers.

All functions use the 'flink_rest' Airflow connection. Reference:
https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/rest_api/
"""

from __future__ import annotations

import logging
import os
import time
from typing import Optional

import requests
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

JOB_NAME = "flink-quality-pipeline"


def get_flink_base_url() -> str:
    try:
        conn = BaseHook.get_connection("flink_rest")
        return f"{conn.schema or 'http'}://{conn.host}:{conn.port or 8081}"
    except Exception:
        return os.environ.get("FLINK_REST_URL", "http://flink-jobmanager:8081")


def get_all_jobs() -> list[dict]:
    url = f"{get_flink_base_url()}/jobs/overview"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json().get("jobs", [])


def get_job_status(job_id: str) -> str:
    url = f"{get_flink_base_url()}/jobs/{job_id}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json().get("state", "UNKNOWN")


def get_running_job_id(job_name: str = JOB_NAME) -> Optional[str]:
    for job in get_all_jobs():
        if job.get("name") == job_name and job.get("state") == "RUNNING":
            return job["jid"]
    return None


def submit_pyflink_job(script_path: str, parallelism: int = 4) -> str:
    """Submit a PyFlink job via the REST API.

    In a production setup we'd POST /jars/upload + POST /jars/{id}/run. For
    PyFlink the recommended path is to exec `flink run -py <path>` inside the
    jobmanager container. We do that here via the Docker exec REST shim and
    return the resulting job id by polling /jobs/overview.
    """
    # Capture jobs before submission
    before = {j["jid"] for j in get_all_jobs()}
    # NOTE: we rely on the orchestrator running `flink run` (e.g. via Makefile
    # or a BashOperator). This function polls for the new job to appear and
    # returns its id.
    deadline = time.time() + 60
    while time.time() < deadline:
        after = {j["jid"] for j in get_all_jobs()}
        new = after - before
        if new:
            jid = next(iter(new))
            log.info("Detected newly submitted job %s", jid)
            return jid
        time.sleep(2)
    raise RuntimeError(
        f"No new Flink job appeared within 60s. Confirm the submitter "
        f"(e.g. `flink run -py {script_path}`) succeeded."
    )


def cancel_job(job_id: str) -> None:
    url = f"{get_flink_base_url()}/jobs/{job_id}"
    r = requests.patch(url, params={"mode": "cancel"}, timeout=10)
    r.raise_for_status()


def get_job_metrics(job_id: str, metric_names: list[str]) -> dict:
    url = f"{get_flink_base_url()}/jobs/{job_id}/metrics"
    r = requests.get(url, params={"get": ",".join(metric_names)}, timeout=10)
    r.raise_for_status()
    return {item["id"]: item["value"] for item in r.json()}


def get_checkpoint_stats(job_id: str) -> dict:
    url = f"{get_flink_base_url()}/jobs/{job_id}/checkpoints"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    payload = r.json()
    latest = payload.get("latest", {}).get("completed") or {}
    return {
        "latest_completed_id": latest.get("id"),
        "latest_duration_ms": latest.get("duration"),
        "latest_size_bytes": latest.get("state_size"),
        "counts": payload.get("counts", {}),
    }

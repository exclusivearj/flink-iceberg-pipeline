"""p1_dlq_monitor — hourly DLQ surveillance.

Computes DLQ rate = dlq_lag / (dlq_lag + main_lag), and posts Slack alerts
when it exceeds the configured threshold.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import requests
from airflow.decorators import dag, task
from airflow.models import Variable

from utils.kafka_utils import (
    count_dlq_reasons,
    get_consumer_group_lag,
    sample_dlq_messages,
)

log = logging.getLogger(__name__)


@dag(
    dag_id="p1_dlq_monitor",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["project1", "kafka", "dlq", "monitoring"],
    description="Hourly DLQ rate surveillance + reason analysis + Slack alerts.",
)
def p1_dlq_monitor():
    @task
    def get_dlq_lag() -> dict:
        dlq = get_consumer_group_lag("flink-quality-pipeline", "dlq_events")
        main = get_consumer_group_lag("flink-quality-pipeline", "page_events")
        return {
            "dlq_total_lag": dlq["total_lag"],
            "main_total_lag": main["total_lag"],
            "dlq_partitions": dlq["partitions"],
            "measured_at": datetime.now(timezone.utc).isoformat(),
        }

    @task
    def sample_dlq_reasons() -> dict:
        messages = sample_dlq_messages("dlq_events", max_messages=200, timeout_seconds=10)
        return {"reason_counts": count_dlq_reasons(messages), "sample_size": len(messages)}

    @task
    def evaluate_dlq_rate(lag_info: dict, reason_info: dict) -> dict:
        threshold = float(Variable.get("dlq_rate_threshold", default_var="0.05"))
        denom = lag_info["dlq_total_lag"] + lag_info["main_total_lag"]
        rate = (lag_info["dlq_total_lag"] / denom) if denom > 0 else 0.0
        return {
            "dlq_rate": rate,
            "threshold": threshold,
            "exceeded": rate > threshold,
            "reason_breakdown": reason_info["reason_counts"],
            "sample_size": reason_info["sample_size"],
        }

    @task
    def alert_on_threshold_breach(evaluation: dict) -> None:
        if not evaluation["exceeded"]:
            log.info(
                "DLQ rate %.2f%% within threshold %.2f%%. No alert.",
                evaluation["dlq_rate"] * 100, evaluation["threshold"] * 100,
            )
            return

        top_reasons = sorted(
            evaluation["reason_breakdown"].items(), key=lambda x: -x[1]
        )[:3]
        log.error(
            "DLQ rate %.2f%% exceeds %.2f%% — top reasons: %s",
            evaluation["dlq_rate"] * 100,
            evaluation["threshold"] * 100,
            top_reasons,
        )

        token = Variable.get("slack_webhook_token", default_var="")
        if not token or token == "REPLACE_ME":
            return

        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f":rotating_light: P1 DLQ rate {evaluation['dlq_rate']*100:.1f}% > "
                                f"threshold {evaluation['threshold']*100:.1f}%",
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Top DLQ reasons:*\n" + "\n".join(
                            f"  • `{r}` — {n}" for r, n in top_reasons
                        ) + "\n\n<http://localhost:8080|Open Kafka UI>",
                    },
                },
            ]
        }
        try:
            requests.post(
                f"https://hooks.slack.com/services/{token}",
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=5,
            )
        except Exception:
            log.exception("Slack post failed (non-fatal).")

    lag = get_dlq_lag()
    reasons = sample_dlq_reasons()
    evaluation = evaluate_dlq_rate(lag, reasons)
    alert_on_threshold_breach(evaluation)


dag = p1_dlq_monitor()

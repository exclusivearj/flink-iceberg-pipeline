# flink-iceberg-pipeline

> Production-style Flink streaming pipeline with inline data quality gates, an Apache Iceberg sink on MinIO, and Airflow orchestration on top.

This is **Project 1** in the Netflix L5 portfolio. It demonstrates PyFlink DataStream API + side-output DLQ patterns, Iceberg schema evolution + time travel, exactly-once checkpointing, Prometheus/Grafana observability, and Airflow lifecycle/maintenance/health DAGs.

## Stack

| Component | Technology |
|---|---|
| Stream processor | Apache Flink 1.18 (PyFlink) |
| Message broker | Apache Kafka 3.6 (3 partitions, 2 topics) |
| Object storage | MinIO (S3-compatible) |
| Table format | Apache Iceberg 1.4 |
| Catalog | Iceberg REST catalog (`tabulario/iceberg-rest`) |
| Observability | Prometheus + Grafana |
| Orchestration | Astronomer Airflow 2.9 (Astro Runtime 10.5.0) |
| Data quality | [`pipeline-sentinel`](vendor/) — vendored wheel |

## Architecture

```
event-generator ──▶ kafka(page_events) ──▶ Flink job ─┬─[DLQ side out]──▶ kafka(dlq_events)
                                                      │
                                                      └─[main]─▶ 5min tumbling window
                                                                       │
                                                                       ▼
                                                              Iceberg page_events_aggregated
                                                              (MinIO/warehouse)
```

Inline quality gates inside the Flink ProcessFunction:
1. **null fields** — `user_id`, `event_type`, `timestamp_ms` must not be null/empty
2. **schema** — `timestamp_ms` must be `int`; `duration_ms` must be `int >= 0` if present
3. **event_type** — must be one of 7 allowed values
4. **device_type** — must be one of 4 allowed values
5. **freshness** — `timestamp_ms` must be within `[now-24h, now+60s]`

First failing gate routes the raw payload + reason to the DLQ.

## One-command bring-up

```bash
docker compose up -d
```

Brings up: zookeeper, kafka, kafka-init, kafka-ui, minio, minio-init, iceberg-rest, flink-jobmanager, flink-taskmanager, event-generator, prometheus, grafana, plus the Airflow stack (postgres, airflow-init, webserver, scheduler).

**UIs:**
- Flink:     <http://localhost:8081>
- Kafka UI:  <http://localhost:8080>
- MinIO:     <http://localhost:9001>  (minioadmin / minioadmin)
- Grafana:   <http://localhost:3000>  (admin / admin)
- Airflow:   <http://localhost:8082>  (admin / admin)
- Prometheus:<http://localhost:9090>

## Workflow

```bash
make up                        # full stack
make submit                    # submit PyFlink job via the Flink CLI inside the container
make logs                      # tail Flink JM+TM logs

# After a few minutes, time-travel demo:
make venv                      # one-time: create local venv + install vendored sentinel
make query                     # python iceberg/query_time_travel.py

make test                      # unit tests (schemas, gates, sink DDL)
make down                      # tear down + delete volumes
```

## Airflow

4 DAGs orchestrate this stack:

| DAG | Schedule | Purpose |
|---|---|---|
| `p1_pipeline_controller` | manual | wait for Kafka+MinIO+Flink → init Iceberg → submit Flink job → store job_id |
| `p1_iceberg_maintenance` | `0 2 * * *` | compact `page_events_aggregated` → expire snapshots > 7d → validate (using `pipeline-sentinel`) → annotate Grafana |
| `p1_dlq_monitor` | `0 * * * *` | sample DLQ topic → count reasons → alert Slack if DLQ rate > 5% |
| `p1_health_report` | `30 8 * * *` | aggregate Flink + Kafka + Iceberg metrics into a daily summary |

See [airflow/](airflow/) for the Astronomer scaffold.

## Repository layout

```
flink-iceberg-pipeline/
├── docker-compose.yml              ← 14-service stack (core + airflow)
├── Makefile                        ← `make up`, `make submit`, `make test`, ...
├── flink_job/                      ← PyFlink job
│   ├── job.py                      ← main entry point
│   ├── quality_gates.py            ← QualityGateProcessor + 5 gate functions
│   ├── schemas.py                  ← PageEvent dataclass + JSON Schema
│   ├── sinks.py                    ← Iceberg + Kafka DLQ DDL helpers
│   ├── Dockerfile                  ← Flink 1.18 + Python + Kafka/Iceberg JARs
│   └── requirements.txt
├── generator/                      ← synthetic event producer
│   ├── generate_events.py
│   ├── Dockerfile
│   └── requirements.txt
├── iceberg/
│   ├── init_catalog.py             ← bootstrap REST catalog + raw table
│   └── query_time_travel.py        ← snapshot listing + AS OF query demo
├── prometheus/prometheus.yml
├── grafana/
│   ├── provisioning/{datasources,dashboards}/...
│   └── dashboards/pipeline_overview.json
├── tests/                          ← schemas, gates, sink DDL
├── airflow/                        ← Astronomer scaffold (Dockerfile, DAGs, utils)
└── vendor/
    └── pipeline_sentinel-0.1.0-py3-none-any.whl   ← vendored from Project 3
```

## Standalone vs sibling-repo development

This repo is **standalone**: `pipeline-sentinel` is installed from the vendored wheel under `vendor/`. To develop the library and this pipeline together, replace the wheel reference with an editable install pointing to a sibling clone:

```bash
.venv/bin/pip install -e ~/Documents/Developer/pipeline-sentinel
```

## Spec sources

Authoritative specs live in `~/Documents/Developer/data-engineering-projects/files/projects/project1-flink-pipeline-orchestration/` — `TASKS.md`, `AIRFLOW_TASKS.md`, `README.md`, `AIRFLOW_README.md`.

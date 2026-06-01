PYTHON ?= python3.11
VENV   := .venv
PIP    := $(VENV)/bin/pip3
OBSERVE := $(VENV)/.deps-installed

.PHONY: help venv up down restart submit test logs query clean \
        airflow-build airflow-init airflow-up airflow-down airflow-logs airflow-ui \
        airflow-trigger-controller

help:
	@echo "Targets:"
	@echo "  make venv          create local .venv (Python 3.11) + install test deps + vendored observe"
	@echo "  make up            docker compose up -d (core + airflow stack)"
	@echo "  make down          docker compose down -v (deletes volumes)"
	@echo "  make restart       restart Flink JM/TM"
	@echo "  make submit        submit PyFlink job via Flink REST"
	@echo "  make logs          tail Flink JM + TM logs"
	@echo "  make test          run unit tests (auto-creates .venv if needed)"
	@echo "  make query         python iceberg/query_time_travel.py (against localhost)"
	@echo "  make clean         remove .venv, __pycache__, etc."
	@echo "  make airflow-ui    show URL for Airflow UI (http://localhost:8082)"

$(OBSERVE):
	rm -rf $(VENV)
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install pytest pytest-cov pyiceberg[s3]==0.5.1 pyarrow confluent-kafka faker
	$(PIP) install vendor/pipeline_observe-0.1.0-py3-none-any.whl
	touch $(OBSERVE)

venv: $(OBSERVE)

up:
	docker compose up -d

down:
	docker compose down -v

restart:
	docker compose restart flink-jobmanager flink-taskmanager

submit:
	docker compose exec flink-jobmanager flink run -py /opt/flink_job/job.py

logs:
	docker compose logs -f flink-jobmanager flink-taskmanager

test: $(OBSERVE)
	$(VENV)/bin/pytest tests/ -v

query: $(OBSERVE)
	ICEBERG_CATALOG_URI=http://localhost:8181 MINIO_ENDPOINT=http://localhost:9000 \
		$(VENV)/bin/python iceberg/query_time_travel.py

clean:
	rm -rf $(VENV) .pytest_cache .coverage __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# ── Airflow targets ─────────────────────────────────────────────
airflow-build:
	docker compose build airflow-webserver

airflow-init:
	docker compose up airflow-init

airflow-up: airflow-build airflow-init
	docker compose up -d airflow-webserver airflow-scheduler

airflow-down:
	docker compose stop airflow-webserver airflow-scheduler postgres

airflow-logs:
	docker compose logs -f airflow-scheduler

airflow-trigger-controller:
	docker compose exec airflow-webserver airflow dags trigger p1_pipeline_controller

airflow-ui:
	@echo "Airflow UI: http://localhost:8082  (admin/admin)"

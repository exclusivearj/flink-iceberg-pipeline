"""Main PyFlink job.

Pipeline:
  Kafka(page_events) → deserialize → QualityGateProcessor →
    ├ DLQ side output → Kafka(dlq_events)
    └ main output → 5-min tumbling window aggregation →
        Iceberg sink (page_events_aggregated)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types as TI
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

# Make the flink_job package importable inside the JM/TM container. The modules
# import each other as `flink_job.*`, so the package PARENT (/opt) goes on the path.
sys.path.insert(0, "/opt")

from flink_job.quality_gates import DLQ_TAG, METRICS_TAG, QualityGateProcessor  # noqa: E402
from flink_job.schemas import deserialize_event  # noqa: E402


def _kafka_source(bootstrap: str, topic: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(topic)
        .set_group_id("flink-quality-pipeline")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def _kafka_dlq_sink(bootstrap: str, topic: str = "dlq_events") -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


class TimestampAssigner:
    """Pulls event-time from the JSON `timestamp_ms` field for watermarking."""

    def extract_timestamp(self, value: str, _record_timestamp: int) -> int:
        try:
            return int(json.loads(value)["timestamp_ms"])
        except Exception:
            return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


class AggregateWindow(ProcessWindowFunction):
    """Per-(event_type, country_code) 5-min window aggregation."""

    def process(self, key, context, elements):
        event_type, country = key
        users = set()
        count = 0
        for raw in elements:
            count += 1
            try:
                e = json.loads(raw)
                if e.get("user_id"):
                    users.add(e["user_id"])
            except Exception:
                continue
        window = context.window()
        yield (
            window.start,
            window.end,
            event_type,
            country,
            count,
            len(users),
            int(datetime.now(timezone.utc).timestamp() * 1000),
        )


def build_job(env: StreamExecutionEnvironment) -> "object":
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    parallelism = int(os.environ.get("PARALLELISM", "4"))
    checkpoint_ms = int(os.environ.get("CHECKPOINT_INTERVAL_MS", "60000"))

    env.set_parallelism(parallelism)
    env.enable_checkpointing(checkpoint_ms, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)

    source = _kafka_source(bootstrap, "page_events")
    raw_stream = env.from_source(
        source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(2))
        .with_timestamp_assigner(TimestampAssigner()),
        "kafka-source",
    )

    processed = raw_stream.process(
        QualityGateProcessor(), output_type=Types.STRING()
    )

    # DLQ side output → Kafka(dlq_events)
    dlq_stream = processed.get_side_output(DLQ_TAG)
    dlq_stream.sink_to(_kafka_dlq_sink(bootstrap))

    # Aggregation: 5-min tumbling window, keyed by (event_type, country_code)
    def _key_fn(raw: str):
        try:
            e = json.loads(raw)
            return (e["event_type"], e.get("country_code", "??"))
        except Exception:
            return ("unknown", "??")

    aggregated = (
        processed
        .key_by(_key_fn, key_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .process(
            AggregateWindow(),
            output_type=Types.TUPLE([
                Types.LONG(), Types.LONG(),  # window_start, window_end (ms)
                Types.STRING(), Types.STRING(),  # event_type, country_code
                Types.LONG(), Types.LONG(),  # event_count, unique_users
                Types.LONG(),  # processed_at_ms
            ]),
        )
    )

    # The aggregated tuple stream is handed back to main(), which converts it to
    # a Table and INSERTs it into the Iceberg sink (see main()).
    return aggregated


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    # The Iceberg sink is wired via the Table API: register the REST catalog +
    # target table, build the DataStream pipeline, convert the aggregated stream
    # to a Table, and INSERT INTO the Iceberg table. The Iceberg connector
    # commits a new snapshot on every successful checkpoint.
    from pyflink.table import StreamTableEnvironment  # noqa: E402

    table_env = StreamTableEnvironment.create(env)
    from flink_job.sinks import create_iceberg_sink  # noqa: E402

    iceberg_table = create_iceberg_sink(table_env)
    logging.info("Iceberg sink table registered: %s", iceberg_table)

    aggregated = build_job(env)

    # Convert the aggregated tuple stream — (window_start_ms, window_end_ms,
    # event_type, country_code, event_count, unique_users, processed_at_ms),
    # exposed as columns f0..f6 — into the Iceberg table's typed columns. The
    # epoch-millis longs become TIMESTAMP(3)/TIMESTAMP_LTZ(3); event_date is
    # derived from window_start.
    table_env.create_temporary_view("agg_stream", aggregated)
    stmt = table_env.create_statement_set()
    stmt.add_insert_sql(
        f"""
        INSERT INTO {iceberg_table}
        SELECT
            CAST(TO_TIMESTAMP_LTZ(f0, 3) AS TIMESTAMP(3)),
            CAST(TO_TIMESTAMP_LTZ(f1, 3) AS TIMESTAMP(3)),
            f2,
            f3,
            f4,
            f5,
            TO_TIMESTAMP_LTZ(f6, 3),
            CAST(TO_TIMESTAMP_LTZ(f0, 3) AS DATE)
        FROM agg_stream
        """
    )

    # attach_as_datastream() folds the Table INSERT into the DataStream job, so a
    # single env.execute() runs the Kafka source, quality gates, DLQ sink, window
    # aggregation, and Iceberg sink as one job.
    stmt.attach_as_datastream()
    env.execute("flink-quality-pipeline")


if __name__ == "__main__":
    main()

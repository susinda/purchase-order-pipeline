"""DAG 3: Kafka consumer-group -> Snowflake RAW microbatches -> managed dbt.

No DAG 1 manifest is needed. Read up to PO_ANALYTICS_MAX_MESSAGES from the
analytics consumer group, MERGE validated records in groups of 10 and commit
Kafka offsets only after successful Snowflake persistence (or DLT acknowledgement).
Run Snowflake-managed dbt only after ingestion completes without quarantines.
"""
import base64
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import DAG, task
from include.po_events import read_event
from include.po_settings import QUALIFIED_IDENTIFIER, get_setting, kafka_ca_file, snowflake_raw_table

LOG = logging.getLogger("airflow.task")


def setting(name, default=None, required=False):
    value = get_setting(name, default)
    if required and not value:
        raise ValueError(f"Missing setting {name} in env.vars or deployment environment")
    return value


def consumer_config():
    return {
        "bootstrap.servers": setting("KAFKA_BOOTSTRAP_SERVERS", required=True),
        "security.protocol": setting("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanism": setting("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": setting("KAFKA_CONSUMER_SASL_USERNAME", required=True),
        "sasl.password": setting("KAFKA_CONSUMER_SASL_PASSWORD", required=True),
        "group.id": setting("KAFKA_ANALYTICS_GROUP_ID", "purchase-orders-analytics"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "ssl.ca.location": kafka_ca_file(),
    }


def publish_to_dlt(event, reason, position):
    from confluent_kafka import Producer

    dlt_topic = setting("KAFKA_SNOWFLAKE_DLT_TOPIC", required=True)
    config = {
        "bootstrap.servers": setting("KAFKA_BOOTSTRAP_SERVERS", required=True),
        "security.protocol": setting("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanism": setting("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": setting("KAFKA_CONSUMER_SASL_USERNAME", required=True),
        "sasl.password": setting("KAFKA_CONSUMER_SASL_PASSWORD", required=True),
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 30000,
    }
    config["ssl.ca.location"] = kafka_ca_file()
    producer = Producer(config)
    results = []

    def delivered(err, msg):
        results.append(err)
        if err:
            LOG.error("Snowflake DLT delivery failed: %s", err)
        else:
            LOG.warning("Snowflake DLT acknowledged offset=%d", msg.offset())

    LOG.warning("Quarantine invalid RAW event=%s order=%s reason=%s topic=%s",
                event.get("event_id"), event.get("order_number"), reason, dlt_topic)
    producer.produce(
        dlt_topic,
        key=str(event.get("order_number") or event.get("event_id") or "unknown").encode(),
        value=json.dumps({
            "destination": "snowflake",
            "error_type": "permanent_record_error",
            "reason": reason,
            "original_event": event,
            "source_position": position,
        }).encode(),
        callback=delivered,
    )
    outstanding = producer.flush(timeout=35)
    if outstanding or len(results) != 1 or results[0] is not None:
        raise RuntimeError("Snowflake DLT delivery was not acknowledged")


def validate_event(event):
    if event.get("_invalid_json"):
        return "Kafka payload is not valid JSON"
    if not event.get("event_id") or not event.get("batch_id"):
        return "Missing event_id or batch_id"
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return "Missing or invalid purchase-order JSON object"
    number = payload.get("OrderNumber")
    if not number or str(number) != str(event.get("order_number")):
        return "Missing or mismatched OrderNumber"
    return None


def merge_raw_chunk(hook, events, batch_id, raw_table):
    """MERGE up to 10 complete events with ONE Snowflake SQL statement.

    Using SELECT + UNION ALL avoids embedding JSON in SQL and keeps all data bound.
    A matched EVENT_ID is left unchanged, making a whole-task retry safe.
    """
    if not events:
        return 0
    selects = []
    parameters = []
    for event in events:
        selects.append(
            "SELECT %s::VARCHAR AS EVENT_ID, %s::VARCHAR AS BATCH_ID, "
            "%s::VARCHAR AS ORDER_NUMBER, %s::VARCHAR AS LOAD_TYPE, "
            "PARSE_JSON(%s) AS PAYLOAD"
        )
        parameters.extend((
            event["event_id"], event["batch_id"], event["order_number"],
            event.get("load_type"),
            json.dumps(event["payload"], separators=(",", ":")),
        ))

    # Deduplicate event IDs in the SQL source so a repeated event in the
    # same ten-record chunk never makes MERGE nondeterministic.
    sql = f"""
    MERGE INTO {raw_table} AS target
    USING (
        SELECT EVENT_ID, BATCH_ID, ORDER_NUMBER, LOAD_TYPE, PAYLOAD
        FROM ({' UNION ALL '.join(selects)}) AS incoming
        QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY ORDER_NUMBER) = 1
    ) AS source
      ON target.EVENT_ID = source.EVENT_ID
    WHEN NOT MATCHED THEN INSERT
      (EVENT_ID, BATCH_ID, ORDER_NUMBER, LOAD_TYPE, PAYLOAD, INGESTED_AT)
    VALUES
      (source.EVENT_ID, source.BATCH_ID, source.ORDER_NUMBER,
       source.LOAD_TYPE, source.PAYLOAD, CURRENT_TIMESTAMP())
    """
    hook.run(sql, parameters=tuple(parameters))

    # Check persistence of all distinct event IDs in the chunk; this must
    # finish successfully before the next Kafka messages are read.
    unique_ids = sorted({event["event_id"] for event in events})
    placeholders = ", ".join(["%s"] * len(unique_ids))
    row = hook.get_first(
        f"SELECT COUNT(DISTINCT EVENT_ID) FROM {raw_table} "
        f"WHERE EVENT_ID IN ({placeholders})",
        parameters=tuple(unique_ids),
    )
    if not row or int(row[0]) != len(unique_ids):
        raise RuntimeError(
            f"RAW verification failed for batch={batch_id}: "
            f"found={row[0] if row else 0}/{len(unique_ids)} unique IDs"
        )
    return len(events)


with DAG(
    dag_id="dag_kafka_to_snowflake",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["purchase-orders", "snowflake", "dlt", "microbatch"],
) as dag:

    @task(retries=2, retry_delay=timedelta(seconds=30),
          retry_exponential_backoff=True,
          max_retry_delay=timedelta(minutes=5),
          execution_timeout=timedelta(minutes=45))
    def consume_and_load_snowflake():
        """Consume available Kafka events and persist/commit in bounded chunks.

        Important: never commit beyond records that have been saved in RAW or
        acknowledged by the DLT producer. A retry safely replays uncommitted
        records because RAW MERGEs are idempotent by EVENT_ID.
        """
        from confluent_kafka import Consumer, TopicPartition
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        topic = setting("KAFKA_TOPIC", "purchase-orders")
        raw_table = snowflake_raw_table()
        chunk_size = int(setting("PO_SNOWFLAKE_BATCH_SIZE", "10"))
        max_messages = int(setting("PO_ANALYTICS_MAX_MESSAGES", "100"))
        idle_seconds = int(setting("PO_ANALYTICS_IDLE_SECONDS", "30"))
        if not topic:
            raise ValueError("KAFKA_TOPIC must not be empty")
        if not 1 <= chunk_size <= 100:
            raise ValueError("PO_SNOWFLAKE_BATCH_SIZE must be between 1 and 100")
        if max_messages <= 0 or idle_seconds <= 0:
            raise ValueError("PO_ANALYTICS_MAX_MESSAGES and PO_ANALYTICS_IDLE_SECONDS must be positive")

        config = consumer_config()
        LOG.info(
            "SNOWFLAKE consumer: topic=%s group=%s username=%s chunk=%d "
            "max_messages=%d idle_seconds=%d offset_reset=%s",
            topic, config["group.id"], config["sasl.username"], chunk_size,
            max_messages, idle_seconds, config["auto.offset.reset"],
        )

        hook = SnowflakeHook(
            snowflake_conn_id=setting("SNOWFLAKE_CONNECTION_ID", "snowflake_default")
        )
        consumer = Consumer(config)
        pending = []                 # (event, partition, offset) already read, not committed
        processed = 0
        loaded = 0
        quarantined = 0
        chunks = 0

        def commit_positions(entries):
            """Commit next offset for each partition, after durable processing.

            The consumer delivers records in offset order within each partition.
            Every entry in a committed chunk has been resolved first.
            """
            if not entries:
                return
            next_offsets = {}
            for _, partition, offset in entries:
                next_offsets[partition] = max(next_offsets.get(partition, 0), offset + 1)
            committed = consumer.commit(
                offsets=[TopicPartition(topic, partition, next_offset)
                         for partition, next_offset in sorted(next_offsets.items())],
                asynchronous=False,
            )
            if not committed or any(item.error for item in committed):
                raise RuntimeError(f"Kafka offset commit failed: {committed}")
            LOG.info("COMMITTED offsets=%s", next_offsets)

        def flush_pending():
            nonlocal loaded, chunks
            if not pending:
                return
            events = [event for event, _, _ in pending]
            # Each SQL MERGE and verification must finish before offset commits.
            # Replaying after a failed commit is safe because MERGE uses EVENT_ID.
            LOG.info("MERGE chunk=%d rows=%d", chunks + 1, len(events))
            merge_raw_chunk(hook, events, "kafka-consumer-group", raw_table)
            commit_positions(pending)
            loaded += len(events)
            chunks += 1
            LOG.info("MERGE confirmed chunk=%d loaded=%d", chunks, loaded)
            pending.clear()

        try:
            # subscribe() uses this group's committed offsets, unlike the old
            # explicit-offset manifest reader. The first run without committed
            # offsets uses 'earliest' and may replay retained topic history.
            consumer.subscribe([topic])
            idle_deadline = time.monotonic() + idle_seconds
            while processed < max_messages:
                remaining = idle_deadline - time.monotonic()
                if remaining <= 0:
                    LOG.info("Kafka idle for %ds: stop after %d messages", idle_seconds, processed)
                    break
                msg = consumer.poll(min(1.0, remaining))
                if msg is None:
                    continue
                if msg.error():
                    raise RuntimeError(f"Kafka consume failure: {msg.error()}")

                partition, offset = msg.partition(), msg.offset()
                pos = {"topic": topic, "partition": partition, "offset": offset}
                raw = msg.value() or b""
                try:
                    event = read_event(raw, msg.headers())
                except ValueError as exc:
                    event = {
                        "event_id": f"malformed-{partition}-{offset}",
                        "order_number": None,
                        "_invalid_json": True,
                        "raw_payload_base64": base64.b64encode(raw).decode(),
                    }
                    LOG.warning("Invalid JSON partition=%d offset=%d: %s", partition, offset, exc)

                LOG.info("READ %d/%d order=%s event=%s partition=%d offset=%d",
                         processed + 1, max_messages, event.get("order_number"),
                         event.get("event_id"), partition, offset)
                reason = validate_event(event)
                if reason:
                    # First persist+commit preceding valid records. Never commit
                    # the invalid record's offset before earlier records are safe.
                    flush_pending()
                    publish_to_dlt(event, reason, pos)
                    commit_positions([(event, partition, offset)])
                    quarantined += 1
                    LOG.warning("QUARANTINED partition=%d offset=%d reason=%s",
                                partition, offset, reason)
                else:
                    pending.append((event, partition, offset))
                    if len(pending) >= chunk_size:
                        flush_pending()

                processed += 1
                idle_deadline = time.monotonic() + idle_seconds

            flush_pending()
        finally:
            consumer.close()

        LOG.info("SNOWFLAKE RESULT processed=%d loaded=%d chunks=%d quarantined=%d",
                 processed, loaded, chunks, quarantined)
        if quarantined:
            raise AirflowFailException(
                f"Snowflake processing quarantined {quarantined} record(s) in analytics DLT; "
                "dbt will not run"
            )
        if processed == 0:
            raise AirflowSkipException("No new Kafka messages available for Snowflake")
        return {"read_count": processed, "ingested_count": loaded,
                "quarantined_count": 0, "chunk_count": chunks}

    @task(retries=1, retry_delay=timedelta(seconds=60), execution_timeout=timedelta(minutes=45))
    def run_dbt(result):
        from airflow.exceptions import AirflowSkipException
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        project = setting("SNOWFLAKE_DBT_PROJECT", "").strip()
        if not project:
            LOG.info("dbt not configured; RAW ingestion complete (%d Kafka messages)", result["read_count"])
            raise AirflowSkipException("Set SNOWFLAKE_DBT_PROJECT when the dbt project is ready")
        if not re.fullmatch(QUALIFIED_IDENTIFIER, project):
            raise ValueError("SNOWFLAKE_DBT_PROJECT must be DATABASE.SCHEMA.PROJECT")
        LOG.info("Run Snowflake-managed dbt: %s, Kafka messages=%d", project, result["read_count"])
        hook = SnowflakeHook(
            snowflake_conn_id=setting("SNOWFLAKE_CONNECTION_ID", "snowflake_default")
        )
        raw_database, raw_schema, raw_table = snowflake_raw_table().upper().split(".")
        result_row = hook.get_first(
            f"EXECUTE DBT PROJECT {project} ARGS = 'build' "
            "ENV_VARS = ("
            f"'DBT_PO_RAW_DATABASE' = '{raw_database}', "
            f"'DBT_PO_RAW_SCHEMA' = '{raw_schema}', "
            f"'DBT_PO_RAW_TABLE' = '{raw_table}')"
        )
        if not result_row or str(result_row[0]).upper() != "TRUE":
            raise RuntimeError(f"Snowflake-managed dbt build failed for {project}")
        LOG.info("DBT command succeeded after %d Kafka messages", result["read_count"])

    load_result = consume_and_load_snowflake()
    run_dbt(load_result)


# Fail DAG parsing (rather than silently succeeding with an empty graph)
# if Airflow does not register the expected tasks or their dependencies.
EXPECTED_TASKS = {"consume_and_load_snowflake", "run_dbt"}
if set(dag.task_ids) != EXPECTED_TASKS:
    raise RuntimeError(f"Unexpected DAG 3 tasks: {sorted(dag.task_ids)}")
if dag.get_task("run_dbt").upstream_task_ids != {"consume_and_load_snowflake"}:
    raise RuntimeError("run_dbt must depend on consume_and_load_snowflake")
LOG.info("DAG 3 parsed: %s tasks=%s", dag.dag_id, sorted(dag.task_ids))

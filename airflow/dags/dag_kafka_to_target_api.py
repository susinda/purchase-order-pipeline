"""DAG 2: Kafka purchase-order snapshots -> Target API; quarantine bad records.

Graph: consume_and_deliver_orders (Kafka group poll -> validate -> deliver or quarantine -> commit)
Can be triggered manually or by DAG 1; no Kafka-offset manifest is required.
Kafka's Apache Airflow provider installs confluent-kafka, used directly here because
existing credentials come from env.vars, not Airflow Kafka Connections.
"""
import base64
import json
import logging
import time
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
from airflow.sdk import DAG, task
from include.po_events import read_event
from include.po_settings import get_setting, kafka_ca_file

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
        "group.id": setting("KAFKA_BACKEND_GROUP_ID", "purchase-orders-backend"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "ssl.ca.location": kafka_ca_file(),
    }


def publish_to_dlt(event, reason, source_position):
    """Publish one failed event to the TARGET DLT; block until broker ACK."""
    from confluent_kafka import Producer

    dlt_topic = setting("KAFKA_TARGET_DLT_TOPIC", required=True)
    producer_config = {
        "bootstrap.servers": setting("KAFKA_BOOTSTRAP_SERVERS", required=True),
        "security.protocol": setting("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanism": setting("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        # The shared consumer account has WRITE on the backend DLT in Aiven.
        "sasl.username": setting("KAFKA_CONSUMER_SASL_USERNAME", required=True),
        "sasl.password": setting("KAFKA_CONSUMER_SASL_PASSWORD", required=True),
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 30000,
    }
    producer_config["ssl.ca.location"] = kafka_ca_file()

    producer = Producer(producer_config)
    responses = []

    def on_delivery(err, msg):
        responses.append(err)
        if err is None:
            LOG.warning("DLT acknowledged topic=%s partition=%d offset=%d",
                        msg.topic(), msg.partition(), msg.offset())
        else:
            LOG.error("DLT delivery error: %s", err)

    failed_record = {
        "destination": "target_api",
        "error_type": "permanent_record_error",
        "reason": reason,
        "original_event": event,
        "source_position": source_position,
    }
    LOG.warning("Quarantining order=%s event=%s into %s: %s",
                event.get("order_number"), event.get("event_id"), dlt_topic, reason)
    producer.produce(
        dlt_topic,
        key=str(event.get("order_number") or event.get("event_id") or "unknown").encode(),
        value=json.dumps(failed_record).encode("utf-8"),
        callback=on_delivery,
    )
    outstanding = producer.flush(timeout=35)
    if outstanding or len(responses) != 1 or responses[0] is not None:
        raise RuntimeError("Target DLT delivery not confirmed; source event remains unresolved")


def check_required_fields(record, path, fields, children=()):
    """Report the first missing key in an order or its optional child arrays."""
    if not isinstance(record, dict):
        return f"{path} must be an object"
    for field in fields:
        if field not in record:
            return f"Missing required field {path}.{field}"
    for collection, child_fields, grandchildren in children:
        rows = record.get(collection)
        if rows is None:
            continue
        if not isinstance(rows, list):
            return f"{path}.{collection} must be an array"
        for index, row in enumerate(rows):
            reason = check_required_fields(row, f"{path}.{collection}[{index}]", child_fields, grandchildren)
            if reason:
                return reason
    return None


def check_order(event):
    """Record-level checks only. Unexpected structure is a permanent data error."""
    if event.get("_invalid_json"):
        return "Kafka payload is not valid JSON"
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return "Missing or invalid purchase-order payload"
    reason = check_required_fields(payload, "payload", ("OrderNumber", "LastUpdated"), (
        ("OrderLines", ("LineNumber",), (
            ("CodingRows", ("RowIndex",), ()),
            ("GoodsReceipts", ("GoodsReceiptNumber",), ()),
        )),
    ))
    if reason:
        return reason
    number = payload["OrderNumber"]
    if not number or str(number) != str(event.get("order_number")):
        return "Missing or mismatched OrderNumber"
    if not event.get("event_id"):
        return "Missing event_id"
    return None


with DAG(
    dag_id="dag_kafka_to_target_api",
    description="Read available Kafka events using consumer-group offsets and POST to Target API",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["purchase-orders", "target", "dlt"],
) as dag:

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )
    def consume_and_deliver_orders():
        """Read available Kafka records, deliver each, then commit its offset.

        No parent manifest is needed. The consumer group tracks the next offset.
        After a successful API POST (or broker-acknowledged DLT quarantine),
        commit synchronously. Failures leave the current record uncommitted.

        This is a finite Airflow run, not a continuous Kafka listener:
        stop after PO_BACKEND_MAX_MESSAGES records or PO_BACKEND_IDLE_SECONDS
        of no messages. Repeated runs resume from committed group offsets.
        """
        from confluent_kafka import Consumer, TopicPartition
        import requests

        config = consumer_config()
        topic = setting("KAFKA_TOPIC", "purchase-orders")
        max_messages = int(setting("PO_BACKEND_MAX_MESSAGES", "100"))
        idle_seconds = int(setting("PO_BACKEND_IDLE_SECONDS", "30"))
        if max_messages <= 0 or idle_seconds <= 0:
            raise ValueError("PO_BACKEND_MAX_MESSAGES and PO_BACKEND_IDLE_SECONDS must be positive")
        if not topic:
            raise ValueError("KAFKA_TOPIC must not be empty")

        url = setting("TARGET_API_URL", required=True).rstrip("/") + "/purchase-orders"
        auth = (setting("TARGET_API_USERNAME", required=True),
                setting("TARGET_API_PASSWORD", required=True))

        LOG.info(
            "TARGET consumer starting: topic=%s group=%s username=%s "
            "max_messages=%s idle_seconds=%s offset_reset=%s",
            topic, config["group.id"], config["sasl.username"], max_messages,
            idle_seconds, config["auto.offset.reset"],
        )

        consumer = Consumer(config)
        delivered = quarantined = consumed = 0
        try:
            # subscribe() uses Kafka consumer-group committed offsets. Unlike
            # assign() with an explicit offset manifest, it does not start over
            # from the beginning after a successful commit.
            consumer.subscribe([topic])
            idle_deadline = time.monotonic() + idle_seconds
            with requests.Session() as session:
                while consumed < max_messages:
                    seconds_left = idle_deadline - time.monotonic()
                    if seconds_left <= 0:
                        LOG.info("No more Kafka messages for %ss; ending this DAG run", idle_seconds)
                        break
                    message = consumer.poll(min(1.0, seconds_left))
                    if message is None:
                        continue
                    if message.error():
                        raise RuntimeError(f"Kafka consume failure: {message.error()}")

                    partition, offset = message.partition(), message.offset()
                    source_position = {
                        "topic": topic, "partition": partition, "offset": offset
                    }
                    raw = message.value() or b""
                    try:
                        event = read_event(raw, message.headers())
                    except ValueError as error:
                        event = {
                            "event_id": f"malformed-{partition}-{offset}",
                            "order_number": None,
                            "_invalid_json": True,
                            "raw_payload_base64": base64.b64encode(raw).decode(),
                        }
                        LOG.warning("Malformed Kafka JSON partition=%d offset=%d: %s",
                                    partition, offset, error)

                    LOG.info("READ order=%s batch_id=%s event_id=%s partition=%d offset=%d",
                             event.get("order_number"), event.get("batch_id"), event.get("event_id"),
                             partition, offset)
                    reason = check_order(event)
                    if reason:
                        # Commit only after confirmed DLT delivery; the DLT
                        # captures bad source data for separate investigation.
                        publish_to_dlt(event, reason, source_position)
                        quarantined += 1
                        LOG.warning("QUARANTINED offset=%d reason=%s", offset, reason)
                    else:
                        order_number = event["order_number"]
                        LOG.info("POST order=%s event=%s", order_number, event["event_id"])
                        response = session.post(
                            url,
                            json=event["payload"],
                            auth=auth,
                            headers={"Idempotency-Key": event["event_id"]},
                            timeout=30,
                        )
                        LOG.info("Target HTTP=%d order=%s", response.status_code, order_number)
                        if response.status_code in (400, 422):
                            # Only use DLT for statuses defined by the Target API
                            # as permanent, record-level validation failures.
                            publish_to_dlt(
                                event,
                                f"Target rejected record HTTP {response.status_code}",
                                source_position,
                            )
                            quarantined += 1
                        else:
                            response.raise_for_status()
                            delivered += 1
                            LOG.info("DELIVERED order=%s total=%d", order_number, delivered)

                    # Kafka's commit() stores the NEXT offset. Explicit offset
                    # commit makes the processed record unambiguous, even if
                    # the consumer has prefetched more records.
                    committed = consumer.commit(
                        offsets=[TopicPartition(topic, partition, offset + 1)],
                        asynchronous=False,
                    )
                    if not committed or any(item.error for item in committed):
                        raise RuntimeError(
                            "Kafka offset commit failed for "
                            f"{topic} partition={partition} next_offset={offset + 1}: "
                            f"{committed}"
                        )
                    consumed += 1
                    idle_deadline = time.monotonic() + idle_seconds
                    LOG.info("COMMITTED partition=%d next_offset=%d processed=%d/%d",
                             partition, offset + 1, consumed, max_messages)
        finally:
            consumer.close()

        LOG.info("TARGET run complete processed=%d delivered=%d quarantined=%d",
                 consumed, delivered, quarantined)
        if quarantined:
            # Quarantined records are intentionally committed (resolved by DLT)
            # and are not automatically replayed. Flag the run for investigation.
            raise AirflowFailException(
                f"Target run had {quarantined} quarantined record(s) in backend DLT"
            )
        return {"processed": consumed, "delivered": delivered, "quarantined": quarantined}

    consume_and_deliver_orders()


# Fail parsing if this DAG accidentally registers no tasks.
assert set(dag.task_ids) == {"consume_and_deliver_orders"}, (
    f"DAG 2 task registration mismatch: {sorted(dag.task_ids)}"
)

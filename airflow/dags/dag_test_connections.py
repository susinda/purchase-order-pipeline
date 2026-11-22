"""Lightweight connection checks for the purchase-order pipeline.

Tests Source /health, Target /health, Kafka metadata, and Snowflake
authentication and database visibility. An opt-in Kafka round-trip uses a
dedicated diagnostic topic.

Settings are loaded from Astronomer task environment variables first, then
from env.vars in the Astro project root using include.po_settings.

DAG Graph: source_api_test   target_api_test   snowflake_test
           kafka_producer_test -> kafka_consumer_test
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone

from airflow.sdk import Param, dag, task
from include.po_settings import get_setting, kafka_ca_file, required_setting, snowflake_raw_table

LOG = logging.getLogger("airflow.task")


def setting(name, default=None):
    """Read Astro environment first, then airflow/env.vars, then a default."""
    if default is None:
        return required_setting(name)
    return get_setting(name, default)


def kafka_config(username_variable, password_variable):
    """Configuration shared by the test producer and consumer."""
    config = {
        "bootstrap.servers": setting("KAFKA_BOOTSTRAP_SERVERS"),
        "security.protocol": setting("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanism": setting("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": setting(username_variable),
        "sasl.password": setting(password_variable),
        "socket.timeout.ms": 15000,
        "ssl.ca.location": kafka_ca_file(),
    }
    return config


@dag(
    dag_id="dag_test_connections",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["purchase-orders", "connection-test"],
    params={
        "enable_live_kafka_test": Param(
            False, type="boolean",
            description="Publish a diagnostic event to KAFKA_TEST_TOPIC and read it back.",
        ),
    },
)
def test_connections():

    @task
    def source_api_test():
        """GET the Source FastAPI /health endpoint."""
        import requests

        base_url = setting("SOURCE_API_URL")
        username = setting("SOURCE_API_USERNAME")
        password = setting("SOURCE_API_PASSWORD")
        url = base_url.rstrip("/") + "/health"

        LOG.info("[SOURCE] Test: GET %s", url)
        LOG.info("[SOURCE] Basic authentication: configured")
        started = time.monotonic()
        response = requests.get(
            url,
            auth=(username, password),
            timeout=15,
            allow_redirects=False,
        )
        elapsed_ms = (time.monotonic() - started) * 1000

        LOG.info("[SOURCE] Status: HTTP %s", response.status_code)
        LOG.info("[SOURCE] Duration: %.0f ms", elapsed_ms)
        LOG.info("[SOURCE] Content-Type: %s", response.headers.get("Content-Type"))
        if response.status_code == 200:
            # Only log a short, NON-SENSITIVE response from this known /health route.
            LOG.info("[SOURCE] Health response: %s", response.text[:200])
        response.raise_for_status()
        if response.status_code != 200:
            raise RuntimeError(f"Source health check expected HTTP 200, got {response.status_code}")
        LOG.info("[SOURCE] PASS")

    @task
    def target_api_test():
        """GET the Target FastAPI /health endpoint."""
        import requests

        base_url = setting("TARGET_API_URL")
        username = setting("TARGET_API_USERNAME")
        password = setting("TARGET_API_PASSWORD")
        url = base_url.rstrip("/") + "/health"

        LOG.info("[TARGET] Test: GET %s", url)
        LOG.info("[TARGET] Basic authentication: configured")
        started = time.monotonic()
        response = requests.get(
            url,
            auth=(username, password),
            timeout=15,
            allow_redirects=False,
        )
        elapsed_ms = (time.monotonic() - started) * 1000

        LOG.info("[TARGET] Status: HTTP %s", response.status_code)
        LOG.info("[TARGET] Duration: %.0f ms", elapsed_ms)
        LOG.info("[TARGET] Content-Type: %s", response.headers.get("Content-Type"))
        if response.status_code == 200:
            LOG.info("[TARGET] Health response: %s", response.text[:200])
        response.raise_for_status()
        if response.status_code != 200:
            raise RuntimeError(f"Target health check expected HTTP 200, got {response.status_code}")
        LOG.info("[TARGET] PASS")

    @task
    def kafka_producer_test(**context) -> dict:
        """Verify producer credentials; optionally publish a diagnostic event."""
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient

        config = kafka_config(
            "KAFKA_PRODUCER_SASL_USERNAME", "KAFKA_PRODUCER_SASL_PASSWORD"
        )
        purchase_order_topic = setting("KAFKA_TOPIC", "purchase-orders")
        live_test = context["params"]["enable_live_kafka_test"]
        topic = setting("KAFKA_TEST_TOPIC") if live_test else purchase_order_topic
        if live_test and topic == purchase_order_topic:
            raise ValueError("KAFKA_TEST_TOPIC must differ from KAFKA_TOPIC")

        LOG.info("[PRODUCER] Existing topic: %s", topic)
        LOG.info("[PRODUCER] SASL user: %s", config["sasl.username"])
        LOG.info("[PRODUCER] Checking broker authentication and topic metadata")
        metadata = AdminClient(config).list_topics(topic=topic, timeout=15)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Topic {topic!r} unavailable for producer: "
                               f"{getattr(topic_metadata, 'error', None)}")
        LOG.info("[PRODUCER] Topic accessible; partitions: %d", len(topic_metadata.partitions))

        if not live_test:
            LOG.info("[PRODUCER] PASS (metadata only; WRITE permission not verified)")
            LOG.info("[PRODUCER] Live publishing disabled to protect production consumers")
            return {"mode": "metadata", "topic": topic}

        test_id = str(uuid.uuid4())
        payload = {"kind": "connection_test", "test_id": test_id}
        delivered = {}

        def on_delivery(error, message):
            if error:
                delivered["error"] = str(error)
                LOG.error("[PRODUCER] Delivery failed: %s", error)
            else:
                delivered["partition"] = message.partition()
                delivered["offset"] = message.offset()
                LOG.info("[PRODUCER] Broker ACK partition=%s offset=%s",
                         message.partition(), message.offset())

        LOG.warning("[PRODUCER] LIVE TEST ENABLED: publishing diagnostic event to %s", topic)
        producer = Producer(config)
        producer.produce(
            topic=topic,
            key=test_id.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=on_delivery,
        )
        outstanding = producer.flush(timeout=20)
        if outstanding or "error" in delivered or "partition" not in delivered:
            raise RuntimeError(f"Kafka delivery failed or timed out: {delivered.get('error')}")
        LOG.info("[PRODUCER] PASS (actual WRITE verified)")
        return {"mode": "live", "topic": topic, "test_id": test_id,
                "partition": delivered["partition"], "offset": delivered["offset"]}

    @task
    def kafka_consumer_test(produced_message: dict):
        """Verify consumer metadata; read exact test event if live test was enabled."""
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient

        config = kafka_config(
            "KAFKA_CONSUMER_SASL_USERNAME", "KAFKA_CONSUMER_SASL_PASSWORD"
        )
        topic = produced_message["topic"]
        LOG.info("[CONSUMER] Existing topic: %s", topic)
        LOG.info("[CONSUMER] SASL user: %s", config["sasl.username"])
        LOG.info("[CONSUMER] Checking broker authentication and topic metadata")
        metadata = AdminClient(config).list_topics(topic=topic, timeout=15)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Topic {topic!r} unavailable for consumer: "
                               f"{getattr(topic_metadata, 'error', None)}")
        LOG.info("[CONSUMER] Topic accessible; partitions: %d", len(topic_metadata.partitions))

        if produced_message["mode"] != "live":
            LOG.info("[CONSUMER] PASS (metadata only; READ permission not verified)")
            return

        config.update({
            "group.id": setting("KAFKA_TEST_GROUP_ID", "purchaseorders-connection-test"),
            "enable.auto.commit": False,
            "auto.offset.reset": "error",
        })
        partition = produced_message["partition"]
        offset = produced_message["offset"]
        expected_id = produced_message["test_id"]
        LOG.info("[CONSUMER] Reading exact diagnostic event partition=%s offset=%s",
                 partition, offset)
        consumer = Consumer(config)
        try:
            consumer.assign([TopicPartition(topic, partition, offset)])
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(f"Kafka consumer error: {message.error()}")
                actual = json.loads(message.value().decode("utf-8"))
                LOG.info("[CONSUMER] Received partition=%s offset=%s payload=%s",
                         message.partition(), message.offset(), actual)
                if message.partition() != partition or message.offset() != offset:
                    raise RuntimeError("Received unexpected Kafka message position")
                if actual.get("kind") != "connection_test" or actual.get("test_id") != expected_id:
                    raise RuntimeError("Received Kafka event did not match produced test ID")
                LOG.info("[CONSUMER] PASS (actual READ verified)")
                return
            raise TimeoutError("Diagnostic Kafka message not received within 20 seconds")
        finally:
            consumer.close()

    @task
    def snowflake_test():
        """Check the configured Snowflake connection and database visibility."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        connection_id = setting("SNOWFLAKE_CONNECTION_ID", "snowflake_default")
        LOG.info("[SNOWFLAKE] Connection ID: %s", connection_id)
        hook = SnowflakeHook(snowflake_conn_id=connection_id)
        started = time.monotonic()
        row = hook.get_first(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()"
        )
        if not row:
            raise RuntimeError("Snowflake returned no rows from identity query")

        LOG.info("[SNOWFLAKE] User: %s", row[0])
        LOG.info("[SNOWFLAKE] Role: %s", row[1])
        LOG.info("[SNOWFLAKE] Warehouse: %s", row[2])
        LOG.info("[SNOWFLAKE] Current database: %s", row[3])

        # SHOW DATABASES does not require a current database to be selected.
        database_name = snowflake_raw_table().split(".", 1)[0].upper()
        databases = hook.get_records(f"SHOW DATABASES LIKE '{database_name}'")
        if not any(database[1] == database_name for database in databases):
            raise RuntimeError(
                f"Snowflake database {database_name} does not exist or is not visible to this role"
            )
        LOG.info("[SNOWFLAKE] Database exists: %s", database_name)
        LOG.info("[SNOWFLAKE] Duration: %.0f ms", (time.monotonic()-started)*1000)
        LOG.info("[SNOWFLAKE] PASS")

    # Airflow draws this dependency in Graph view automatically.
    kafka_consumer_test(kafka_producer_test())

    # These are independent tests and may run in parallel.
    source_api_test()
    target_api_test()
    snowflake_test()


test_connections()

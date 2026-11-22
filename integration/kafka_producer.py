import argparse
import hashlib
import os
from urllib.request import urlopen

from integration.po_pipeline.events import (
    ACK_TIMEOUT_SECONDS, LOAD_TYPES, fetch_extract as fetch_source_extract, json_bytes, validate_extract,
)
from integration.po_pipeline.settings import api_credentials, application_setting

from .kafka_config import connection_options, kafka_topic


def kafka_config(env):
    return {
        **connection_options(env, role="producer"),
        "acks": "all",
        "max_block_ms": ACK_TIMEOUT_SECONDS * 1000,
    }


def fetch_extract(source_url, load_type, source_auth):
    return fetch_source_extract(source_url, load_type, source_auth, opener=urlopen)


def prepare_messages(extract, load_type):
    orders, extracted_at = validate_extract(extract, load_type)

    batch_id = hashlib.sha256(json_bytes(extract)).hexdigest()
    messages = []
    for index, order in enumerate(orders):
        order_number = order.get("OrderNumber") if isinstance(order, dict) else None
        if not isinstance(order_number, str) or not order_number.strip():
            raise ValueError(f"Order at index {index} must include OrderNumber")

        # The batch hash covers the full extract. The index distinguishes its records.
        event_id = hashlib.sha256(f"{batch_id}:{index}".encode("ascii")).hexdigest()
        headers = [
            ("batch_id", batch_id.encode("ascii")),
            ("event_id", event_id.encode("ascii")),
            ("load_type", load_type.encode("utf-8")),
            ("extracted_at", extracted_at.encode("utf-8")),
        ]
        messages.append((order_number.encode("utf-8"), json_bytes(order), headers))
    return batch_id, messages


def publish_load(load_type, *, source_url, source_auth, topic, producer_factory, producer_options):
    extract = fetch_extract(source_url, load_type, source_auth)
    batch_id, messages = prepare_messages(extract, load_type)
    if not messages:
        return batch_id, 0

    producer = producer_factory(**producer_options)
    try:
        for key, value, headers in messages:
            receipt = producer.send(topic, key=key, value=value, headers=headers)
            receipt.get(timeout=ACK_TIMEOUT_SECONDS)
    finally:
        producer.close(timeout=ACK_TIMEOUT_SECONDS)

    return batch_id, len(messages)


def main():
    parser = argparse.ArgumentParser(description="Publish one purchase-order extract to Kafka.")
    parser.add_argument("load_type", choices=LOAD_TYPES)
    args = parser.parse_args()

    try:
        env = {"SOURCE_API_URL": "http://127.0.0.1:8000", **os.environ}
        producer_options = kafka_config(env)
        topic = kafka_topic(env)
        source_url = application_setting("source.api.url", env)
        source_auth = api_credentials("source", env)
    except ValueError as exc:
        parser.error(str(exc))

    from kafka import KafkaProducer

    batch_id, count = publish_load(
        args.load_type,
        source_url=source_url,
        source_auth=source_auth,
        topic=topic,
        producer_factory=KafkaProducer,
        producer_options=producer_options,
    )
    print(f"Kafka acknowledged {count} records for batch {batch_id} on {topic}")


if __name__ == "__main__":
    main()

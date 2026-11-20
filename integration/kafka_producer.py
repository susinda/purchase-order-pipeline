import argparse
import base64
import hashlib
import json
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .kafka_config import connection_options, required_setting


LOAD_TYPES = ("initial", "incremental")
SOURCE_TIMEOUT_SECONDS = 30
ACK_TIMEOUT_SECONDS = 30


def kafka_config(env):
    return {
        **connection_options(env),
        "acks": "all",
        "max_block_ms": ACK_TIMEOUT_SECONDS * 1000,
    }


def fetch_extract(source_url, load_type, source_auth):
    query = urlencode({"load_type": load_type})
    request = Request(f"{source_url.rstrip('/')}/purchase-orders?{query}")
    username, password = source_auth
    credentials = base64.b64encode(f"{username}:{password}".encode("ascii")).decode("ascii")
    request.add_unredirected_header("Authorization", f"Basic {credentials}")
    with urlopen(request, timeout=SOURCE_TIMEOUT_SECONDS) as response:
        return json.load(response)


def json_bytes(payload):
    return json.dumps(
        payload, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def prepare_messages(extract, load_type):
    if not isinstance(extract, dict) or extract.get("load_type") != load_type:
        raise ValueError(f"Source must return a {load_type} extract")

    orders = extract.get("purchased_orders")
    count = extract.get("record_count")
    if not isinstance(orders, list) or type(count) is not int or count != len(orders):
        raise ValueError("record_count must match the purchased_orders array")

    extracted_at = extract.get("extracted_at")
    if not isinstance(extracted_at, str) or not extracted_at.strip():
        raise ValueError("Source extract must include extracted_at")

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
        producer_options = kafka_config(os.environ)
        topic = required_setting(os.environ, "KAFKA_TOPIC")
        source_auth = (
            required_setting(os.environ, "SOURCE_API_USERNAME"),
            required_setting(os.environ, "SOURCE_API_PASSWORD"),
        )
    except ValueError as exc:
        parser.error(str(exc))

    from kafka import KafkaProducer

    batch_id, count = publish_load(
        args.load_type,
        source_url=os.environ.get("SOURCE_API_URL", "http://127.0.0.1:8000"),
        source_auth=source_auth,
        topic=topic,
        producer_factory=KafkaProducer,
        producer_options=producer_options,
    )
    print(f"Kafka acknowledged {count} records for batch {batch_id} on {topic}")


if __name__ == "__main__":
    main()

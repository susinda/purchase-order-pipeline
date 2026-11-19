import argparse
import json
import os

from .kafka_producer import ACK_TIMEOUT_SECONDS, kafka_config


def reject_non_json_number(value):
    raise ValueError(f"{value} is not a valid JSON number")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Publish one JSON message to Kafka.")
    parser.add_argument("topic", help="Kafka topic to publish to")
    parser.add_argument("message", help="JSON value, quoted as a single shell argument")
    args = parser.parse_args(argv)

    try:
        json.loads(args.message, parse_constant=reject_non_json_number)
        value = args.message.encode("utf-8")
    except ValueError as exc:
        parser.error(f"Invalid JSON message: {exc}")

    try:
        options = kafka_config(os.environ)
    except ValueError as exc:
        parser.error(str(exc))

    from kafka import KafkaProducer

    producer = KafkaProducer(**options)
    try:
        receipt = producer.send(args.topic, value=value).get(timeout=ACK_TIMEOUT_SECONDS)
    finally:
        producer.close(timeout=ACK_TIMEOUT_SECONDS)

    print(f"Published to {receipt.topic}[{receipt.partition}] at offset {receipt.offset}")


if __name__ == "__main__":
    main()

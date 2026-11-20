import argparse
import os
from itertools import islice

from dags.po_pipeline.settings import application_setting

from .kafka_config import connection_options, kafka_group_id, kafka_topic


def consumer_options(env):
    return {
        **connection_options(env, role="inspection"),
        "client_id": application_setting("kafka.inspection.client.id", env, override="KAFKA_CONSUMER_CLIENT_ID"),
        "group_id": kafka_group_id(env, "inspection"),
        "auto_offset_reset": application_setting("kafka.inspection.offset.reset", env, override="KAFKA_AUTO_OFFSET_RESET"),
        "enable_auto_commit": False,
    }


def consume_messages(*, topic, options, consumer_factory, max_messages=None):
    consumer = consumer_factory(topic, **options)
    count = 0
    try:
        for message in islice(consumer, max_messages):
            key = message.key.decode("utf-8") if message.key is not None else None
            value = message.value.decode("utf-8") if message.value is not None else None
            print(
                f"Consumed from {message.topic}[{message.partition}] offset={message.offset}: "
                f"key={key}, value={value}",
                flush=True,
            )
            consumer.commit()
            count += 1
    finally:
        consumer.close(autocommit=False)
    return count


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def main():
    parser = argparse.ArgumentParser(description="Inspect purchase-order messages from Kafka.")
    parser.add_argument("--max-messages", type=positive_int, help="Exit after this many records.")
    parser.add_argument(
        "--idle-timeout",
        type=positive_int,
        metavar="SECONDS",
        help="Exit after this long without a record.",
    )
    args = parser.parse_args()
    try:
        topic = kafka_topic(os.environ)
        options = consumer_options(os.environ)
    except ValueError as exc:
        parser.error(str(exc))
    if args.idle_timeout is not None:
        options["consumer_timeout_ms"] = args.idle_timeout * 1000

    from kafka import KafkaConsumer

    try:
        consume_messages(
            topic=topic,
            options=options,
            consumer_factory=KafkaConsumer,
            max_messages=args.max_messages,
        )
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()

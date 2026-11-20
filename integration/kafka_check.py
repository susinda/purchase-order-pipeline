import os

from kafka import KafkaConsumer

from .kafka_config import connection_options, required_setting


def main():
    topic = required_setting(os.environ, "KAFKA_TOPIC")
    consumer = KafkaConsumer(
        **connection_options(os.environ),
        client_id="purchaseorders-smoke",
        group_id=None,
        enable_auto_commit=False,
        allow_auto_create_topics=False,
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise SystemExit(f"Kafka returned no visible partitions for topic {topic}")
        print(f"Kafka returned {len(partitions)} partitions for topic {topic}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

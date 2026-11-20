import os

from kafka import KafkaConsumer

from .kafka_config import connection_options, kafka_topic


def main():
    topic = kafka_topic(os.environ)
    consumer = KafkaConsumer(
        **connection_options(os.environ, role="producer"),
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

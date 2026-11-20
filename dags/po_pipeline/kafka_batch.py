from collections import Counter, defaultdict
import logging
import os
from time import monotonic

from .events import (
    ACK_TIMEOUT_SECONDS, PermanentBatchError, build_batch, decode_event, fetch_extract, json_bytes, validate_manifest,
)
from .kafka_config import connection_options, kafka_group_id, kafka_topic
from .settings import api_credentials, application_setting


logger = logging.getLogger(__name__)


def destination_groups():
    backend = kafka_group_id(os.environ, "backend")
    analytics = kafka_group_id(os.environ, "analytics")
    if not backend.strip() or not analytics.strip() or backend == analytics:
        raise PermanentBatchError("Configure distinct Kafka group IDs for backend and analytics")
    return backend, analytics


def publish_batch(load_type, run_id):
    from kafka import KafkaProducer

    env = os.environ
    topic = kafka_topic(env)
    extract = fetch_extract(
        application_setting("source.api.url", env), load_type, api_credentials("source", env),
    )
    manifest, events = build_batch(extract, load_type, run_id)
    manifest.update(topic=topic, records=[])
    if events:
        producer = KafkaProducer(
            **connection_options(env, role="producer"), acks="all", enable_idempotence=True,
            max_in_flight_requests_per_connection=1, max_block_ms=ACK_TIMEOUT_SECONDS * 1000,
        )
        try:
            for event in events:
                receipt = producer.send(topic, key=event["order_number"].encode(), value=json_bytes(event))
                metadata = receipt.get(timeout=ACK_TIMEOUT_SECONDS)
                manifest["records"].append({
                    "event_id": event["event_id"], "partition": metadata.partition, "offset": metadata.offset,
                })
        finally:
            producer.close(timeout=ACK_TIMEOUT_SECONDS)
    validate_manifest(manifest)
    logger.info("Kafka acknowledged batch %s with %s events", manifest["batch_id"], manifest["expected_count"])
    return manifest


def consume_batch(conf, group_id, deliver, *, role=None):
    from kafka import KafkaConsumer, TopicPartition

    manifest = validate_manifest(conf)
    counts = Counter()
    if not manifest["records"]:
        logger.info("Batch %s is empty", manifest["batch_id"])
        return {}
    topic = manifest["topic"]
    if topic != kafka_topic(os.environ):
        raise PermanentBatchError("Manifest topic does not match the configured Kafka topic")
    timeout = int(application_setting("kafka.batch.timeout.seconds", override="KAFKA_BATCH_TIMEOUT_SECONDS"))
    if timeout <= 0:
        raise ValueError("KAFKA_BATCH_TIMEOUT_SECONDS must be positive")
    positions = defaultdict(dict)
    for record in manifest["records"]:
        positions[TopicPartition(topic, record["partition"])][record["offset"]] = record["event_id"]

    deadline = monotonic() + timeout
    consumer = KafkaConsumer(
        **connection_options(os.environ, role=role), group_id=group_id,
        enable_auto_commit=False, auto_offset_reset="none",
        allow_auto_create_topics=False,
    )
    try:
        _deliver_positions(consumer, positions, manifest, deliver, counts, deadline)
        _commit_contiguous(consumer, positions, deadline)
    finally:
        logger.info("Batch %s group %s outcomes=%s", manifest["batch_id"], group_id, dict(counts))
        consumer.close(autocommit=False, timeout_ms=5000)
    return dict(counts)


def _deliver_positions(consumer, positions, manifest, deliver, counts, deadline):
    remaining = {partition: dict(offsets) for partition, offsets in positions.items()}
    consumer.assign(list(remaining))
    for partition, offsets in remaining.items():
        consumer.seek(partition, min(offsets))

    while remaining:
        seconds = deadline - monotonic()
        if seconds <= 0:
            missing = sum(len(offsets) for offsets in remaining.values())
            raise TimeoutError(f"Batch {manifest['batch_id']} still has {missing} undelivered events")
        for partition, messages in consumer.poll(timeout_ms=min(1000, max(1, int(seconds * 1000))), max_records=100).items():
            for message in messages:
                if partition not in remaining or message.offset not in remaining[partition]:
                    counts["outside_manifest"] += 1
                    continue
                if monotonic() >= deadline:
                    raise TimeoutError(f"Delivery deadline reached for batch {manifest['batch_id']}")
                event_id = remaining[partition][message.offset]
                try:
                    event = decode_event(message, event_id, manifest)
                    outcome = deliver(event)
                except Exception:
                    counts["failed"] += 1
                    logger.error("Delivery failed for event %s at %s:%s", event_id, partition, message.offset)
                    raise
                counts[outcome] += 1
                del remaining[partition][message.offset]
                if not remaining[partition]:
                    del remaining[partition]
                    consumer.pause(partition)


def _commit_contiguous(consumer, positions, deadline):
    from kafka.structs import OffsetAndMetadata

    def remaining_ms():
        milliseconds = int((deadline - monotonic()) * 1000)
        if milliseconds <= 0:
            raise TimeoutError("Kafka batch deadline reached before offset commits finished")
        return milliseconds

    commits = {}
    for partition, delivered in positions.items():
        start = consumer.committed(partition, timeout_ms=remaining_ms())
        if start is None:
            start = consumer.beginning_offsets([partition], timeout_ms=remaining_ms())[partition]
        end = start
        while end in delivered:
            end += 1
        if end > start:
            commits[partition] = OffsetAndMetadata(end, "")
        if any(offset >= end for offset in delivered):
            logger.warning("Offset for %s held at %s to preserve a gap outside this batch", partition, end)
    if commits:
        consumer.commit(offsets=commits, timeout_ms=remaining_ms())

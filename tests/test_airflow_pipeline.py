import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from kafka import TopicPartition

from dags.po_pipeline.events import PermanentBatchError, build_batch, json_bytes
from dags.po_pipeline.kafka_batch import consume_batch, publish_batch
from dags.po_pipeline.kafka_config import connection_options, kafka_group_id, kafka_topic
from dags.po_pipeline import warehouse


SAMPLE_DATA = Path(__file__).resolve().parents[1] / "api" / "source" / "sample_data"
ENV = {"KAFKA_TOPIC": "purchaseorders", "KAFKA_BOOTSTRAP_SERVERS": "broker.example", "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"}
WAREHOUSE_ENV = {
    "SNOWFLAKE_RAW_SCHEMA": "TEST_DB.RAW",
    "SNOWFLAKE_RELATIONAL_SCHEMA": "TEST_DB.RELATIONAL",
    "SNOWFLAKE_DBT_PROJECT": "TEST_DB.DBT.PURCHASE_ORDERS",
}


def extract(load_type):
    return json.loads((SAMPLE_DATA / f"purchased_order_{load_type}.json").read_bytes())


class BatchTest(unittest.TestCase):
    def setUp(self):
        data = extract("initial")
        data["purchased_orders"] = data["purchased_orders"][:2]
        data["record_count"] = 2
        self.manifest, self.events = build_batch(data, "initial", "run-one")
        self.manifest.update(topic="purchaseorders", records=[
            {"event_id": self.events[0]["event_id"], "partition": 0, "offset": 7},
            {"event_id": self.events[1]["event_id"], "partition": 0, "offset": 9},
        ])
        self.partition = TopicPartition("purchaseorders", 0)
        self.messages = {
            self.partition: [self.message(self.events[0], 7), SimpleNamespace(offset=8), self.message(self.events[1], 9)],
        }
        self.consumer = self.enterContext(patch("kafka.KafkaConsumer")).return_value
        self.consumer.poll.return_value = self.messages
        self.consumer.committed.return_value = 7
        self.enterContext(patch.dict("os.environ", ENV, clear=True))

    @staticmethod
    def message(event, offset):
        return SimpleNamespace(
            key=event["order_number"].encode(), value=json_bytes(event),
            topic="purchaseorders", partition=0, offset=offset,
        )

    def test_skips_an_unrelated_offset_without_committing_across_it(self):
        deliver = Mock(return_value="inserted")
        result = consume_batch(self.manifest, "backend", deliver)
        self.assertEqual(result, {"inserted": 2, "outside_manifest": 1})
        self.assertEqual([call.args[0] for call in deliver.call_args_list], self.events)
        self.assertEqual(self.consumer.commit.call_args.kwargs["offsets"][self.partition].offset, 8)
        self.consumer.close.assert_called_once_with(autocommit=False, timeout_ms=5000)

    def test_delivery_failure_leaves_offsets_uncommitted(self):
        deliver = Mock(side_effect=["inserted", TimeoutError("Target timed out")])
        with self.assertRaises(TimeoutError):
            consume_batch(self.manifest, "backend", deliver)
        self.consumer.commit.assert_not_called()

    def test_manifest_position_cannot_deliver_a_different_batch(self):
        self.messages[self.partition][0] = self.message(self.events[0] | {"batch_id": "f" * 64}, 7)
        deliver = Mock()
        with self.assertRaises(PermanentBatchError):
            consume_batch(self.manifest, "backend", deliver)
        deliver.assert_not_called()
        self.consumer.commit.assert_not_called()

    def test_publisher_returns_only_acknowledged_positions_with_stable_ids(self):
        env = {"SOURCE_API_URL": "https://source.example", "SOURCE_API_USERNAME": "test", "SOURCE_API_PASSWORD": "test"}
        data = extract("incremental")
        manifest, events = build_batch(data, "incremental", "run-one")
        self.assertEqual((manifest, events), build_batch(data, "incremental", "run-one"))
        self.assertNotEqual(manifest["batch_id"], build_batch(data, "incremental", "run-two")[0]["batch_id"])
        with patch.dict("os.environ", env), patch("dags.po_pipeline.kafka_batch.fetch_extract", return_value=data), \
             patch("kafka.KafkaProducer") as factory:
            broker = factory.return_value
            broker.send.return_value.get.side_effect = [
                SimpleNamespace(partition=1, offset=100 + offset) for offset in range(len(events))
            ]
            published = publish_batch("incremental", "run-one")
        self.assertEqual(published["records"][0], {"event_id": events[0]["event_id"], "partition": 1, "offset": 100})
        self.assertEqual(published["expected_count"], 20)
        first = broker.send.call_args_list[0].kwargs
        self.assertEqual(first["key"], events[0]["order_number"].encode())
        self.assertEqual(json.loads(first["value"]), events[0])

    def test_kafka_role_credentials_override_only_their_own_fallback(self):
        settings = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker.example",
            "KAFKA_SASL_USERNAME": "shared",
            "KAFKA_SASL_PASSWORD": "shared-password",
            "KAFKA_BACKEND_SASL_USERNAME": "backend",
            "KAFKA_BACKEND_SASL_PASSWORD": "backend-password",
        }
        self.assertEqual(connection_options(settings, role="backend")["sasl_plain_username"], "backend")
        self.assertEqual(connection_options(settings, role="analytics")["sasl_plain_username"], "shared")
        del settings["KAFKA_BACKEND_SASL_PASSWORD"]
        with self.assertRaisesRegex(ValueError, "KAFKA_BACKEND_SASL_PASSWORD"):
            connection_options(settings, role="backend")

    def test_kafka_uses_properties_for_names_and_environment_for_passwords(self):
        properties = {
            "kafka.topic": "configured-topic",
            "kafka.backend.group": "configured-group",
            "kafka.consumer.username": "configured-user",
            "kafka.consumer.password": "${CONSUMER_TEST_PASSWORD}",
            "kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP_SERVERS}",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "SCRAM-SHA-256",
            "aiven.api.token": "${AIVEN_API_TOKEN}",
        }
        settings = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker.example",
            "KAFKA_CONSUMER_SASL_PASSWORD": "test-${password}",
            "CONSUMER_TEST_PASSWORD": "test-${password}",
            "KAFKA_BACKEND_SASL_USERNAME": "legacy-user",
        }
        with patch("dags.po_pipeline.settings.application_properties", return_value=properties):
            self.assertEqual(kafka_topic(settings), "configured-topic")
            self.assertEqual(kafka_group_id(settings, "backend"), "configured-group")
            for role in ("backend", "analytics"):
                options = connection_options(settings, role=role)
                self.assertEqual(options["sasl_plain_username"], "configured-user")
                self.assertEqual(options["sasl_plain_password"], "test-${password}")
            self.assertEqual(kafka_topic(settings | {"KAFKA_TOPIC": "explicit-topic"}), "explicit-topic")


class WarehouseTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.dict("os.environ", WAREHOUSE_ENV, clear=True))
        self.cursor = self.enterContext(patch.object(warehouse, "connect")).return_value.cursor.return_value

    def test_partial_raw_batch_does_not_touch_relational_tables(self):
        manifest, events = build_batch(extract("incremental"), "incremental", "run-one")
        manifest.update(topic="purchaseorders", records=[
            {"event_id": event["event_id"], "partition": 0, "offset": index}
            for index, event in enumerate(events)
        ])
        self.cursor.fetchall.return_value = [(events[0]["event_id"],)]
        with self.assertRaisesRegex(PermanentBatchError, "full manifest"):
            warehouse.prepare_relational(manifest)
        self.cursor.execute.assert_called_once()
        self.assertIn("SELECT EVENT_ID", self.cursor.execute.call_args.args[0])

    def test_snowflake_success_flag_controls_dbt_task_result(self):
        self.cursor.sfqid = "query-id"
        self.cursor.fetchone.return_value = (False, "test failure")
        with self.assertRaisesRegex(RuntimeError, "query-id"):
            warehouse.build_dbt_project()
        self.cursor.fetchone.return_value = (True,)
        warehouse.build_dbt_project()
        self.assertIn("ARGS = 'build'", self.cursor.execute.call_args.args[0])

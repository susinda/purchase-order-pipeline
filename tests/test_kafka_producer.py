import io
import json
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from integration import kafka_producer


SAMPLE_DATA = Path(__file__).resolve().parents[1] / "requirements_docs" / "sample_data"


class KafkaProducerTest(unittest.TestCase):
    def setUp(self):
        self.producer = Mock()
        self.factory = Mock(return_value=self.producer)
        self.options = {"bootstrap_servers": "broker.example.invalid:18787"}

    def publish(self, load_type):
        return kafka_producer.publish_load(
            load_type,
            source_url="http://source.local:8000/",
            topic="purchaseorders",
            producer_factory=self.factory,
            producer_options=self.options,
        )

    def test_kafka_configuration_uses_environment(self):
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker.example.invalid:18787",
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
            "KAFKA_SASL_USERNAME": "test-user",
            "KAFKA_SASL_PASSWORD": "test-password",
            "KAFKA_SSL_CAFILE": "/tmp/test-ca.pem",
        }
        self.assertEqual(
            kafka_producer.kafka_config(env),
            {
                "bootstrap_servers": env["KAFKA_BOOTSTRAP_SERVERS"],
                "security_protocol": env["KAFKA_SECURITY_PROTOCOL"],
                "sasl_mechanism": env["KAFKA_SASL_MECHANISM"],
                "sasl_plain_username": env["KAFKA_SASL_USERNAME"],
                "sasl_plain_password": env["KAFKA_SASL_PASSWORD"],
                "ssl_cafile": env["KAFKA_SSL_CAFILE"],
                "acks": "all",
                "max_block_ms": 30000,
            },
        )
        del env["KAFKA_SASL_PASSWORD"]
        with self.assertRaisesRegex(ValueError, "KAFKA_SASL_PASSWORD"):
            kafka_producer.kafka_config(env)

    def test_publishes_both_fixtures_with_their_full_orders(self):
        for load_type, expected_count in (("initial", 100), ("incremental", 20)):
            with self.subTest(load_type=load_type):
                self.factory.reset_mock()
                self.producer.reset_mock()
                fixture = (SAMPLE_DATA / f"purchased_order_{load_type}.json").read_bytes()
                extract = json.loads(fixture)
                with patch.object(kafka_producer, "urlopen", return_value=io.BytesIO(fixture)) as http:
                    batch_id, count = self.publish(load_type)

                http.assert_called_once_with(
                    f"http://source.local:8000/purchase-orders?load_type={load_type}", timeout=30
                )
                self.factory.assert_called_once_with(**self.options)
                self.assertEqual(count, expected_count)
                self.assertEqual(self.producer.send.return_value.get.call_count, count)
                self.producer.close.assert_called_once_with(timeout=30)

                event_ids = set()
                for order, call in zip(
                    extract["purchased_orders"], self.producer.send.call_args_list, strict=True
                ):
                    self.assertEqual(call.args, ("purchaseorders",))
                    self.assertEqual(call.kwargs["key"], order["OrderNumber"].encode())
                    self.assertEqual(json.loads(call.kwargs["value"]), order)
                    headers = dict(call.kwargs["headers"])
                    self.assertEqual(headers["batch_id"], batch_id.encode())
                    self.assertEqual(headers["load_type"], load_type.encode())
                    self.assertEqual(headers["extracted_at"], extract["extracted_at"].encode())
                    event_ids.add(headers["event_id"])
                self.assertEqual(len(event_ids), expected_count)

    def test_replay_keeps_ids_and_preserves_both_versions_of_an_order(self):
        extract = json.loads((SAMPLE_DATA / "purchased_order_incremental.json").read_bytes())
        batch_id, messages = kafka_producer.prepare_messages(extract, "incremental")
        self.assertEqual(
            kafka_producer.prepare_messages(extract, "incremental"), (batch_id, messages)
        )
        version_ids = [
            dict(headers)["event_id"] for key, _, headers in messages if key == b"PO1040078"
        ]
        self.assertEqual(len(set(version_ids)), 2)

    def test_invalid_extract_does_not_open_a_kafka_connection(self):
        extract = {"load_type": "initial", "record_count": 1, "purchased_orders": []}
        with patch.object(kafka_producer, "fetch_extract", return_value=extract):
            with self.assertRaisesRegex(ValueError, "record_count"):
                self.publish("initial")
        self.factory.assert_not_called()

    def test_broker_acknowledgement_failure_closes_producer_and_fails_run(self):
        extract = json.loads((SAMPLE_DATA / "purchased_order_initial.json").read_bytes())
        self.producer.send.return_value.get.side_effect = TimeoutError("broker timed out")
        with patch.object(kafka_producer, "fetch_extract", return_value=extract):
            with self.assertRaisesRegex(TimeoutError, "broker timed out"):
                self.publish("initial")
        self.producer.send.assert_called_once()
        self.producer.close.assert_called_once_with(timeout=30)

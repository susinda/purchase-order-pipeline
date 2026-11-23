import unittest
from unittest.mock import patch

from integration.kafka_config import connection_options, kafka_group_id, kafka_topic


class KafkaConfigTest(unittest.TestCase):
    def test_producer_credentials_leave_inspection_fallback_available(self):
        settings = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker.example",
            "KAFKA_SASL_USERNAME": "shared",
            "KAFKA_SASL_PASSWORD": "shared-password",
            "KAFKA_PRODUCER_SASL_USERNAME": "producer",
            "KAFKA_PRODUCER_SASL_PASSWORD": "producer-password",
        }
        self.assertEqual(connection_options(settings, role="producer")["sasl_plain_username"], "producer")
        self.assertEqual(connection_options(settings, role="inspection")["sasl_plain_username"], "shared")
        del settings["KAFKA_PRODUCER_SASL_PASSWORD"]
        with self.assertRaisesRegex(ValueError, "KAFKA_PRODUCER_SASL_PASSWORD"):
            connection_options(settings, role="producer")

    def test_kafka_uses_properties_for_names_and_environment_for_passwords(self):
        properties = {
            "kafka.topic": "configured-topic",
            "kafka.inspection.group": "configured-group",
            "kafka.inspection.username": "configured-user",
            "kafka.inspection.password": "${INSPECTION_TEST_PASSWORD}",
            "kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP_SERVERS}",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "SCRAM-SHA-256",
        }
        settings = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker.example",
            "KAFKA_INSPECTION_SASL_PASSWORD": "test-${password}",
            "INSPECTION_TEST_PASSWORD": "test-${password}",
        }
        with patch("integration.po_pipeline.settings.application_properties", return_value=properties):
            self.assertEqual(kafka_topic(settings), "configured-topic")
            self.assertEqual(kafka_group_id(settings, "inspection"), "configured-group")
            options = connection_options(settings, role="inspection")
            self.assertEqual(options["sasl_plain_username"], "configured-user")
            self.assertEqual(options["sasl_plain_password"], "test-${password}")
            self.assertEqual(kafka_topic(settings | {"KAFKA_TOPIC": "explicit-topic"}), "explicit-topic")

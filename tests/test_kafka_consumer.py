import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call, patch

from integration import kafka_consumer


class KafkaConsumerTest(unittest.TestCase):
    def setUp(self):
        self.consumer = MagicMock()
        self.factory = Mock(return_value=self.consumer)
        self.message = SimpleNamespace(
            topic="purchaseorders",
            partition=0,
            offset=7,
            key=b"PO1040078",
            value=b'{"OrderNumber":"PO1040078"}',
        )
        self.options = kafka_consumer.consumer_options(
            {
                "KAFKA_BOOTSTRAP_SERVERS": "broker.example.invalid:9092",
                "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
            }
        )

    def consume(self, **kwargs):
        return kafka_consumer.consume_messages(
            topic="purchaseorders",
            options=self.options,
            consumer_factory=self.factory,
            **kwargs,
        )

    def test_consumer_group_client_and_offset_reset_can_be_configured(self):
        options = kafka_consumer.consumer_options({
            "KAFKA_BOOTSTRAP_SERVERS": "broker.example.invalid:18787",
            "KAFKA_SASL_USERNAME": "test-user",
            "KAFKA_SASL_PASSWORD": "test-password",
            "KAFKA_SSL_CAFILE": "/tmp/test-ca.pem",
            "KAFKA_CONSUMER_GROUP_ID": "test-group",
            "KAFKA_CONSUMER_CLIENT_ID": "test-client",
            "KAFKA_AUTO_OFFSET_RESET": "latest",
        })
        self.assertEqual(options["security_protocol"], "SASL_SSL")
        self.assertEqual(options["ssl_cafile"], "/tmp/test-ca.pem")
        self.assertEqual(options["group_id"], "test-group")
        self.assertEqual(options["client_id"], "test-client")
        self.assertEqual(options["auto_offset_reset"], "latest")
        self.assertFalse(options["enable_auto_commit"])

    def test_prints_before_committing_and_stops_at_limit(self):
        iterator = iter([self.message, self.message])
        self.consumer.__iter__.return_value = iterator
        operations = Mock()
        operations.attach_mock(self.consumer.commit, "commit")
        operations.attach_mock(self.consumer.close, "close")
        with patch("builtins.print", operations.print):
            self.assertEqual(self.consume(max_messages=1), 1)

        self.factory.assert_called_once_with("purchaseorders", **self.options)
        self.assertIs(next(iterator), self.message)
        self.assertEqual(operations.mock_calls, [
            call.print(
                "Consumed from purchaseorders[0] offset=7: "
                'key=PO1040078, value={"OrderNumber":"PO1040078"}',
                flush=True,
            ),
            call.commit(),
            call.close(autocommit=False),
        ])

    def test_output_failure_does_not_commit_and_closes_consumer(self):
        self.consumer.__iter__.return_value = [self.message]
        with patch("builtins.print", side_effect=BrokenPipeError):
            with self.assertRaises(BrokenPipeError):
                self.consume()
        self.consumer.commit.assert_not_called()
        self.consumer.close.assert_called_once_with(autocommit=False)

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, patch

from integration import k_producer


class SingleMessageProducerTest(unittest.TestCase):
    def test_sends_one_json_value_to_cli_topic_using_environment_config(self):
        factory = Mock()
        producer = factory.return_value
        producer.send.return_value.get.return_value = SimpleNamespace(
            topic="test-topic", partition=2, offset=7
        )
        env = {
            "KAFKA_TOPIC": "batch-topic",
            "KAFKA_BOOTSTRAP_SERVERS": "broker.example.invalid:18787",
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_SASL_USERNAME": "test-user",
            "KAFKA_SASL_PASSWORD": "test-password",
            "KAFKA_SSL_CAFILE": "/tmp/test-ca.pem",
        }
        message = '{"message":"café","value":1.2300}'
        output = io.StringIO()
        with (
            patch.dict("sys.modules", {"kafka": SimpleNamespace(KafkaProducer=factory)}),
            patch.dict("os.environ", env, clear=True),
            redirect_stdout(output),
        ):
            k_producer.main(["test-topic", message])

        self.assertEqual(factory.call_args.kwargs["bootstrap_servers"], env["KAFKA_BOOTSTRAP_SERVERS"])
        self.assertEqual(factory.call_args.kwargs["security_protocol"], env["KAFKA_SECURITY_PROTOCOL"])
        self.assertEqual(factory.call_args.kwargs["ssl_cafile"], env["KAFKA_SSL_CAFILE"])
        self.assertEqual(factory.call_args.kwargs["sasl_plain_username"], env["KAFKA_SASL_USERNAME"])
        self.assertEqual(factory.call_args.kwargs["sasl_plain_password"], env["KAFKA_SASL_PASSWORD"])
        producer.send.assert_called_once_with("test-topic", value=message.encode("utf-8"))
        producer.send.return_value.get.assert_called_once_with(timeout=30)
        producer.close.assert_called_once_with(timeout=30)
        self.assertEqual(output.getvalue(), "Published to test-topic[2] at offset 7\n")

    def test_rejects_invalid_json_before_reading_kafka_config(self):
        for message in ('{"message":}', '{"number":NaN}'):
            with self.subTest(message=message), redirect_stderr(io.StringIO()) as error:
                with patch.object(k_producer, "kafka_config") as config:
                    with self.assertRaises(SystemExit) as exit_error:
                        k_producer.main(["test-topic", message])
                self.assertEqual(exit_error.exception.code, 2)
                self.assertIn("Invalid JSON message", error.getvalue())
                config.assert_not_called()

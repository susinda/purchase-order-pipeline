from contextlib import redirect_stderr, redirect_stdout
from io import BytesIO, StringIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlsplit

from integration import provision_kafka


class KafkaProvisionTest(unittest.TestCase):
    def setUp(self):
        folder = self.enterContext(TemporaryDirectory())
        self.properties = Path(folder) / "application.properties"
        self.properties.write_text(
            "aiven.project=${AIVEN_PROJECT}\n"
            "aiven.service.name=${AIVEN_SERVICE_NAME}\n"
            "aiven.api.token=${AIVEN_API_TOKEN}\n"
            "kafka.topic=orders\n"
            "kafka.backend.dlt=orders.backend.dlt\n"
            "kafka.analytics.dlt=orders.analytics.dlt\n"
            "kafka.producer.username=orders-producer\n"
            "kafka.backend.username=orders-backend\n"
            "kafka.analytics.username=orders-analytics\n"
            "kafka.backend.group=orders-target\n"
            "kafka.analytics.group=orders-reporting\n"
            "kafka.topic.partitions=1\n"
            "kafka.topic.replication=2\n"
            "kafka.topic.retention.hours=72\n",
            encoding="utf-8",
        )
        self.enterContext(patch("sys.argv", ["provision_kafka", "--properties", str(self.properties)]))
        self.enterContext(patch.dict("os.environ", {
            "AIVEN_PROJECT": "demo-project",
            "AIVEN_SERVICE_NAME": "demo-service",
            "AIVEN_API_TOKEN": "test-token",
        }, clear=True))
        self.output = self.enterContext(redirect_stdout(StringIO()))
        self.topics = {"existing-topic": {"topic_name": "existing-topic"}}
        self.users = {"orders-producer": {"username": "orders-producer", "password": "existing-password"}}
        self.created_topics = []
        self.created_users = []
        self.acls = []

    def aiven(self, request, *, timeout):
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")
        prefix = "/v1/project/demo-project/service/demo-service"
        url = urlsplit(request.full_url)
        self.assertEqual(url.netloc, "api.aiven.io")
        self.assertTrue(url.path.startswith(prefix + "/"))
        path = url.path.removeprefix(prefix)
        payload = json.loads(request.data) if request.data else None
        if request.get_method() == "GET" and path.startswith(("/topic/", "/user/")):
            _, resource, name = path.split("/")
            state = self.topics if resource == "topic" else self.users
            if name not in state:
                raise HTTPError(request.full_url, 404, "Not Found", {}, BytesIO(b"{}"))
            return BytesIO(json.dumps({resource: state[name]}).encode())
        if (request.get_method(), path) == ("POST", "/topic"):
            topic = payload["topic_name"]
            self.created_topics.append(topic)
            self.topics[topic] = payload
            return BytesIO(b"{}")
        if (request.get_method(), path) == ("POST", "/user"):
            username = payload["username"]
            self.created_users.append(username)
            self.users[username] = payload | {"password": "generated-password"}
            return BytesIO(json.dumps({"user": self.users[username]}).encode())
        if (request.get_method(), path) == ("GET", "/kafka/acl"):
            return BytesIO(json.dumps({"kafka_acl": self.acls}).encode())
        if (request.get_method(), path) == ("POST", "/kafka/acl"):
            self.acls.append(payload | {"id": f"acl{len(self.acls)}"})
            return BytesIO(b"{}")
        self.fail(f"Unexpected Aiven request: {request.get_method()} {path}")

    def test_provisions_topics_users_and_exact_grants_once(self):
        with patch.object(provision_kafka, "urlopen", side_effect=self.aiven):
            provision_kafka.main()
            provision_kafka.main()
        self.assertEqual(self.created_topics, ["orders", "orders.backend.dlt", "orders.analytics.dlt"])
        self.assertEqual(self.created_users, ["orders-backend", "orders-analytics"])
        self.assertEqual(self.topics["orders"]["partitions"], 1)
        self.assertEqual(self.users["orders-producer"]["password"], "existing-password")
        self.assertNotIn("password", self.output.getvalue())
        self.assertEqual(len(self.acls), 7)
        self.assertEqual({
            (acl["principal"], acl["resource_type"], acl["resource_name"], acl["operation"]) for acl in self.acls
        }, {
            ("User:orders-producer", "Topic", "orders", "Write"),
            ("User:orders-backend", "Topic", "orders", "Read"),
            ("User:orders-backend", "Topic", "orders.backend.dlt", "Write"),
            ("User:orders-backend", "Group", "orders-target", "Read"),
            ("User:orders-analytics", "Topic", "orders", "Read"),
            ("User:orders-analytics", "Topic", "orders.analytics.dlt", "Write"),
            ("User:orders-analytics", "Group", "orders-reporting", "Read"),
        })
        for acl in self.acls:
            self.assertEqual(acl["permission_type"], "ALLOW")
            self.assertEqual(acl["pattern_type"], "LITERAL")

    def test_rejects_wildcard_group_before_contacting_aiven(self):
        settings = self.properties.read_text(encoding="utf-8")
        self.properties.write_text(settings.replace("orders-target", "*"), encoding="utf-8")
        with patch.object(provision_kafka, "urlopen") as urlopen, redirect_stderr(StringIO()) as stderr:
            with self.assertRaises(SystemExit) as error:
                provision_kafka.main()
        self.assertEqual(error.exception.code, 1)
        self.assertIn("without whitespace or wildcards", stderr.getvalue())
        urlopen.assert_not_called()

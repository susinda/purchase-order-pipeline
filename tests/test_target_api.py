import json
import unittest
from unittest.mock import patch

import pymysql
from fastapi.testclient import TestClient

from api.target.main import app


class TargetApiTest(unittest.TestCase):
    def setUp(self):
        env = {
            "TARGET_API_USERNAME": "target-user",
            "TARGET_API_PASSWORD": "test-api-password",
            "MYSQL_PASSWORD": "test-db-password",
        }
        self.enterContext(patch.dict("os.environ", env, clear=True))
        self.connect = self.enterContext(patch("api.target.main.pymysql.connect"))
        connection = self.connect.return_value.__enter__.return_value
        self.cursor = connection.cursor.return_value.__enter__.return_value
        self.client = self.enterContext(TestClient(app))
        self.auth = ("target-user", "test-api-password")
        self.url = "/purchase-orders/PO1040078"
        self.snapshot = {
            "OrderNumber": "PO1040078",
            "OrderLines": [{"LineNumber": "1", "GoodsReceipts": [{"ReceivedQuantity": -2}]}],
        }

    def test_upsert_responses_and_full_snapshot(self):
        for rows, status_code, result in (
            (1, 201, "inserted"),
            (2, 200, "updated"),
            (0, 200, "duplicate"),
        ):
            with self.subTest(result=result):
                self.cursor.execute.return_value = rows
                response = self.client.put(self.url, json=self.snapshot, auth=self.auth)
                self.assertEqual(response.status_code, status_code)
                self.assertEqual(response.json(), {"order_number": "PO1040078", "status": result})
                _, (order_number, snapshot) = self.cursor.execute.call_args.args
                self.assertEqual(order_number, "PO1040078")
                self.assertEqual(json.loads(snapshot), self.snapshot)
                self.assertTrue(self.connect.call_args.kwargs["autocommit"])

    def test_authentication_failures_do_not_connect_to_mysql(self):
        for auth in (None, ("wrong-user", self.auth[1]), (self.auth[0], "wrong-password")):
            with self.subTest(auth=auth):
                response = self.client.put(self.url, json=self.snapshot, auth=auth)
                self.assertEqual(response.status_code, 401)
                self.assertEqual(response.headers["www-authenticate"], 'Basic realm="purchase-order-target"')
        self.connect.assert_not_called()

    def test_rejects_a_batch_or_mismatched_order_number(self):
        for payload in ([self.snapshot], {"purchased_orders": [self.snapshot]}, {"OrderNumber": "PO-NEXT"}):
            with self.subTest(payload=payload):
                response = self.client.put(self.url, json=payload, auth=self.auth)
                self.assertEqual(response.status_code, 422)
        self.connect.assert_not_called()

    def test_health_reports_the_image_tag_after_querying_mysql(self):
        with patch.dict("os.environ", {"APP_IMAGE_TAG": "target-test-tag"}):
            response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})
        self.assertEqual(response.headers["X-Image-Tag"], "target-test-tag")
        self.cursor.execute.assert_called_once_with("SELECT 1 FROM purchase_orders LIMIT 1")

    def test_requires_basic_auth_for_swagger_and_openapi(self):
        for path in ("/docs", "/openapi.json"):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 401)
                self.assertEqual(self.client.get(path, auth=self.auth).status_code, 200)
        schema = self.client.get("/openapi.json", auth=self.auth).json()
        self.assertEqual(
            schema["paths"]["/purchase-orders/{order_number}"]["put"]["security"],
            [{"HTTPBasic": []}],
        )
        self.connect.assert_not_called()

    def test_database_errors_return_5xx(self):
        self.connect.side_effect = pymysql.OperationalError(2003, "connection refused")
        with self.assertLogs("api.target.main", level="ERROR"):
            response = self.client.put(self.url, json=self.snapshot, auth=self.auth)
            health = self.client.get("/health")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(health.status_code, 503)
        self.assertNotIn("connection refused", response.text)

        self.connect.side_effect = None
        self.cursor.execute.side_effect = pymysql.ProgrammingError(1146, "table missing")
        with self.assertLogs("api.target.main", level="ERROR"):
            response = self.client.put(self.url, json=self.snapshot, auth=self.auth)
        self.assertEqual(response.status_code, 500)
        self.assertNotIn("table missing", response.text)

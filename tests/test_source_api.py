import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.source.main import app


SAMPLE_DATA = Path(__file__).resolve().parents[1] / "api" / "source" / "sample_data"


class SourceApiTest(unittest.TestCase):
    def setUp(self):
        self.auth = ("source-test-user", "source-test-password")
        self.enterContext(patch.dict("os.environ", {
            "SOURCE_API_USERNAME": self.auth[0],
            "SOURCE_API_PASSWORD": self.auth[1],
        }))

    def test_serves_both_full_extract_fixtures(self):
        with TestClient(app) as client:
            for load_type, record_count in (("initial", 100), ("incremental", 20)):
                with self.subTest(load_type=load_type):
                    fixture = SAMPLE_DATA / f"purchased_order_{load_type}.json"
                    expected = json.loads(fixture.read_text(encoding="utf-8"))
                    response = client.get(
                        "/purchase-orders", params={"load_type": load_type}, auth=self.auth
                    )
                    self.assertEqual(response.status_code, 200)
                    payload = response.json()
                    self.assertEqual(payload, expected)
                    self.assertEqual(len(payload["purchased_orders"]), record_count)

    def test_health_endpoint_reports_ready(self):
        with patch.dict("os.environ", {"APP_IMAGE_TAG": "source-test-tag"}):
            with TestClient(app) as client:
                response = client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})
        self.assertEqual(response.headers["X-Image-Tag"], "source-test-tag")

    def test_rejects_missing_and_invalid_load_type(self):
        with TestClient(app) as client:
            for params in ({}, {"load_type": "daily"}):
                with self.subTest(params=params):
                    response = client.get("/purchase-orders", params=params, auth=self.auth)
                    self.assertEqual(response.status_code, 422)

    def test_requires_basic_auth_for_orders_and_docs(self):
        with TestClient(app) as client:
            for path in ("/purchase-orders?load_type=initial", "/docs", "/openapi.json"):
                with self.subTest(path=path):
                    for auth in (None, (self.auth[0], "wrong-password")):
                        response = client.get(path, auth=auth)
                        self.assertEqual(response.status_code, 401)
                        self.assertEqual(
                            response.headers["www-authenticate"], 'Basic realm="purchase-order-source"'
                        )
            docs = client.get("/docs", auth=self.auth)
            self.assertEqual(docs.status_code, 200)
            self.assertIn("/openapi.json", docs.text)
            self.assertEqual(docs.headers["cache-control"], "no-store")
            schema = client.get("/openapi.json", auth=self.auth)
            self.assertEqual(schema.status_code, 200)
            self.assertEqual(schema.headers["cache-control"], "no-store")
            paths = schema.json()["paths"]
            self.assertEqual(paths["/purchase-orders"]["get"]["security"], [{"HTTPBasic": []}])
            self.assertNotIn("security", paths["/health"]["get"])

    def test_startup_fails_when_record_count_does_not_match_fixture(self):
        with TemporaryDirectory() as temp_dir:
            folder = Path(temp_dir)
            (folder / "purchased_order_initial.json").write_text(
                json.dumps({"load_type": "initial", "record_count": 1, "purchased_orders": []}),
                encoding="utf-8",
            )
            with patch("api.source.main.SAMPLE_DATA_DIR", folder):
                with self.assertRaisesRegex(ValueError, "record_count"):
                    with TestClient(app):
                        pass

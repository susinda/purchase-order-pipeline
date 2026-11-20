import argparse
import base64
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SAMPLE_DATA = Path(__file__).resolve().parents[1] / "api" / "source" / "sample_data"


def request_json(url, *, order=None, credentials=None, image_tag=None):
    headers = {}
    data = None
    if order is not None:
        data = json.dumps(order).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=headers, method="POST" if data is not None else "GET")
    if credentials:
        request.add_unredirected_header("Authorization", f"Basic {credentials}")
    try:
        response = urlopen(request, timeout=5)
    except HTTPError as error:
        response = error
    with response:
        if image_tag and response.headers.get("X-Image-Tag") != image_tag:
            return response.status, None
        body = response.read()
        if response.headers.get_content_type() == "application/json":
            return response.status, json.loads(body)
        return response.status, None


def expect(actual, expected, description):
    if actual != expected:
        raise RuntimeError(f"{description} failed (received HTTP {actual[0]})")


def check_health(base_url, image_tag=None):
    deadline = time.monotonic() + (300 if image_tag else 60)
    while True:
        try:
            status, body = request_json(f"{base_url}/health", image_tag=image_tag)
            if (status, body) == (200, {"status": "ok"}):
                print(f"Healthy at {base_url}/health")
                return
            last_failure = f"HTTP {status}"
            if status == 200 and image_tag:
                last_failure += f" without a healthy response for image tag {image_tag}"
        except (URLError, ConnectionError, TimeoutError) as error:
            last_failure = str(error)
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{base_url}/health did not become healthy: {last_failure}")
        time.sleep(2)


def basic_credentials(check):
    prefix = f"{check.upper()}_API"
    username = os.environ[f"{prefix}_USERNAME"]
    password = os.environ[f"{prefix}_PASSWORD"]
    return base64.b64encode(f"{username}:{password}".encode("ascii")).decode("ascii")


def check_docs(base_url, credentials):
    for path in ("/docs", "/openapi.json"):
        url = f"{base_url}{path}"
        if request_json(url)[0] != 401 or request_json(url, credentials=credentials)[0] != 200:
            raise RuntimeError(f"{path} did not enforce Basic Auth")


def check_source(base_url, credentials):
    if request_json(f"{base_url}/purchase-orders?load_type=initial")[0] != 401:
        raise RuntimeError("Source API accepted a request without Basic Auth")
    for load_type in ("initial", "incremental"):
        check_source_extract(base_url, credentials, load_type)


def check_source_extract(base_url, credentials, load_type):
    fixture = SAMPLE_DATA / f"purchased_order_{load_type}.json"
    payload = json.loads(fixture.read_bytes())
    response = request_json(
        f"{base_url}/purchase-orders?load_type={load_type}", credentials=credentials
    )
    expect(response, (200, payload), f"{load_type} extract comparison")
    print(f"Source API returned the full {load_type} fixture")


def check_target(base_url, credentials):
    order_number = "PO-CI"
    order = {"OrderNumber": order_number, "OrderLines": [{"LineNumber": "10", "Quantity": "1"}]}
    url = f"{base_url}/purchase-orders"
    if request_json(url, order=order)[0] != 401:
        raise RuntimeError("Target API accepted a request without Basic Auth")

    def submit(result):
        expect(
            request_json(url, order=order, credentials=credentials),
            (200, {"order_number": order_number, "status": result}),
            f"Target API {result} check",
        )

    submit("inserted")
    submit("duplicate")
    order["OrderLines"][0]["Quantity"] = "2"
    submit("updated")
    submit("duplicate")
    print("Target API verified Basic Auth, insert, update and duplicate handling against MySQL")


def main():
    parser = argparse.ArgumentParser(description="Check a running purchase-order API.")
    parser.add_argument("check", choices=("source", "source-smoke", "target", "health"))
    parser.add_argument("base_url")
    parser.add_argument("--image-tag", help="Wait for the running API to report this image tag.")
    args = parser.parse_args()
    base_url = args.base_url.rstrip("/")

    if args.check == "source-smoke":
        if not base_url.startswith("https://"):
            parser.error("source-smoke requires an HTTPS URL")
        check_source_extract(base_url, basic_credentials("source"), "incremental")
        return

    check_health(base_url, args.image_tag)
    if args.check == "health":
        return
    credentials = basic_credentials(args.check)
    check_docs(base_url, credentials)
    if args.check == "source":
        check_source(base_url, credentials)
    else:
        check_target(base_url, credentials)


if __name__ == "__main__":
    main()

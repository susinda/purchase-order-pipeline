import base64
import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen


LOAD_TYPES = ("initial", "incremental")
SOURCE_TIMEOUT_SECONDS = 30
ACK_TIMEOUT_SECONDS = 30


def json_bytes(payload):
    return json.dumps(
        payload, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def authenticated_request(url, credentials, **kwargs):
    request = Request(url, **kwargs)
    token = base64.b64encode(":".join(credentials).encode("ascii")).decode("ascii")
    request.add_unredirected_header("Authorization", f"Basic {token}")
    return request


def fetch_extract(source_url, load_type, source_auth, *, opener=urlopen):
    query = urlencode({"load_type": load_type})
    request = authenticated_request(f"{source_url.rstrip('/')}/purchase-orders?{query}", source_auth)
    with opener(request, timeout=SOURCE_TIMEOUT_SECONDS) as response:
        return json.load(response)


def validate_extract(extract, load_type):
    if not isinstance(extract, dict) or extract.get("load_type") != load_type:
        raise ValueError(f"Source must return a {load_type} extract")
    orders = extract.get("purchased_orders")
    count = extract.get("record_count")
    if not isinstance(orders, list) or type(count) is not int or count != len(orders):
        raise ValueError("record_count must match the purchased_orders array")
    extracted_at = extract.get("extracted_at")
    if not isinstance(extracted_at, str) or not extracted_at.strip():
        raise ValueError("Source extract must include extracted_at")
    return orders, extracted_at

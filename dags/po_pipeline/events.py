import base64
from datetime import datetime, timezone
import hashlib
import json
import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen


LOAD_TYPES = ("initial", "incremental")
PUBLISHER_DAG_ID = "dag_api_to_kafka"
SOURCE_TIMEOUT_SECONDS = 30
ACK_TIMEOUT_SECONDS = 30


class PermanentBatchError(ValueError):
    """The manifest, event, or delivery response needs intervention."""


def json_bytes(payload):
    return json.dumps(
        payload, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def digest(payload):
    return hashlib.sha256(json_bytes(payload)).hexdigest()


def utc_timestamp(value):
    if not isinstance(value, str):
        raise PermanentBatchError("Expected an ISO timestamp with a timezone")
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError as error:
        raise PermanentBatchError("Invalid ISO timestamp") from error
    if timestamp.utcoffset() is None:
        raise PermanentBatchError("Expected an ISO timestamp with a timezone")
    return timestamp.astimezone(timezone.utc).isoformat(timespec="microseconds")


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
        raise PermanentBatchError(f"Source must return a {load_type} extract")
    orders = extract.get("purchased_orders")
    count = extract.get("record_count")
    if not isinstance(orders, list) or type(count) is not int or count != len(orders):
        raise PermanentBatchError("record_count must match the purchased_orders array")
    extracted_at = extract.get("extracted_at")
    if not isinstance(extracted_at, str) or not extracted_at.strip():
        raise PermanentBatchError("Source extract must include extracted_at")
    return orders, extracted_at


def require_id(value, name):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise PermanentBatchError(f"Invalid {name}")
    return value


def require_text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise PermanentBatchError(f"Missing or invalid {name}")
    return value


def children(parent, field, key):
    rows = parent.get(field)
    if not isinstance(rows, list):
        raise PermanentBatchError(f"{field} must be an array")
    ids = set()
    for row in rows:
        if not isinstance(row, dict):
            raise PermanentBatchError(f"{field} must contain objects")
        row_id = require_text(row.get(key), key)
        if row_id in ids:
            raise PermanentBatchError(f"Repeated {key} within {field}")
        ids.add(row_id)
    return rows


def validate_order(order):
    if not isinstance(order, dict):
        raise PermanentBatchError("Order must be an object")
    order_number = require_text(order.get("OrderNumber"), "OrderNumber")
    if len(order_number) > 255:
        raise PermanentBatchError("OrderNumber exceeds the Target API limit")
    timestamp = utc_timestamp(order.get("LastUpdated"))
    receipt_numbers = set()
    for line in children(order, "OrderLines", "LineNumber"):
        children(line, "CodingRows", "RowIndex")
        for receipt in children(line, "GoodsReceipts", "GoodsReceiptNumber"):
            number = receipt["GoodsReceiptNumber"]
            if number in receipt_numbers:
                raise PermanentBatchError("GoodsReceiptNumber must be unique within an order snapshot")
            receipt_numbers.add(number)
    return order_number, timestamp


def build_batch(extract, load_type, run_id):
    if load_type not in LOAD_TYPES:
        raise PermanentBatchError("load_type must be initial or incremental")
    orders, extracted_at = validate_extract(extract, load_type)
    source = {
        "name": require_text(extract.get("source"), "source"),
        "extracted_at": utc_timestamp(extracted_at),
    }
    batch_id = digest([PUBLISHER_DAG_ID, run_id])
    events = []
    for index, order in enumerate(orders):
        number, updated = validate_order(order)
        order_hash = digest(order)
        events.append({
            "schema_version": 1,
            "batch_id": batch_id,
            "event_id": digest([batch_id, index, order_hash]),
            "record_index": index,
            "order_number": number,
            "last_updated": updated,
            "snapshot_hash": order_hash,
            "load_type": load_type,
            "source": source,
            "order": order,
        })
    return {
        "batch_id": batch_id,
        "load_type": load_type,
        "expected_count": len(events),
        "source": source,
    }, events


def validate_manifest(conf):
    if not isinstance(conf, dict):
        raise PermanentBatchError("Pass the publisher's batch manifest in dag_run.conf")
    require_id(conf.get("batch_id"), "batch_id")
    require_text(conf.get("topic"), "topic")
    if conf.get("load_type") not in LOAD_TYPES:
        raise PermanentBatchError("Invalid manifest load_type")
    source = conf.get("source")
    if not isinstance(source, dict):
        raise PermanentBatchError("Missing manifest source metadata")
    require_text(source.get("name"), "source name")
    utc_timestamp(source.get("extracted_at"))
    records = conf.get("records")
    expected = conf.get("expected_count")
    if type(expected) is not int or not isinstance(records, list) or len(records) != expected:
        raise PermanentBatchError("expected_count must match the manifest records")
    ids, positions = set(), set()
    for record in records:
        if not isinstance(record, dict):
            raise PermanentBatchError("Invalid record in manifest")
        event_id = require_id(record.get("event_id"), "event_id")
        partition, offset = record.get("partition"), record.get("offset")
        if any(type(value) is not int or value < 0 for value in (partition, offset)):
            raise PermanentBatchError("Invalid Kafka partition or offset in manifest")
        if event_id in ids or (partition, offset) in positions:
            raise PermanentBatchError("Repeated event or Kafka position in manifest")
        ids.add(event_id)
        positions.add((partition, offset))
    return conf


def decode_event(message, event_id, manifest):
    try:
        event = json.loads(message.value)
        if not isinstance(event, dict) or event.get("schema_version") != 1:
            raise PermanentBatchError("Unsupported event envelope")
        for field in ("batch_id", "load_type", "source"):
            if event.get(field) != manifest[field]:
                raise PermanentBatchError(f"Event {field} does not match the batch manifest")
        order = event.get("order")
        number, updated = validate_order(order)
        order_hash = digest(order)
        index = event.get("record_index")
        if type(index) is not int or not 0 <= index < manifest["expected_count"]:
            raise PermanentBatchError("Invalid event record index")
        if event.get("event_id") != event_id or digest([manifest["batch_id"], index, order_hash]) != event_id:
            raise PermanentBatchError("Event ID does not match the manifest or the payload")
        if event.get("snapshot_hash") != order_hash or event.get("last_updated") != updated:
            raise PermanentBatchError("Event version does not match its order")
        if event.get("order_number") != number or message.key != number.encode("utf-8"):
            raise PermanentBatchError("Event order number does not match its Kafka key")
    except (TypeError, ValueError) as error:
        raise PermanentBatchError(
            f"Invalid event at {message.topic}[{message.partition}] offset {message.offset}: {error}"
        ) from error
    return event

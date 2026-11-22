"""Read current and previous purchase-order Kafka message formats."""

import json


def read_event(raw, headers):
    event = json.loads(raw)
    if not isinstance(event, dict):
        raise ValueError("Expected JSON object")

    if "payload" in event:
        return event
    if event.get("schema_version") == 1 and "order" in event:
        return {**event, "payload": event["order"]}
    if "OrderNumber" in event:
        metadata = {
            name: value.decode("utf-8")
            for name, value in headers or ()
            if name in {"batch_id", "event_id", "load_type"} and value is not None
        }
        return {**metadata, "payload": event, "order_number": event["OrderNumber"]}
    return event

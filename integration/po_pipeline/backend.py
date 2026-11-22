import json
from urllib.error import HTTPError
from urllib.request import urlopen

from .events import PermanentBatchError, authenticated_request, json_bytes
from .settings import api_credentials, application_setting


def deliver_to_backend(event):
    base_url = application_setting("target.api.url").rstrip("/")
    credentials = api_credentials("target")
    request = authenticated_request(
        f"{base_url}/purchase-orders", credentials, method="POST", data=json_bytes(event["order"]),
        headers={"Content-Type": "application/json", "Idempotency-Key": event["event_id"]},
    )
    try:
        with urlopen(request, timeout=15) as response:
            result = json.load(response)
    except HTTPError as error:
        code = error.code
        error.close()
        if code in {408, 425, 429} or 500 <= code < 600:
            raise RuntimeError(f"Transient Target API HTTP {code}") from error
        raise PermanentBatchError(f"Target API rejected event {event['event_id']} with HTTP {code}") from error
    if not isinstance(result, dict) or result.get("order_number") != event["order_number"]:
        raise PermanentBatchError("Target API did not acknowledge the expected order")
    outcome = result.get("status")
    if outcome not in {"inserted", "updated", "duplicate", "stale"}:
        raise PermanentBatchError("Target API returned an unknown delivery result")
    return outcome

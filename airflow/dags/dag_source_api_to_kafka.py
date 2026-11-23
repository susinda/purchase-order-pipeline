"""DAG 1: Fetch complete purchase-order snapshots and publish one Kafka event per order.

Graph: read_source_api -> validate_purchase_orders -> publish_to_kafka -> both consumers

Manual parameter: load_type = initial | incremental (the Source API's actual values).
Small exercise dataset: the source batch is passed through XCom (size-limited).
For large production extracts, replace that XCom with durable object storage.
"""
import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, get_current_context, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from include.po_settings import get_setting, kafka_ca_file, required_setting

LOG = logging.getLogger("airflow.task")

with DAG(
    dag_id="dag_source_api_to_kafka",
    description="Fetch and validate purchase orders, publish Kafka events, and trigger both destinations",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={"load_type": Param("incremental", type="string", enum=["initial", "incremental"])},
    tags=["purchase-orders", "producer"],
) as dag:

    @task(retries=2, retry_delay=timedelta(seconds=20))
    def read_source_api():
        """Call actual repository API: GET /purchase-orders?load_type=initial|incremental."""
        import requests

        load_type = get_current_context()["params"]["load_type"]
        url = required_setting("SOURCE_API_URL").rstrip("/") + "/purchase-orders"
        auth = (required_setting("SOURCE_API_USERNAME"), required_setting("SOURCE_API_PASSWORD"))
        LOG.info("[1/3] GET Source API: %s?load_type=%s", url, load_type)
        started = time.monotonic()
        response = requests.get(url, params={"load_type": load_type}, auth=auth, timeout=45)
        LOG.info("Source HTTP=%s elapsed=%.2fs bytes=%s", response.status_code,
                 time.monotonic() - started, len(response.content))
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            orders = data
        elif isinstance(data, dict):
            orders = next((data[key] for key in ("purchased_orders", "purchase_orders", "orders", "PurchaseOrders", "data")
                           if isinstance(data.get(key), list)), None)
        else:
            orders = None
        if not isinstance(orders, list):
            raise ValueError("Unexpected Source API JSON; expected list or object containing order list")
        # XCom is suitable for the small supplied fixtures only, not large data transfers.
        serialized = json.dumps(orders, separators=(",", ":"))
        max_size = int(get_setting("PO_MAX_XCOM_BYTES", "1048576"))
        if len(serialized.encode("utf-8")) > max_size:
            raise ValueError("Extract exceeds safe XCom size. Stage in object storage instead.")
        LOG.info("Fetched %d purchase-order snapshots", len(orders))
        return {"load_type": load_type, "orders": orders}

    @task(do_xcom_push=False)
    def validate_purchase_orders(extract):
        """Check the fields used to identify and version orders and their children."""

        def require_key(record, field, path, *, allow_integer=False):
            if not isinstance(record, dict):
                raise ValueError(f"{path} must be an object")
            value = record.get(field)
            if isinstance(value, str) and value.strip():
                return
            if allow_integer and type(value) is int:
                return
            raise ValueError(f"{path}.{field} is missing or invalid")

        def children(record, field, path):
            rows = record.get(field)
            if rows is None:
                return []
            if not isinstance(rows, list):
                raise ValueError(f"{path}.{field} must be an array")
            return rows

        orders = extract["orders"]
        for order_index, order in enumerate(orders):
            order_path = f"orders[{order_index}]"
            require_key(order, "OrderNumber", order_path)
            require_key(order, "LastUpdated", order_path)
            for line_index, line in enumerate(children(order, "OrderLines", order_path)):
                line_path = f"{order_path}.OrderLines[{line_index}]"
                require_key(line, "LineNumber", line_path, allow_integer=True)
                for collection, key in (("CodingRows", "RowIndex"), ("GoodsReceipts", "GoodsReceiptNumber")):
                    for row_index, row in enumerate(children(line, collection, line_path)):
                        row_path = f"{line_path}.{collection}[{row_index}]"
                        require_key(row, key, row_path, allow_integer=True)

        LOG.info("[2/3] Validated key fields for %d purchase-order snapshots", len(orders))

    @task(retries=2, retry_delay=timedelta(seconds=20))
    def publish_to_kafka(extract):
        """Publish one event per order and confirm all Kafka broker acknowledgements."""
        from confluent_kafka import Producer

        context = get_current_context()
        run_id = context["run_id"]
        batch_id = "po-" + hashlib.sha256(run_id.encode()).hexdigest()[:20]
        settings = {
            "bootstrap.servers": required_setting("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": get_setting("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            "sasl.mechanism": get_setting("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
            "sasl.username": required_setting("KAFKA_PRODUCER_SASL_USERNAME"),
            "sasl.password": required_setting("KAFKA_PRODUCER_SASL_PASSWORD"),
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 30000,
        }
        settings["ssl.ca.location"] = kafka_ca_file()
        producer = Producer(settings)
        topic = get_setting("KAFKA_TOPIC", "purchase-orders")
        acknowledged = [0]
        errors = []
        orders = extract["orders"]
        LOG.info("[3/3] Publish %d purchase-order events to Kafka topic=%s run_id=%s batch_id=%s",
                 len(orders), topic, run_id, batch_id)

        def receipt(error, message):
            if error:
                errors.append(str(error))
                LOG.error("Kafka delivery rejected: %s", error)
            else:
                acknowledged[0] += 1
                LOG.info("ACK topic=%s batch_id=%s partition=%d offset=%d", message.topic(),
                         batch_id, message.partition(), message.offset())

        for index, order in enumerate(orders, 1):
            # The batch ID distinguishes separate runs. Identical snapshots in a retry keep their IDs.
            content = json.dumps(order, sort_keys=True, separators=(",", ":"))
            event_id = hashlib.sha256(f"{batch_id}:{content}".encode()).hexdigest()
            event = {
                "event_id": event_id,
                "batch_id": batch_id,
                "load_type": extract["load_type"],
                "order_number": str(order["OrderNumber"]),
                "last_updated": order.get("LastUpdated"),
                "payload": order,
            }
            LOG.info("Queue order %d/%d OrderNumber=%s batch_id=%s event_id=%s", index,
                     len(orders), order["OrderNumber"], batch_id, event_id)
            producer.produce(topic, key=str(order["OrderNumber"]).encode(),
                             value=json.dumps(event, separators=(",", ":")).encode(),
                             callback=receipt)
            producer.poll(0)

        pending = producer.flush(timeout=60)
        if pending or errors or acknowledged[0] != len(orders):
            raise RuntimeError(f"Kafka publish incomplete: acknowledged={acknowledged[0]} "
                               f"expected={len(orders)} pending={pending} errors={errors[:3]}")
        LOG.info("ALL %d events broker-acknowledged; downstream DAGs can start", acknowledged[0])
        # Only a small audit summary; consumer groups independently manage offsets.
        return {"batch_id": batch_id, "published_count": acknowledged[0],
                "load_type": extract["load_type"], "topic": topic}

    extracted = read_source_api()
    validated = validate_purchase_orders(extracted)
    published = publish_to_kafka(extracted)
    validated >> published
    # These independent group consumers need no manifest; they run in PARALLEL.
    target = TriggerDagRunOperator(
        task_id="trigger_target_dag",
        trigger_dag_id="dag_kafka_to_target_api",
        trigger_run_id="target__{{ ti.xcom_pull(task_ids='publish_to_kafka')['batch_id'] }}",
        wait_for_completion=False,
        skip_when_already_exists=True,
    )
    analytics = TriggerDagRunOperator(
        task_id="trigger_snowflake_dag",
        trigger_dag_id="dag_kafka_to_snowflake",
        trigger_run_id="analytics__{{ ti.xcom_pull(task_ids='publish_to_kafka')['batch_id'] }}",
        wait_for_completion=False,
        skip_when_already_exists=True,
    )
    published >> [target, analytics]

# ---------------------------------------------------------------------------
# Parse-time validation: Airflow must NOT register an empty/incomplete DAG.
# These checks run when Airflow imports this Python file, not when tasks run.
# ---------------------------------------------------------------------------
DAG_CODE_VERSION = "2026-11-23-purchase-order-validation"
EXPECTED_TASK_IDS = {
    "read_source_api",
    "validate_purchase_orders",
    "publish_to_kafka",
    "trigger_target_dag",
    "trigger_snowflake_dag",
}

actual_task_ids = set(dag.task_ids)
if actual_task_ids != EXPECTED_TASK_IDS:
    raise RuntimeError(
        f"{DAG_CODE_VERSION}: DAG {dag.dag_id!r} has incorrect tasks. "
        f"Expected={sorted(EXPECTED_TASK_IDS)}, actual={sorted(actual_task_ids)}. "
        "Check TaskFlow calls and deployment contents."
    )

EXPECTED_UPSTREAM = {
    "read_source_api": set(),
    "validate_purchase_orders": {"read_source_api"},
    "publish_to_kafka": {"read_source_api", "validate_purchase_orders"},
    "trigger_target_dag": {"publish_to_kafka"},
    "trigger_snowflake_dag": {"publish_to_kafka"},
}
for task_id, expected_upstream in EXPECTED_UPSTREAM.items():
    actual_upstream = set(dag.get_task(task_id).upstream_task_ids)
    if actual_upstream != expected_upstream:
        raise RuntimeError(
            f"{DAG_CODE_VERSION}: incorrect dependencies for {task_id!r}: "
            f"expected upstream={sorted(expected_upstream)}, "
            f"actual upstream={sorted(actual_upstream)}"
        )

logging.getLogger(__name__).warning(
    "DAG PARSE OK version=%s dag_id=%s tasks=%s",
    DAG_CODE_VERSION,
    dag.dag_id,
    sorted(dag.task_ids),
)

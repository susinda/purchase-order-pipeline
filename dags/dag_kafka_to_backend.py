from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
from airflow.sdk import DAG, get_current_context, task

from po_pipeline.backend import deliver_to_backend
from po_pipeline.events import PermanentBatchError
from po_pipeline.kafka_batch import consume_batch, destination_groups


with DAG(
    dag_id="dag_kafka_to_backend",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["purchase-orders"],
) as dag:
    @task(
        retries=3, retry_delay=timedelta(seconds=30), retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=20),
    )
    def deliver_batch():
        conf = get_current_context()["dag_run"].conf
        try:
            group, _ = destination_groups()
            return consume_batch(conf, group, deliver_to_backend, role="backend")
        except PermanentBatchError as error:
            raise AirflowFailException(str(error)) from error

    deliver_batch()

from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
from airflow.sdk import DAG, get_current_context, task

from po_pipeline.events import PermanentBatchError
from po_pipeline.warehouse import build_dbt_project, ingest_raw_batch, prepare_relational


with DAG(
    dag_id="dag_kafka_to_analytics",
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
    def ingest_raw():
        try:
            return ingest_raw_batch(get_current_context()["dag_run"].conf)
        except PermanentBatchError as error:
            raise AirflowFailException(str(error)) from error

    @task(retries=2, retry_delay=timedelta(seconds=30), execution_timeout=timedelta(minutes=10))
    def reconcile_relational():
        try:
            prepare_relational(get_current_context()["dag_run"].conf)
        except PermanentBatchError as error:
            raise AirflowFailException(str(error)) from error

    @task(execution_timeout=timedelta(minutes=20))
    def run_dbt():
        build_dbt_project()

    ingest_raw() >> reconcile_relational() >> run_dbt()

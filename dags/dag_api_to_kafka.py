from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, get_current_context, task

from po_pipeline.events import LOAD_TYPES, PUBLISHER_DAG_ID, PermanentBatchError
from po_pipeline.kafka_batch import publish_batch


with DAG(
    dag_id=PUBLISHER_DAG_ID,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    render_template_as_native_obj=True,
    params={"load_type": Param("incremental", type="string", enum=list(LOAD_TYPES))},
    tags=["purchase-orders"],
) as dag:
    @task(retries=2, retry_delay=timedelta(seconds=30), execution_timeout=timedelta(minutes=15))
    def publish_to_kafka():
        context = get_current_context()
        try:
            return publish_batch(context["params"]["load_type"], context["run_id"])
        except PermanentBatchError as error:
            raise AirflowFailException(str(error)) from error

    manifest = "{{ ti.xcom_pull(task_ids='publish_to_kafka') }}"
    run_id = "batch__{{ ti.xcom_pull(task_ids='publish_to_kafka')['batch_id'] }}"
    trigger_backend = TriggerDagRunOperator(
        task_id="trigger_backend",
        trigger_dag_id="dag_kafka_to_backend",
        trigger_run_id=run_id,
        conf=manifest,
        wait_for_completion=False,
        skip_when_already_exists=True,
        retries=2,
    )
    trigger_analytics = TriggerDagRunOperator(
        task_id="trigger_analytics",
        trigger_dag_id="dag_kafka_to_analytics",
        trigger_run_id=run_id,
        conf=manifest,
        wait_for_completion=False,
        skip_when_already_exists=True,
        retries=2,
    )
    publish_to_kafka = publish_to_kafka()
    publish_to_kafka >> [trigger_backend, trigger_analytics]

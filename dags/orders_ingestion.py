"""Run one explicitly selected synthetic order batch."""

import logging
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, get_current_context, task
from scripts.failure_notification import notify_task_failure


@dag(
    dag_id="orders_ingestion",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": notify_task_failure,
        "email_on_failure": False,
        "email_on_retry": False,
    },
)
def orders_ingestion():
    @task
    def extract_batch():
        from scripts.generate_orders import DATA
        from scripts.validate_batch import BatchValidationError, batch_fingerprint, extract_batch_files

        batch_id = (get_current_context()["dag_run"].conf or {}).get("batch_id")
        if not isinstance(batch_id, str):
            raise AirflowFailException("DAG run configuration requires a string batch_id")
        try:
            manifest, _ = extract_batch_files(batch_id, DATA / "incoming")
        except BatchValidationError as error:
            raise AirflowFailException(str(error)) from None
        summary = {"batch_id": batch_id, "fingerprint": batch_fingerprint(manifest),
                   "order_count": manifest["expected_row_counts"]["raw_orders.csv"],
                   "item_count": manifest["expected_row_counts"]["raw_items.csv"]}
        logging.info("Extracted batch %s: expected %s orders, %s items", batch_id,
                     summary["order_count"], summary["item_count"])
        return summary

    @task
    def validate_batch(batch):
        from scripts.validate_batch import BatchDatabaseError, BatchValidationError, validate_batch_summary

        try:
            summary = validate_batch_summary(batch["batch_id"], expected_fingerprint=batch["fingerprint"])
        except BatchDatabaseError as error:
            raise RuntimeError(str(error)) from None
        except BatchValidationError as error:
            raise AirflowFailException(str(error)) from None
        logging.info("Validated batch %s: %s orders, %s items",
                     summary["batch_id"], summary["order_count"], summary["item_count"])
        return summary

    @task
    def load_batch(batch):
        from scripts.load_batch import BatchLoadDatabaseError, BatchLoadError, load_batch as load
        from scripts.validate_batch import BatchValidationError

        try:
            result = load(batch["batch_id"], expected_fingerprint=batch["fingerprint"],
                          airflow_run_id=get_current_context()["run_id"])
        except BatchLoadDatabaseError as error:
            raise RuntimeError(str(error)) from None
        except (BatchLoadError, BatchValidationError) as error:
            raise AirflowFailException(str(error)) from None
        logging.info("Batch %s: %s, inserted %s orders and %s items",
                     result["batch_id"], result["status"],
                     result["orders_inserted"], result["items_inserted"])
        return result

    load_batch(validate_batch(extract_batch()))


orders_ingestion()

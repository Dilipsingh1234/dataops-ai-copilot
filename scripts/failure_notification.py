"""Send a safe local notification for an Airflow task failure."""

import logging
import os
import re
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage


def notify_task_failure(context):
    """Send one notification when Airflow calls the task failure callback."""
    try:
        task = context["ti"]
        dag_run = context["dag_run"]
        batch_id = (dag_run.conf or {}).get("batch_id")
        if not isinstance(batch_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", batch_id):
            batch_id = "unavailable"
        summaries = {
            "extract_batch": "Batch extraction failed. Check the task log for details.",
            "validate_batch": "Batch validation failed. Check the task log for details.",
            "load_batch": "Batch loading failed. Check the task log for details.",
        }
        summary = summaries.get(task.task_id, "Task failed. Check the task log for details.")
        message = EmailMessage()
        message["From"] = os.getenv("FAILURE_EMAIL_FROM", "airflow@example.invalid")
        message["To"] = os.getenv("FAILURE_EMAIL_TO", "developer@example.invalid")
        message["Subject"] = "Airflow task failed"
        message.set_content(
            f"DAG ID: {task.dag_id}\n"
            f"Run ID: {task.run_id}\n"
            f"Task ID: {task.task_id}\n"
            f"Batch ID: {batch_id}\n"
            f"Failure timestamp (UTC): {datetime.now(timezone.utc).isoformat()}\n"
            f"Status: failed\n"
            f"Summary: {summary}\n"
        )
        with smtplib.SMTP("mailpit", 1025, timeout=10) as smtp:
            smtp.send_message(message)
    except (OSError, smtplib.SMTPException, ValueError, KeyError, AttributeError, TypeError):
        # Avoid logging SMTP responses or context, which can contain sensitive values.
        logging.error("Unable to send Airflow task failure notification.")

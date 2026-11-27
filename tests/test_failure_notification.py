import logging
import smtplib
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts.failure_notification import notify_task_failure


class FailureNotificationTest(unittest.TestCase):
    def setUp(self):
        self.context = {
            "ti": SimpleNamespace(dag_id="orders_ingestion", run_id="run-123", task_id="validate_batch"),
            "dag_run": SimpleNamespace(conf={"batch_id": "synthetic-123"}),
            "exception": RuntimeError("postgresql://user:secret@db/private raw SQL"),
        }

    @patch("scripts.failure_notification.smtplib.SMTP")
    @patch.dict("os.environ", {"FAILURE_EMAIL_FROM": "sender@example.invalid",
                               "FAILURE_EMAIL_TO": "recipient@example.invalid"})
    def test_message_contains_safe_context(self, smtp):
        notify_task_failure(self.context)
        smtp.assert_called_once_with("mailpit", 1025, timeout=10)
        message = smtp.return_value.__enter__.return_value.send_message.call_args.args[0]
        self.assertEqual(message["From"], "sender@example.invalid")
        self.assertEqual(message["To"], "recipient@example.invalid")
        body = message.get_content()
        for expected in ("orders_ingestion", "run-123", "validate_batch", "synthetic-123",
                         "Failure timestamp (UTC):", "Status: failed", "Batch validation failed"):
            self.assertIn(expected, body)
        self.assertNotIn("secret", body)
        self.assertNotIn("raw SQL", body)

    @patch("scripts.failure_notification.smtplib.SMTP")
    def test_invalid_batch_is_not_echoed(self, smtp):
        self.context["dag_run"].conf["batch_id"] = "secret\nunsafe"
        notify_task_failure(self.context)
        body = smtp.return_value.__enter__.return_value.send_message.call_args.args[0].get_content()
        self.assertIn("Batch ID: unavailable", body)
        self.assertNotIn("secret", body)

    @patch("scripts.failure_notification.smtplib.SMTP", side_effect=smtplib.SMTPException("secret"))
    def test_smtp_failure_is_safe_and_does_not_raise(self, smtp):
        with self.assertLogs(level=logging.ERROR) as captured:
            self.assertIsNone(notify_task_failure(self.context))
        self.assertNotIn("secret", " ".join(captured.output))
        smtp.assert_called_once()


if __name__ == "__main__":
    unittest.main()

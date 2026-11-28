import unittest
from unittest.mock import MagicMock, patch

from backend.airflow_client import AirflowClientError
from backend.pipeline_tools import get_pipeline_failure


class PipelineToolsTest(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("backend.pipeline_tools.AirflowClient")
        factory = self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.client = factory.return_value.__enter__.return_value
        self.client._sanitize.side_effect = lambda value: value
        self.client.get_dag_run.return_value = {"dag_run_id": "run-1", "state": "failed"}
        self.client.get_latest_failed_run.return_value = self.client.get_dag_run.return_value
        self.client.get_task_instances.return_value = [
            {"task_id": "validate_batch", "state": "failed", "try_number": 1},
            {"task_id": "load_batch", "state": "upstream_failed", "try_number": 0},
        ]
        self.client.get_task_log.return_value = {"log_excerpt": "invalid subtotal", "truncated": False}

    def test_exact_run_and_upstream_state(self):
        result = get_pipeline_failure("orders_ingestion", "run-1")
        self.assertEqual(result["evidence_completeness"], "complete")
        self.assertEqual(result["failed_tasks"][0]["attempts"][0]["log_excerpt"], "invalid subtotal")
        self.assertEqual(len(result["failed_tasks"]), 1)
        self.assertEqual(result["task_states"][1]["task_state"], "upstream_failed")
        self.client.get_dag_run.assert_called_once_with("orders_ingestion", "run-1")

    def test_latest_and_no_failed_run(self):
        self.assertEqual(get_pipeline_failure("orders_ingestion")["run_id"], "run-1")
        self.client.get_latest_failed_run.return_value = None
        self.assertEqual(get_pipeline_failure("orders_ingestion")["status"], "not_found")

    def test_unavailable_and_pagination_errors_are_safe(self):
        self.client.get_latest_failed_run.side_effect = AirflowClientError("secret pagination limit")
        result = get_pipeline_failure("orders_ingestion")
        self.assertEqual(result["status"], "error")
        self.assertNotIn("secret", str(result))

    def test_unavailable_log_is_partial(self):
        self.client.get_task_log.side_effect = AirflowClientError("secret")
        result = get_pipeline_failure("orders_ingestion", "run-1")
        self.assertEqual(result["evidence_completeness"], "partial")
        self.assertNotIn("secret", str(result))

    def test_limits_and_attempts(self):
        self.client.get_task_instances.return_value = [
            {"task_id": str(i), "state": "failed", "try_number": 3} for i in range(4)
        ]
        self.client.get_task_log.return_value = {"log_excerpt": "x" * 13000, "truncated": False}
        result = get_pipeline_failure("orders_ingestion", "run-1")
        self.assertEqual(result["evidence_completeness"], "partial")
        self.assertEqual(len(result["failed_tasks"]), 3)
        self.assertEqual(len(result["failed_tasks"][0]["attempts"][0]["log_excerpt"]), 12000)
        self.assertTrue(result["failed_tasks"][0]["attempts"][0]["truncated"])

    def test_rejects_other_dag_and_wrong_run(self):
        self.assertEqual(get_pipeline_failure("other")["status"], "error")
        self.assertEqual(get_pipeline_failure("orders_ingestion", "wrong")["status"], "error")


if __name__ == "__main__":
    unittest.main()

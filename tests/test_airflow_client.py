import unittest
from unittest.mock import patch

import httpx

from backend.airflow_client import AirflowClient, AirflowClientError


class AirflowClientTest(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict("os.environ", {
            "AIRFLOW_API_USERNAME": "viewer",
            "AIRFLOW_API_PASSWORD": "test-password",
            "POSTGRES_PASSWORD": "database-secret",
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def client(self, handler):
        client = AirflowClient(transport=httpx.MockTransport(handler))
        self.addCleanup(client.close)
        return client

    def test_refresh_once_and_encode_identifiers(self):
        calls = []
        def handler(request):
            calls.append(request)
            if request.url.path == "/auth/token":
                return httpx.Response(201, json={"access_token": f"token-{len(calls)}"})
            if len(calls) == 2:
                return httpx.Response(401)
            return httpx.Response(200, json={"state": "failed"})
        client = self.client(handler)
        self.assertEqual(client.get_dag_run("dag space", "run/id")["state"], "failed")
        self.assertEqual(len(calls), 4)
        self.assertIn(b"dag%20space", calls[-1].url.raw_path)
        self.assertIn(b"run%2Fid", calls[-1].url.raw_path)

    def test_pagination_and_latest_timestamp(self):
        def handler(request):
            if request.url.path == "/auth/token":
                return httpx.Response(201, json={"access_token": "test-token"})
            if request.url.params.get("cursor"):
                return httpx.Response(200, json={"dag_runs": [
                    {"state": "failed", "dag_run_id": "later", "start_date": "2026-01-01T03:00:00+00:00"}],
                    "next_cursor": None})
            return httpx.Response(200, json={"dag_runs": [
                {"state": "failed", "dag_run_id": "earlier", "start_date": "2026-01-01T04:00:00+02:00"}],
                "next_cursor": "page-two"})
        self.assertEqual(self.client(handler).get_latest_failed_run("orders")["dag_run_id"], "later")

    def test_pagination_limit_fails_without_returning_latest(self):
        def handler(request):
            if request.url.path == "/auth/token":
                return httpx.Response(201, json={"access_token": "test-token"})
            cursor = int(request.url.params.get("cursor", "0"))
            return httpx.Response(200, json={"dag_runs": [], "next_cursor": str(cursor + 1)})
        with self.assertRaisesRegex(AirflowClientError, "pagination limit"):
            self.client(handler).get_latest_failed_run("orders")

    def test_log_structured_exception_redaction_and_limit(self):
        def handler(request):
            if request.url.path == "/auth/token":
                return httpx.Response(201, json={"access_token": "test-token"})
            return httpx.Response(200, json={"content": [
                {"event": "failure database-secret", "error_detail": [{"exc_value": "invalid test-password"}]}],
                "continuation_token": None})
        result = self.client(handler).get_task_log("dag", "run", "task", 1)
        self.assertEqual(result["log_excerpt"], "failure [REDACTED]\ninvalid [REDACTED]")
        self.assertFalse(result["truncated"])
        self.assertTrue(self.client(handler).get_task_log("dag", "run", "task", 1, max_chars=5)["truncated"])
        with self.assertRaisesRegex(AirflowClientError, "download limit"):
            self.client(handler).get_task_log("dag", "run", "task", 1, max_bytes=40)

    def test_statuses_and_malformed_response_are_safe(self):
        for status, message in [(401, "authentication failed"), (403, "access denied"),
                                (404, "resource not found"), (500, "request failed")]:
            with self.subTest(status=status):
                def handler(request):
                    if request.url.path == "/auth/token":
                        return httpx.Response(201, json={"access_token": "test-token"})
                    return httpx.Response(status, text="test-password database-secret")
                with self.assertRaises(AirflowClientError) as error:
                    self.client(handler).get_dag_run("dag", "run")
                self.assertIn(message, str(error.exception))
                self.assertNotIn("secret", str(error.exception))
        def malformed(request):
            return httpx.Response(201, json={"access_token": "token"}) if request.url.path == "/auth/token" else httpx.Response(200, content=b"{")
        with self.assertRaisesRegex(AirflowClientError, "malformed"):
            self.client(malformed).get_dag_run("dag", "run")

    def test_timeout_and_network_errors(self):
        for exc, expected in ((httpx.ReadTimeout("secret"), "timed out"),
                              (httpx.ConnectError("secret"), "unavailable")):
            def handler(request):
                raise exc
            with self.assertRaises(AirflowClientError) as error:
                self.client(handler).list_dag_runs("dag")
            self.assertIn(expected, str(error.exception))
            self.assertNotIn("secret", str(error.exception))


if __name__ == "__main__":
    unittest.main()

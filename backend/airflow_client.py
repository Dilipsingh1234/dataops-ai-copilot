"""Read-only access to Airflow's public REST API."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import httpx
from dotenv import dotenv_values


class AirflowClientError(Exception):
    """A safe error suitable for a caller or tool response."""


class AirflowClient:
    def __init__(self, *, transport=None):
        settings = dict(dotenv_values(Path(__file__).resolve().parents[1] / ".env"))
        settings.update(os.environ)
        self._url = (settings.get("AIRFLOW_API_URL") or "http://127.0.0.1:8080").rstrip("/")
        self._username = settings.get("AIRFLOW_API_USERNAME")
        self._password = settings.get("AIRFLOW_API_PASSWORD")
        if not self._username or not self._password:
            raise AirflowClientError("Airflow viewer credentials are missing")
        self._secrets = {value for key, value in settings.items()
                         if value and any(word in key.upper() for word in ("PASSWORD", "SECRET", "TOKEN", "FERNET", "API_KEY"))}
        self._token = None
        self._http = httpx.Client(
            base_url=self._url, timeout=httpx.Timeout(10.0, connect=3.0),
            transport=transport, follow_redirects=False, trust_env=False,
        )

    def close(self):
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _sanitize(self, value):
        if isinstance(value, str):
            for secret in sorted(self._secrets, key=len, reverse=True):
                value = value.replace(secret, "[REDACTED]")
            return value
        if isinstance(value, list):
            return [self._sanitize(item) for item in value]
        if isinstance(value, dict):
            return {key: self._sanitize(item) for key, item in value.items()}
        return value

    def _request(self, method, path, *, params=None, payload=None, limit=1024 * 1024):
        try:
            with self._http.stream(method, path, params=params, json=payload,
                                   headers={"Authorization": f"Bearer {self._token}"} if method == "GET" else {}) as response:
                status = response.status_code
                if status != 200 and status != 201:
                    return status, None
                data = bytearray()
                for chunk in response.iter_bytes(chunk_size=min(8192, limit + 1)):
                    if len(data) + len(chunk) > limit:
                        raise AirflowClientError("Airflow response exceeds the download limit")
                    data.extend(chunk)
            try:
                parsed = json.loads(data)
            except (ValueError, UnicodeDecodeError):
                raise AirflowClientError("Airflow returned a malformed response") from None
            if not isinstance(parsed, dict):
                raise AirflowClientError("Airflow returned a malformed response")
            return status, parsed
        except httpx.TimeoutException:
            raise AirflowClientError("Airflow request timed out") from None
        except httpx.HTTPError:
            raise AirflowClientError("Airflow service is unavailable") from None

    def _authenticate(self):
        status, result = self._request("POST", "/auth/token",
                                       payload={"username": self._username, "password": self._password})
        if status in (401, 403):
            raise AirflowClientError("Airflow authentication failed")
        if status not in (200, 201) or not isinstance(result.get("access_token"), str) or not result["access_token"]:
            raise AirflowClientError("Airflow authentication failed")
        self._token = result["access_token"]
        self._secrets.add(self._token)

    def _get(self, path, *, params=None, limit=1024 * 1024):
        if self._token is None:
            self._authenticate()
        status, data = self._request("GET", path, params=params, limit=limit)
        if status == 401:
            self._token = None
            self._authenticate()
            status, data = self._request("GET", path, params=params, limit=limit)
        if status != 200:
            errors = {401: "Airflow authentication failed", 403: "Airflow access denied",
                      404: "Airflow resource not found"}
            raise AirflowClientError(errors.get(status, "Airflow request failed"))
        return data

    @staticmethod
    def _part(value):
        if not isinstance(value, str) or not value or value in (".", ".."):
            raise AirflowClientError("Invalid Airflow identifier")
        return quote(value, safe="")

    def _pages(self, path, collection, params=None):
        results = []
        cursor = None
        seen = set()
        while True:
            if len(seen) >= 100:
                raise AirflowClientError("Airflow pagination limit exceeded")
            query = dict(params or {}, limit=100)
            if cursor:
                query["cursor"] = cursor
            page = self._get(path, params=query)
            rows = page.get(collection)
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise AirflowClientError("Airflow returned a malformed response")
            results.extend(rows)
            cursor = page.get("next_cursor")
            if cursor is None:
                return self._sanitize(results)
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                raise AirflowClientError("Airflow returned invalid pagination")
            seen.add(cursor)

    def list_dag_runs(self, dag_id):
        return self._pages(f"/api/v2/dags/{self._part(dag_id)}/dagRuns", "dag_runs")

    def get_dag_run(self, dag_id, run_id):
        return self._sanitize(self._get(f"/api/v2/dags/{self._part(dag_id)}/dagRuns/{self._part(run_id)}"))

    def get_task_instances(self, dag_id, run_id):
        path = f"/api/v2/dags/{self._part(dag_id)}/dagRuns/{self._part(run_id)}/taskInstances"
        return self._pages(path, "task_instances")

    def get_latest_failed_run(self, dag_id):
        runs = [run for run in self.list_dag_runs(dag_id) if run.get("state") == "failed"]
        if not runs:
            return None
        if any(not isinstance(run.get("start_date"), str) or not isinstance(run.get("dag_run_id"), str) for run in runs):
            raise AirflowClientError("Airflow returned a malformed run")
        try:
            def order(run):
                timestamp = datetime.fromisoformat(run["start_date"].replace("Z", "+00:00"))
                if timestamp.tzinfo is None:
                    raise ValueError
                return timestamp.astimezone(timezone.utc), run["dag_run_id"]

            return max(runs, key=order)
        except ValueError:
            raise AirflowClientError("Airflow returned a malformed run") from None

    def get_task_log(self, dag_id, run_id, task_id, try_number, *, max_bytes=262144, max_chars=12000):
        if type(try_number) is not int or try_number < 1 or max_bytes < 1 or max_chars < 1:
            raise AirflowClientError("Invalid log limit or attempt number")
        path = (f"/api/v2/dags/{self._part(dag_id)}/dagRuns/{self._part(run_id)}"
                f"/taskInstances/{self._part(task_id)}/logs/{try_number}")
        # A full response is bounded before JSON parsing, so oversized JSON fails safely.
        data = self._get(path, params={"full_content": "true"}, limit=max_bytes)
        entries = data.get("content")
        if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
            raise AirflowClientError("Airflow returned a malformed log")
        lines = []
        for entry in entries:
            if isinstance(entry.get("event"), str):
                lines.append(entry["event"])
            details = entry.get("error_detail", [])
            if not isinstance(details, list):
                raise AirflowClientError("Airflow returned a malformed log")
            for detail in details:
                if not isinstance(detail, dict):
                    raise AirflowClientError("Airflow returned a malformed log")
                if isinstance(detail.get("exc_value"), str):
                    lines.append(detail["exc_value"])
        content = self._sanitize("\n".join(lines))
        return self._sanitize({"dag_id": dag_id, "run_id": run_id, "task_id": task_id,
                               "try_number": try_number, "log_excerpt": content[:max_chars],
                               "truncated": len(content) > max_chars or bool(data.get("continuation_token"))})

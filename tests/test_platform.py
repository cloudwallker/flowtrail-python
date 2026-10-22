"""Behavioral tests use a real SQLite database and the ASGI application."""
from __future__ import annotations

import concurrent.futures
import json
import tempfile
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

try:
    from flowtrail.app import create_app
    from flowtrail.config import Settings
    from flowtrail.store import Store
except ModuleNotFoundError:
    create_app = Settings = Store = None


class ContractExists(unittest.TestCase):
    def test_application_contract_exists(self):
        self.assertIsNotNone(create_app, "The Python platform application is not implemented")


@unittest.skipIf(create_app is None, "Application implementation pending")
class PlatformTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(db_path=Path(self.tmp.name) / "test.sqlite3", quota_bytes=64,
                                 max_object_bytes=64, poll_seconds=0.01)
        self.store = Store(self.settings)
        self.a = self.store.issue_token("alpha", "admin")
        self.b = self.store.issue_token("beta", "operator")
        self.viewer = self.store.issue_token("alpha", "viewer")
        self.client_context = TestClient(create_app(self.settings))
        self.client = self.client_context.__enter__()

    def tearDown(self):
        self.client_context.__exit__(None, None, None)
        self.tmp.cleanup()

    def headers(self, token=None, **extra):
        return {"Authorization": "Bearer " + (token or self.a), **extra}

    def workflow(self, nodes=None, name="demo"):
        return {"workflow": {"name": name, "nodes": nodes or [
            {"id": "first", "type": "text", "depends_on": [],
             "config": {"template": "hello ${inputs.name}"}},
            {"id": "second", "type": "inference", "depends_on": ["first"],
             "config": {"prompt": "${first.output}", "mode": "mock"}},
        ]}, "inputs": {"name": "world"}}

    def create(self, body=None, token=None, **headers):
        response = self.client.post("/api/runs", json=body or self.workflow(),
                                    headers=self.headers(token, **headers))
        self.assertEqual(response.status_code, 202, response.text)
        return response.json()

    def wait_run(self, run_id, status=None, timeout=3):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            result = self.client.get(f"/api/runs/{run_id}", headers=self.headers()).json()
            if result["status"] in (status or {"succeeded", "failed", "interrupted"}):
                return result
            time.sleep(0.01)
        self.fail(f"Run did not settle: {result}")

    def test_authentication_and_client_cannot_choose_tenant(self):
        self.assertEqual(self.client.get("/api/me").status_code, 401)
        self.assertEqual(self.client.get("/api/me", headers=self.headers("bogus")).status_code, 401)
        self.assertEqual(self.client.get("/api/me", headers=self.headers()).json(),
                         {"tenant_id": "alpha", "role": "admin"})
        body = self.workflow()
        body["tenant_id"] = "beta"
        self.assertEqual(self.client.post("/api/runs", json=body, headers=self.headers()).status_code, 422)

    def test_dag_execution_and_mock_inference(self):
        run = self.wait_run(self.create()["id"])
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(run["nodes"][0]["output"], "hello world")
        self.assertEqual(run["nodes"][1]["output"], "[mock] hello world")
        self.assertEqual([n["attempt"] for n in run["nodes"]], [1, 1])

    def test_tenant_isolation_in_runs_events_and_objects(self):
        run = self.create()
        for suffix in ("", "/events?follow=false"):
            response = self.client.get(f"/api/runs/{run['id']}{suffix}", headers=self.headers(self.b))
            self.assertEqual(response.status_code, 404)
        self.assertEqual(self.client.get("/api/runs", headers=self.headers(self.b)).json()["items"], [])
        self.client.put("/api/objects/note.txt", content=b"private", headers=self.headers())
        self.assertEqual(self.client.get("/api/objects/note.txt", headers=self.headers(self.b)).status_code, 404)
        self.assertEqual(self.client.get("/api/objects", headers=self.headers(self.b)).json()["items"], [])
        self.assertEqual(self.client.delete("/api/objects/note.txt", headers=self.headers(self.b)).status_code, 404)

    def test_viewer_can_read_but_cannot_mutate(self):
        self.assertEqual(self.client.get("/api/runs", headers=self.headers(self.viewer)).status_code, 200)
        self.assertEqual(self.client.post("/api/runs", json=self.workflow(), headers=self.headers(self.viewer)).status_code, 403)
        self.assertEqual(self.client.put("/api/objects/x", content=b"x", headers=self.headers(self.viewer)).status_code, 403)
        self.assertEqual(self.client.delete("/api/objects/x", headers=self.headers(self.viewer)).status_code, 403)
        run = self.create()
        self.assertEqual(self.client.post(f"/api/runs/{run['id']}/resume", headers=self.headers(self.viewer)).status_code, 403)

    def test_object_quota_replace_delete_and_content_hash(self):
        response = self.client.put("/api/objects/a", content=b"a" * 40, headers=self.headers())
        self.assertEqual(response.status_code, 201)
        self.assertEqual(len(response.json()["sha256"]), 64)
        self.assertEqual(self.client.put("/api/objects/b", content=b"b" * 25, headers=self.headers()).status_code, 413)
        self.assertEqual(self.client.get("/api/quota", headers=self.headers()).json()["used_bytes"], 40)
        self.assertEqual(self.client.put("/api/objects/a", content=b"x" * 5, headers=self.headers()).status_code, 201)
        self.assertEqual(self.client.get("/api/objects/a", headers=self.headers()).content, b"x" * 5)
        self.assertEqual(self.client.delete("/api/objects/a", headers=self.headers()).status_code, 204)
        self.assertEqual(self.client.get("/api/quota", headers=self.headers()).json()["used_bytes"], 0)

    def test_concurrent_object_writes_cannot_exceed_quota(self):
        def put(name):
            return self.client.put(f"/api/objects/{name}", content=b"x" * 40, headers=self.headers()).status_code
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            statuses = list(executor.map(put, ["a", "b"]))
        self.assertEqual(sorted(statuses), [201, 413])
        self.assertEqual(self.client.get("/api/quota", headers=self.headers()).json()["used_bytes"], 40)

    def test_reject_cycle_missing_dependency_and_nonancestor_reference(self):
        invalid = [
            [{"id": "a", "type": "text", "depends_on": ["a"], "config": {"template": "x"}}],
            [{"id": "a", "type": "text", "depends_on": ["missing"], "config": {"template": "x"}}],
            [{"id": "a", "type": "text", "config": {"template": "a"}},
             {"id": "b", "type": "text", "config": {"template": "${a.output}"}}],
        ]
        for nodes in invalid:
            with self.subTest(nodes=nodes):
                self.assertEqual(self.client.post("/api/runs", json=self.workflow(nodes), headers=self.headers()).status_code, 422)

    def test_no_arbitrary_http_url_and_missing_connector_rejected(self):
        for config in ({"url": "http://169.254.169.254/"}, {"connector": "unknown"}):
            nodes = [{"id": "x", "type": "http", "config": config}]
            self.assertEqual(self.client.post("/api/runs", json=self.workflow(nodes), headers=self.headers()).status_code, 422)

    def test_idempotency_is_tenant_scoped_and_conflicts_on_changed_input(self):
        first = self.create(**{"Idempotency-Key": "same"})
        again = self.create(**{"Idempotency-Key": "same"})
        self.assertEqual(first["id"], again["id"])
        other = self.create(token=self.b, **{"Idempotency-Key": "same"})
        self.assertNotEqual(first["id"], other["id"])
        body = self.workflow()
        body["inputs"]["name"] = "changed"
        self.assertEqual(self.client.post("/api/runs", json=body, headers=self.headers(**{"Idempotency-Key": "same"})).status_code, 409)

    def test_sse_replay_respects_cursor_and_contains_terminal_event(self):
        run = self.wait_run(self.create()["id"])
        response = self.client.get(f"/api/runs/{run['id']}/events?follow=false", headers=self.headers())
        events = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")]
        self.assertEqual(events[-1]["type"], "RUN_SUCCEEDED")
        cursor = events[1]["seq"]
        response = self.client.get(f"/api/runs/{run['id']}/events?follow=false", headers=self.headers(**{"Last-Event-ID": str(cursor)}))
        newer = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")]
        self.assertTrue(newer)
        self.assertTrue(all(item["seq"] > cursor for item in newer))

    def test_token_persisted_only_as_hash(self):
        data = self.settings.db_path.read_bytes()
        self.assertNotIn(self.a.encode(), data)

    def test_malformed_config_types_are_rejected_without_input_echo(self):
        for value in ([], {}, 10, True):
            body = self.workflow([{"id": "x", "type": "inference", "config": {"prompt": "safe", "mode": value}}])
            response = self.client.post("/api/runs", json=body, headers=self.headers())
            self.assertEqual(response.status_code, 422)
        response = self.client.post("/api/runs", json={"private": "DO_NOT_ECHO_THIS_INPUT"}, headers=self.headers())
        self.assertEqual(response.status_code, 422)
        self.assertNotIn("DO_NOT_ECHO_THIS_INPUT", response.text)

    def test_invalid_unicode_is_rejected_before_database_write(self):
        body = self.workflow()
        body["workflow"]["name"] = "\ud800"
        response = self.client.post("/api/runs", content=json.dumps(body).encode(),
                                    headers=self.headers(**{"Content-Type": "application/json"}))
        self.assertEqual(response.status_code, 422)

    def test_request_limits_and_invalid_cursor(self):
        self.assertEqual(self.client.put("/api/objects/large", content=b"x" * 65, headers=self.headers()).status_code, 413)
        self.assertEqual(self.client.post("/api/runs", content=b"x" * (self.settings.max_request_bytes + 1), headers=self.headers()).status_code, 413)
        run = self.create()
        self.assertEqual(self.client.get(f"/api/runs/{run['id']}/events?follow=false", headers=self.headers(**{"Last-Event-ID": "-1"})).status_code, 422)
        self.assertEqual(self.client.get("/api/me?tenant_id=beta", headers=self.headers()).status_code, 422)

    def test_template_values_are_not_recursively_interpreted(self):
        body = self.workflow()
        body["inputs"]["name"] = "${inputs.missing}"
        run = self.wait_run(self.create(body)["id"])
        self.assertEqual(run["nodes"][0]["output"], "hello ${inputs.missing}")

    def test_second_runtime_cannot_own_same_database(self):
        with self.assertRaisesRegex(RuntimeError, "already owns"):
            with TestClient(create_app(self.settings)):
                pass

    def test_text_and_inference_reject_placeholder_amplification(self):
        for node_type, field in (("text", "template"), ("inference", "prompt")):
            body = self.workflow([{"id": "large", "type": node_type,
                                   "config": {field: "${inputs.payload}" * 200}}])
            body["inputs"] = {"payload": "x" * 32768}
            run = self.wait_run(self.create(body)["id"])
            self.assertEqual(run["status"], "failed")
            self.assertIn("byte limit", run["nodes"][0]["error"])


if __name__ == "__main__":
    unittest.main()

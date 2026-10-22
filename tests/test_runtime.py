from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from collections import Counter
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fastapi.testclient import TestClient

from flowtrail.app import create_app
from flowtrail.config import Settings
from flowtrail.models import SubmitRun
from flowtrail.store import Store, StoreError


class Upstream(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, status, data, headers=None):
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
        except (ConnectionError, OSError):
            pass

    def do_GET(self):
        self.server.calls[self.path] += 1
        if self.path == "/fail" or (self.path == "/switch" and not self.server.healthy):
            return self.respond(503, b"UPSTREAM_PRIVATE_DETAIL")
        if self.path == "/redirect":
            return self.respond(302, b"", {"Location": self.server.base + "/ok"})
        if self.path == "/slow":
            time.sleep(0.3)
        if self.path == "/large":
            return self.respond(200, b"x" * 300_000)
        if self.path == "/parallel":
            with self.server.guard:
                self.server.active += 1
                self.server.max_active = max(self.server.max_active, self.server.active)
            time.sleep(0.2)
            with self.server.guard:
                self.server.active -= 1
        return self.respond(200, b'{"healthy":true}')

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.model_request = body
        return self.respond(200, json.dumps({"choices": [{"message": {"content": "model adapter response"}}]}).encode())


class RuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        cls.upstream.base = f"http://127.0.0.1:{cls.upstream.server_port}"
        cls.upstream.guard = threading.Lock()
        cls.thread = threading.Thread(target=cls.upstream.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.upstream.shutdown()
        cls.upstream.server_close()
        cls.thread.join(timeout=2)

    def setUp(self):
        self.upstream.calls = Counter()
        self.upstream.healthy = False
        self.upstream.active = self.upstream.max_active = 0
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(db_path=Path(self.tmp.name) / "test.sqlite3", poll_seconds=0.01,
            connectors={name: {"url": self.upstream.base + "/" + name}
                        for name in ("ok", "fail", "slow", "switch", "redirect", "large", "parallel")},
            model_base_url=self.upstream.base + "/v1", model_name="local-protocol-fixture", model_api_key="test-value")
        self.store = Store(self.settings)
        self.token = self.store.issue_token("demo", "operator")
        self.headers = {"Authorization": "Bearer " + self.token}
        self.open_client()

    def open_client(self):
        self.context = TestClient(create_app(self.settings))
        self.client = self.context.__enter__()

    def close_client(self):
        self.context.__exit__(None, None, None)

    def tearDown(self):
        self.close_client()
        self.tmp.cleanup()

    def create(self, nodes):
        response = self.client.post("/api/runs", json={"workflow": {"name": "runtime", "nodes": nodes}, "inputs": {}}, headers=self.headers)
        self.assertEqual(response.status_code, 202, response.text)
        return response.json()["id"]

    def get(self, run_id):
        return self.client.get(f"/api/runs/{run_id}", headers=self.headers).json()

    def wait(self, run_id, predicate=None):
        end = time.monotonic() + 5
        while time.monotonic() < end:
            run = self.get(run_id)
            if (predicate(run) if predicate else run["status"] in {"succeeded", "failed", "interrupted"}):
                return run
            time.sleep(0.01)
        self.fail(f"Run did not reach expected state: {run}")

    @staticmethod
    def http(node_id, connector, deps=None, timeout=2):
        return {"id": node_id, "type": "http", "depends_on": deps or [],
                "config": {"connector": connector, "timeout_seconds": timeout}}

    def test_failed_dependency_never_executes_and_errors_do_not_leak_body(self):
        run = self.wait(self.create([self.http("first", "fail"), self.http("later", "ok", ["first"])]))
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["nodes"][1]["status"], "skipped")
        self.assertEqual(self.upstream.calls["/ok"], 0)
        self.assertNotIn("UPSTREAM_PRIVATE_DETAIL", json.dumps(run))

    def test_failed_resume_keeps_success_checkpoint_and_runs_downstream(self):
        run_id = self.create([self.http("first", "ok"), self.http("second", "switch", ["first"]),
                              {"id": "last", "type": "text", "depends_on": ["second"], "config": {"template": "${second.output}"}}])
        self.assertEqual(self.wait(run_id)["status"], "failed")
        self.upstream.healthy = True
        self.assertEqual(self.client.post(f"/api/runs/{run_id}/resume", headers=self.headers).status_code, 202)
        run = self.wait(run_id)
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual([n["attempt"] for n in run["nodes"]], [1, 2, 1])
        self.assertEqual(self.upstream.calls["/ok"], 1)

    def test_shutdown_interrupts_and_startup_resumes_only_unfinished_nodes(self):
        run_id = self.create([self.http("first", "ok"), self.http("second", "slow", ["first"])])
        self.wait(run_id, lambda run: run["nodes"][0]["status"] == "succeeded" and run["nodes"][1]["status"] == "running")
        self.close_client()
        self.assertEqual(self.store.get_run("demo", run_id)["status"], "interrupted")
        self.open_client()
        run = self.wait(run_id)
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual([n["attempt"] for n in run["nodes"]], [1, 2])
        self.assertEqual(self.upstream.calls["/ok"], 1)

    def test_deadline_cancels_node_and_never_marks_success_later(self):
        run_id = self.create([self.http("slow", "slow", timeout=0.05)])
        run = self.wait(run_id)
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["nodes"][0]["error"], "Node deadline exceeded")
        time.sleep(0.35)
        self.assertEqual(self.get(run_id)["nodes"][0]["status"], "failed")

    def test_http_redirect_and_oversized_response_fail(self):
        for connector in ("redirect", "large"):
            run = self.wait(self.create([self.http("node", connector)]))
            self.assertEqual(run["status"], "failed")
        self.assertEqual(self.upstream.calls["/ok"], 0)

    def test_real_openai_compatible_protocol_uses_fixed_server_model(self):
        run = self.wait(self.create([{"id": "model", "type": "inference", "config": {"prompt": "hello", "mode": "live"}}]))
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(run["nodes"][0]["output"], "model adapter response")
        self.assertEqual(self.upstream.model_request["model"], "local-protocol-fixture")
        self.assertEqual(self.upstream.model_request["messages"][0]["content"], "hello")

    def test_independent_branches_overlap_but_join_executes_once(self):
        run = self.wait(self.create([self.http("one", "parallel"), self.http("two", "parallel"),
            {"id": "join", "type": "text", "depends_on": ["one", "two"], "config": {"template": "${one.output}${two.output}"}}]))
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(self.upstream.max_active, 2)
        self.assertEqual(run["nodes"][2]["attempt"], 1)

    def test_atomic_queue_limit_also_applies_to_resume(self):
        self.close_client()
        limited = Store(replace(self.settings, max_pending_runs=1))
        definition = SubmitRun.model_validate({"workflow": {"name": "limited", "nodes": [
            {"id": "x", "type": "text", "config": {"template": "hello"}}]}, "inputs": {}}).model_dump()
        first = limited.submit("demo", definition, "a")
        with self.assertRaises(StoreError) as caught:
            limited.submit("demo", definition, "b")
        self.assertEqual(caught.exception.status, 429)
        limited.finish_run(first["id"], "failed", "fixture")
        limited.submit("demo", definition, "b")
        with self.assertRaises(StoreError) as caught:
            limited.resume("demo", first["id"])
        self.assertEqual(caught.exception.status, 429)
        self.open_client()


if __name__ == "__main__":
    unittest.main()

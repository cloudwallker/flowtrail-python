"""Run real processes, then force-kill and restart after a committed checkpoint.

All tokens and databases are temporary. No public service or model key is needed.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]


class Fixture(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.server.calls[self.path] += 1
        if self.path == "/slow":
            time.sleep(1)
        data = b'{"healthy":true}'
        try:
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (ConnectionError, OSError):
            pass


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until(predicate, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (httpx.HTTPError, OSError):
            pass
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for the expected local server state")


def main():
    fixture = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    fixture.calls = Counter()
    thread = threading.Thread(target=fixture.serve_forever, daemon=True)
    thread.start()
    temporary_root = ROOT / ".smoke"
    temporary_root.mkdir(exist_ok=True)
    process = None
    try:
        with tempfile.TemporaryDirectory(dir=temporary_root, prefix="run-") as temporary:
            assert Path(temporary).resolve().is_relative_to(temporary_root.resolve())
            port = free_port()
            base = f"http://127.0.0.1:{port}"
            fixture_base = f"http://127.0.0.1:{fixture.server_port}"
            env = os.environ.copy()
            env["PYTHONPATH"] = str(ROOT / "src")
            env["FLOWTRAIL_DB"] = str(Path(temporary) / "runtime.sqlite3")
            env["FLOWTRAIL_CONNECTORS_JSON"] = json.dumps({
                "fast": {"url": fixture_base + "/fast"}, "slow": {"url": fixture_base + "/slow"}})
            bootstrap = subprocess.run([sys.executable, "-m", "flowtrail", "bootstrap", "--tenant", "smoke", "--role", "admin"],
                                       cwd=ROOT, env=env, capture_output=True, text=True, check=True)
            token = bootstrap.stdout.strip()
            assert len(token) >= 32

            def start():
                proc = subprocess.Popen([sys.executable, "-m", "flowtrail", "serve", "--port", str(port)],
                                        cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                try:
                    wait_until(lambda: httpx.get(base + "/healthz", timeout=1, trust_env=False).status_code == 200)
                except BaseException:
                    proc.kill()
                    proc.wait(timeout=5)
                    raise
                return proc

            process = start()
            with httpx.Client(base_url=base, headers={"Authorization": "Bearer " + token}, timeout=4, trust_env=False) as client:
                assert client.get("/api/me").json() == {"tenant_id": "smoke", "role": "admin"}
                assert client.put("/api/objects/report.txt", content=b"local smoke").status_code == 201
                assert client.get("/api/objects/report.txt").content == b"local smoke"
                workflow = {"workflow": {"name": "process recovery", "nodes": [
                    {"id": "checkpoint", "type": "http", "config": {"connector": "fast"}},
                    {"id": "unfinished", "type": "http", "depends_on": ["checkpoint"], "config": {"connector": "slow"}},
                    {"id": "join", "type": "inference", "depends_on": ["unfinished"],
                     "config": {"prompt": "${unfinished.output}", "mode": "mock"}}]}, "inputs": {}}
                response = client.post("/api/runs", json=workflow, headers={"Idempotency-Key": "process-recovery"})
                assert response.status_code == 202
                run_id = response.json()["id"]

                def ready_to_kill():
                    run = client.get("/api/runs/" + run_id).json()
                    return (run["nodes"][0]["status"] == "succeeded"
                            and run["nodes"][1]["status"] == "running" and fixture.calls["/slow"] >= 1)

                wait_until(ready_to_kill)
                process.kill()  # A real process death: no ASGI lifespan shutdown.
                process.wait(timeout=5)
                process = start()

                def settled():
                    run = client.get("/api/runs/" + run_id).json()
                    return run if run["status"] in {"succeeded", "failed"} else None

                result = wait_until(settled)
                assert result["status"] == "succeeded", result["status"]
                assert [n["attempt"] for n in result["nodes"]] == [1, 2, 1]
                assert fixture.calls["/fast"] == 1
                assert client.get("/api/objects/report.txt").content == b"local smoke"
                replay = client.get(f"/api/runs/{run_id}/events?follow=false").text
                assert "RUN_RECOVERED" in replay and "RUN_SUCCEEDED" in replay
                assert client.post("/api/runs", json=workflow, headers={"Idempotency-Key": "process-recovery"}).json()["id"] == run_id
                assert client.delete("/api/objects/report.txt").status_code == 204
                assert client.get("/api/quota").json()["used_bytes"] == 0
            process.terminate()
            process.wait(timeout=5)
            process = None
            print(json.dumps({"ok": True, "checks": ["bootstrap", "authenticated HTTP API", "objects and quota",
                "real process kill and restart", "successful checkpoint reused", "interrupted node attempted twice",
                "SSE recovery replay", "idempotent submission"], "checkpoint_calls": fixture.calls["/fast"]}))
    finally:
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        fixture.shutdown()
        fixture.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()

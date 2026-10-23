"""Verify frontend retry intent against real FastAPI/SQLite after socket loss.

Run from any directory with this project's Python environment and Node.js 20+.
Every service binds loopback. Tokens are temporary and never logged or written.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx

from smoke import free_port, wait_until

ROOT = Path(__file__).resolve().parents[1]
FAULT_CASES = {"smoke-response-loss", "smoke-body-change", "smoke-identity-change"}


class LossProxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def forward(self):
        content = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        connection = http.client.HTTPConnection("127.0.0.1", self.server.backend_port, timeout=10)
        try:
            # Authorization is forwarded only; it is never stored in records or logs.
            headers = {name: value for name, value in self.headers.items()
                       if name.lower() not in {"host", "connection", "transfer-encoding", "accept-encoding"}}
            headers["Connection"] = "close"
            connection.request(self.command, self.path, body=content, headers=headers)
            upstream = connection.getresponse()
            response = upstream.read()
            drop = False
            if self.command == "POST" and self.path == "/api/runs":
                payload = json.loads(content)
                name = payload.get("workflow", {}).get("name")
                with self.server.guard:
                    if upstream.status == 202 and name in FAULT_CASES and name not in self.server.dropped:
                        self.server.dropped.add(name)
                        drop = True
                    self.server.records.append({"name": name, "key": self.headers.get("Idempotency-Key"),
                        "body_hash": hashlib.sha256(content).hexdigest(), "status": upstream.status,
                        "run_id": json.loads(response).get("id"), "dropped": drop})
            if drop:
                # FastAPI has returned 202 only after SQLite committed. Close
                # without sending even the response headers to reproduce ambiguity.
                self.close_connection = True
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self.connection.close()
                return
            self.send_response(upstream.status)
            for name, value in upstream.getheaders():
                if name.lower() not in {"connection", "content-length", "transfer-encoding", "server", "date"}:
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(response)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(response)
            self.close_connection = True
        finally:
            connection.close()

    do_GET = forward
    do_POST = forward


def stop_process(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", default="node", help="Node executable (default: node from PATH)")
    arguments = parser.parse_args()
    node = shutil.which(arguments.node)
    if node is None:
        raise RuntimeError("Node executable was not found; pass --node with its absolute path")
    temporary_root = ROOT / ".smoke"
    temporary_root.mkdir(exist_ok=True)
    backend = proxy = thread = node_process = None
    try:
        with ExitStack() as cleanup:
            directory = cleanup.enter_context(tempfile.TemporaryDirectory(dir=temporary_root, prefix="submission-"))
            # LIFO: all children stop before TemporaryDirectory attempts removal,
            # including failures while bootstrapping, waiting, or asserting.
            cleanup.callback(lambda: stop_process(backend))
            cleanup.callback(lambda: stop_process(node_process))
            env = os.environ.copy()
            env.update({"PYTHONPATH": str(ROOT / "src"), "FLOWTRAIL_DB": str(Path(directory) / "retry.sqlite3"),
                        "FLOWTRAIL_CONNECTORS_JSON": "{}", "FLOWTRAIL_MODEL_BASE_URL": "",
                        "FLOWTRAIL_MODEL_NAME": "", "FLOWTRAIL_MODEL_API_KEY": ""})

            def bootstrap(tenant):
                result = subprocess.run([sys.executable, "-m", "flowtrail", "bootstrap", "--tenant", tenant,
                    "--role", "operator"], cwd=ROOT, env=env, capture_output=True, timeout=15)
                if result.returncode != 0:
                    raise RuntimeError("Temporary bootstrap failed")
                token = result.stdout.decode("ascii").strip()
                if not 32 <= len(token) <= 256:
                    raise RuntimeError("Temporary bootstrap returned an invalid token")
                return token

            token_a, token_b = bootstrap("submission_a"), bootstrap("submission_b")
            port = free_port()
            backend = subprocess.Popen([sys.executable, "-m", "flowtrail", "serve", "--host", "127.0.0.1",
                "--port", str(port)], cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            wait_until(lambda: httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=1,
                                         trust_env=False).status_code == 200)
            proxy = ThreadingHTTPServer(("127.0.0.1", 0), LossProxy)
            proxy.daemon_threads = False
            proxy.backend_port, proxy.guard = port, threading.Lock()
            proxy.dropped, proxy.records = set(), []
            thread = threading.Thread(target=proxy.serve_forever, daemon=True)
            thread.start()
            node_env = os.environ.copy()
            node_env.update({"FLOWTRAIL_SMOKE_BASE": f"http://127.0.0.1:{proxy.server_port}",
                             "FLOWTRAIL_SMOKE_TOKEN_A": token_a, "FLOWTRAIL_SMOKE_TOKEN_B": token_b})
            node_process = subprocess.Popen([node, str(ROOT / "scripts" / "submission_client.mjs")],
                cwd=directory, env=node_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, encoding="utf-8", errors="replace")
            try:
                stdout, _ = node_process.communicate(timeout=40)
            except subprocess.TimeoutExpired:
                stop_process(node_process)
                raise RuntimeError("Submission client timed out") from None
            if node_process.returncode != 0:
                raise AssertionError("Real frontend submission regression failed")
            result = json.loads(stdout)
            records = proxy.records
            assert len(records) == 7 and all(item["status"] == 202 for item in records)
            assert len(proxy.dropped) == 3
            first, retry = records[:2]
            assert first["dropped"] and not retry["dropped"]
            assert first["key"] and first["key"] == retry["key"]
            assert first["body_hash"] == retry["body_hash"]
            assert first["run_id"] == retry["run_id"]
            assert records[2]["key"] != retry["key"] and records[2]["run_id"] != retry["run_id"]
            assert records[3]["key"] != records[4]["key"]
            assert records[3]["body_hash"] != records[4]["body_hash"]
            assert records[5]["key"] != records[6]["key"]
            assert records[5]["body_hash"] == records[6]["body_hash"]
            assert result["ok"] and result["first_intent_runs"] == 1
            result["faults_verified_after_backend_commit"] = True
            # Shut down the database owner before TemporaryDirectory removes SQLite files.
            stop_process(node_process)
            stop_process(backend)
            backend = None
            print(json.dumps(result, sort_keys=True))
    finally:
        stop_process(node_process)
        stop_process(backend)
        if proxy is not None:
            proxy.shutdown()
            proxy.server_close()
        if thread is not None:
            thread.join(timeout=3)


if __name__ == "__main__":
    main()

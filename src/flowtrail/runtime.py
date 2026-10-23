from __future__ import annotations

import asyncio
import json
import logging

import httpx

from .config import Settings
from .async_store import AsyncStore, finish_before_cancel
from .locking import RuntimeLock
from .models import render
from .store import Store

LOG = logging.getLogger(__name__)


class NodeFailure(Exception):
    """Messages are safe constants and never contain an upstream response or credential."""


class Runtime:
    def __init__(self, store: Store, settings: Settings, db: AsyncStore | None = None):
        self.store, self.settings = store, settings
        self.db = db if db is not None else AsyncStore(
            store, max_workers=settings.db_workers, max_queue=settings.db_queue)
        self.lock = RuntimeLock(settings.db_path)
        self.tasks: dict[str, asyncio.Task] = {}
        self.scheduler = None
        self.capacity = asyncio.Semaphore(settings.max_parallel_nodes)
        self._stop_task = None

    async def start(self):
        try:
            self.lock.acquire()
            await self.db.call("recover")
            self.scheduler = asyncio.create_task(self._schedule(), name="flowtrail-scheduler")
        except BaseException:
            try:
                await self.db.close()
            finally:
                self.lock.release()
            raise

    async def stop(self):
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop(), name="flowtrail-runtime-stop")
        await finish_before_cancel(self._stop_task)

    async def _stop(self):
        try:
            if self.scheduler:
                self.scheduler.cancel()
                await asyncio.gather(self.scheduler, return_exceptions=True)
            running = list(self.tasks.values())
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
            self.tasks.clear()
        finally:
            try:
                await self.db.close()
            finally:
                self.lock.release()

    async def _schedule(self):
        while True:
            try:
                for run_id, task in list(self.tasks.items()):
                    if task.done():
                        # _run handles node failures; this consumes unexpected task failures.
                        if not task.cancelled():
                            task.exception()
                        self.tasks.pop(run_id, None)
                slots = self.settings.max_parallel_runs - len(self.tasks)
                if slots > 0:
                    for run in await self.db.call("queued", slots):
                        try:
                            claimed = await self.db.call("start_run", run["id"])
                        except asyncio.CancelledError:
                            # The transaction may have committed before cancellation
                            # reached its awaiting caller. No run task exists yet.
                            detail = await self.db.call("get_run", run["tenant_id"], run["id"])
                            if detail["status"] == "running":
                                await self.db.call("finish_run", run["id"], "interrupted",
                                                   "Server stopped during execution")
                            raise
                        if claimed:
                            self.tasks[run["id"]] = asyncio.create_task(
                                self._run(run["tenant_id"], run["id"]), name="flowtrail-run")
            except Exception as error:
                # Avoid logging exception messages, which may contain external configuration.
                LOG.warning("Scheduler deferred (%s)", type(error).__name__)
            await asyncio.sleep(self.settings.poll_seconds)

    async def _run(self, tenant_id, run_id):
        try:
            async with asyncio.timeout(300):
                await self._run_nodes(tenant_id, run_id)
        except asyncio.CancelledError:
            await self.db.call("finish_run", run_id, "interrupted", "Server stopped during execution")
            raise
        except TimeoutError:
            await self.db.call("finish_run", run_id, "failed", "Run exceeded its 300 second deadline")
        except Exception:
            await self.db.call("finish_run", run_id, "failed", "Internal execution error")

    async def _run_nodes(self, tenant_id, run_id):
        active: dict[str, asyncio.Task] = {}
        try:
            while True:
                run = await self.db.call("get_run", tenant_id, run_id)
                states = {node["id"]: node for node in run["nodes"]}
                failed = any(node["status"] in {"failed", "interrupted"} for node in states.values())
                outputs = {node_id: node["output"] for node_id, node in states.items() if node["status"] == "succeeded"}
                if not failed:
                    for definition in run["workflow"]["nodes"]:
                        node_id = definition["id"]
                        if len(active) >= self.settings.per_run_parallelism:
                            break
                        if node_id in active or states[node_id]["status"] != "pending":
                            continue
                        if all(states[dep]["status"] == "succeeded" for dep in definition["depends_on"]):
                            active[node_id] = asyncio.create_task(
                                self._execute(tenant_id, run_id, definition, run["inputs"], outputs.copy()))
                if not active:
                    complete = all(node["status"] == "succeeded" for node in states.values())
                    await self.db.call("finish_run", run_id, "succeeded" if complete else "failed",
                                       None if complete else "One or more nodes failed")
                    return
                done, _ = await asyncio.wait(active.values(), return_when=asyncio.FIRST_COMPLETED)
                for node_id, task in list(active.items()):
                    if task in done:
                        await task
                        del active[node_id]
        finally:
            for task in active.values():
                task.cancel()
            await asyncio.gather(*active.values(), return_exceptions=True)

    async def _execute(self, tenant_id, run_id, node, inputs, outputs):
        start_requested = False
        try:
            async with self.capacity:
                start_requested = True
                attempt = await self.db.call("start_node", run_id, node["id"])
                if attempt > 5:
                    raise NodeFailure("Node attempt budget exhausted (maximum 5)")
                async with asyncio.timeout(node["config"].get("timeout_seconds", 10)):
                    output = await self.execute_node(node, inputs, outputs)
                if len(output.encode("utf-8")) > self.settings.max_output_bytes:
                    raise NodeFailure("Node output exceeds the configured byte limit")
                await self.db.call("finish_node", run_id, node["id"], "succeeded", output=output)
        except asyncio.CancelledError:
            if start_requested:
                detail = await self.db.call("get_run", tenant_id, run_id)
                state = next(item for item in detail["nodes"] if item["id"] == node["id"])
                if state["status"] == "running":
                    await self.db.call("finish_node", run_id, node["id"], "interrupted", error="Execution cancelled")
            raise
        except (TimeoutError, httpx.TimeoutException):
            await self.db.call("finish_node", run_id, node["id"], "failed", error="Node deadline exceeded")
        except NodeFailure as error:
            await self.db.call("finish_node", run_id, node["id"], "failed", error=str(error))
        except Exception:
            await self.db.call("finish_node", run_id, node["id"], "failed", error="Node execution failed")

    async def execute_node(self, node, inputs, outputs):
        config = node["config"]
        if node["type"] == "text":
            return self._render(config["template"], inputs, outputs)
        if node["type"] == "http":
            connector = self.settings.connectors.get(config["connector"])
            if connector is None:
                raise NodeFailure("Server connector is no longer configured")
            body = await self._request("GET", connector["url"], headers=connector.get("headers", {}))
            return body.decode("utf-8", errors="replace")
        prompt = self._render(config["prompt"], inputs, outputs)
        if config.get("mode", "mock") == "mock":
            return "[mock] " + prompt
        if not all((self.settings.model_base_url, self.settings.model_name, self.settings.model_api_key)):
            raise NodeFailure("Live model is not configured on the server")
        body = await self._request(
            "POST", self.settings.model_base_url.rstrip("/") + "/chat/completions",
            headers={"Authorization": "Bearer " + self.settings.model_api_key},
            json_body={"model": self.settings.model_name, "stream": False, "max_tokens": 1024,
                       "messages": [{"role": "user", "content": prompt}]})
        try:
            output = json.loads(body)["choices"][0]["message"]["content"]
            if not isinstance(output, str):
                raise ValueError()
            return output
        except (ValueError, KeyError, IndexError, TypeError):
            raise NodeFailure("Model returned an invalid response") from None

    def _render(self, template, inputs, outputs):
        try:
            return render(template, inputs, outputs, max_bytes=self.settings.max_output_bytes)
        except ValueError:
            raise NodeFailure("Rendered template exceeds the configured byte limit") from None

    async def _request(self, method, url, headers=None, json_body=None):
        # All URLs and headers come from the server configuration. Proxies and redirects are off.
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False, timeout=120) as client:
            async with client.stream(method, url, headers=headers, json=json_body) as response:
                if not 200 <= response.status_code < 300:
                    raise NodeFailure("Upstream returned a non-success status")
                parts, size = [], 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > self.settings.max_output_bytes:
                        raise NodeFailure("Upstream response exceeds the configured byte limit")
                    parts.append(chunk)
                return b"".join(parts)

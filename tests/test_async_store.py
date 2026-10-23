from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path

from flowtrail.config import Settings
from flowtrail.runtime import Runtime
from flowtrail.locking import RuntimeLock
from flowtrail.store import Store


class AsyncStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(db_path=Path(self.tmp.name) / "async.sqlite3", poll_seconds=0.01)
        self.store = Store(self.settings)
        self.token = self.store.issue_token("demo", "operator")

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_sqlite_write_lock_does_not_block_startup_heartbeat(self):
        # A synchronous recover() blocks every unrelated coroutine while waiting for SQLite.
        locked, released = threading.Event(), threading.Event()

        def holder():
            with closing(sqlite3.connect(self.settings.db_path)) as connection:
                connection.execute("BEGIN IMMEDIATE")
                locked.set()
                time.sleep(0.3)
                connection.rollback()
            released.set()

        thread = threading.Thread(target=holder)
        thread.start()
        self.assertTrue(locked.wait(1))
        ticks = []

        async def heartbeat():
            while not released.is_set():
                ticks.append(time.monotonic())
                await asyncio.sleep(0.01)

        runtime = Runtime(self.store, self.settings)
        pulse = asyncio.create_task(heartbeat())
        try:
            await runtime.start()
        finally:
            await runtime.stop()
            await pulse
            thread.join(1)
        self.assertGreaterEqual(len(ticks), 8, "SQLite lock acquisition blocked the event loop")

    async def test_cancelled_transaction_retains_capacity_until_commit_finishes(self):
        from flowtrail.async_store import AsyncStore

        entered, release = threading.Event(), threading.Event()
        store = self.store

        class DelayedStore:
            def put_object(self):
                entered.set()
                release.wait(2)
                return store.put_object("demo", "first.txt", b"first")

            def second(self):
                return store.put_object("demo", "second.txt", b"second")

        db = AsyncStore(DelayedStore(), max_workers=1, max_queue=0)
        first = asyncio.create_task(db.call("put_object"))
        while not entered.is_set():
            await asyncio.sleep(0.005)
        second = asyncio.create_task(db.call("second"))
        first.cancel()
        await asyncio.sleep(0.01)
        first.cancel()  # Repeated request cancellation must not escape the drain.
        await asyncio.sleep(0.02)
        try:
            self.assertFalse(first.done())
            self.assertFalse(second.done())
            self.assertEqual(store.list_objects("demo"), [])
        finally:
            release.set()
            results = await asyncio.gather(first, second, return_exceptions=True)
            await db.close()
        self.assertIsInstance(results[0], asyncio.CancelledError)
        self.assertEqual([row["name"] for row in store.list_objects("demo")], ["first.txt", "second.txt"])

    async def test_locked_write_transaction_keeps_unrelated_coroutines_responsive(self):
        from flowtrail.async_store import AsyncStore

        locked, released = threading.Event(), threading.Event()

        def holder():
            with closing(sqlite3.connect(self.settings.db_path)) as connection:
                connection.execute("BEGIN IMMEDIATE")
                locked.set()
                time.sleep(0.3)
                connection.rollback()
            released.set()

        thread = threading.Thread(target=holder)
        thread.start()
        self.assertTrue(locked.wait(1))
        db = AsyncStore(self.store, max_workers=1, max_queue=1)
        ticks = []

        async def heartbeat():
            while not released.is_set():
                ticks.append(time.monotonic())
                await asyncio.sleep(0.01)

        pulse = asyncio.create_task(heartbeat())
        try:
            await db.call("put_object", "demo", "locked.txt", b"committed")
        finally:
            await db.close()
            await pulse
            thread.join(1)
        self.assertGreaterEqual(len(ticks), 8, "The whole SQLite write must run on a worker")
        self.assertEqual(self.store.get_object("demo", "locked.txt")[0], b"committed")

    async def test_health_endpoint_responds_during_real_api_sqlite_lock_wait(self):
        import httpx
        from flowtrail.app import create_app

        app = create_app(self.settings)
        locked, release, writing = threading.Event(), threading.Event(), threading.Event()
        original_write = app.state.store.put_object

        def observed_write(*args, **kwargs):
            writing.set()
            return original_write(*args, **kwargs)

        app.state.store.put_object = observed_write

        def holder():
            with closing(sqlite3.connect(self.settings.db_path)) as connection:
                connection.execute("BEGIN IMMEDIATE")
                locked.set()
                release.wait(2)
                connection.rollback()

        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                thread = threading.Thread(target=holder)
                thread.start()
                self.assertTrue(locked.wait(1))
                write = asyncio.create_task(client.put("/api/objects/responsive.txt", content=b"responsive",
                    headers={"Authorization": "Bearer " + self.token}))
                try:
                    async with asyncio.timeout(1):
                        while not writing.is_set():
                            await asyncio.sleep(0.005)
                    health = await asyncio.wait_for(client.get("/healthz"), timeout=0.3)
                    self.assertEqual(health.status_code, 200)
                    self.assertFalse(write.done(), "The SQLite writer must still be blocked by the held lock")
                finally:
                    release.set()
                    response = await write
                    thread.join(1)
                self.assertEqual(response.status_code, 201)
                self.assertEqual(self.store.get_object("demo", "responsive.txt")[0], b"responsive")

    async def test_shutdown_drains_committed_node_start_before_releasing_process_lock(self):
        entered, release = threading.Event(), threading.Event()

        class DelayedStartStore(Store):
            def start_node(self, run_id, node_id):
                result = super().start_node(run_id, node_id)
                entered.set()
                release.wait(2)
                return result

        store = DelayedStartStore(self.settings)
        run = store.submit("demo", {"workflow": {"name": "drain", "nodes": [
            {"id": "text", "type": "text", "depends_on": [], "config": {"template": "hello"}}]},
            "inputs": {}}, "drain-node")
        runtime = Runtime(store, self.settings)
        await runtime.start()
        while not entered.is_set():
            await asyncio.sleep(0.005)
        stop = asyncio.create_task(runtime.stop())
        await asyncio.sleep(0.03)
        competitor = RuntimeLock(self.settings.db_path)
        try:
            self.assertFalse(stop.done())
            with self.assertRaises(RuntimeError):
                competitor.acquire()
        finally:
            release.set()
            await stop
        final = store.get_run("demo", run["id"])
        self.assertEqual(final["status"], "interrupted")
        self.assertEqual(final["nodes"][0]["status"], "interrupted")
        self.assertIsNone(final["nodes"][0]["output"])
        competitor.acquire()
        competitor.release()

    async def test_cancelled_scheduler_claim_is_marked_interrupted(self):
        entered, release = threading.Event(), threading.Event()

        class DelayedClaimStore(Store):
            def start_run(self, run_id):
                result = super().start_run(run_id)
                entered.set()
                release.wait(2)
                return result

        store = DelayedClaimStore(self.settings)
        run = store.submit("demo", {"workflow": {"name": "claim", "nodes": [
            {"id": "text", "type": "text", "depends_on": [], "config": {"template": "hello"}}]},
            "inputs": {}}, "drain-claim")
        runtime = Runtime(store, self.settings)
        await runtime.start()
        while not entered.is_set():
            await asyncio.sleep(0.005)
        stop = asyncio.create_task(runtime.stop())
        await asyncio.sleep(0.03)
        try:
            self.assertFalse(stop.done())
        finally:
            release.set()
            await stop
        final = store.get_run("demo", run["id"])
        self.assertEqual(final["status"], "interrupted")
        self.assertEqual(final["nodes"][0]["attempt"], 0)

    async def test_cancelled_close_drains_write_and_rejects_new_operations(self):
        from flowtrail.async_store import AsyncStore

        entered, release = threading.Event(), threading.Event()
        store = self.store

        class DelayedStore:
            def write(self):
                entered.set()
                release.wait(2)
                return store.put_object("demo", "drained.txt", b"drained")

        db = AsyncStore(DelayedStore(), max_workers=1, max_queue=1)
        write = asyncio.create_task(db.call("write"))
        while not entered.is_set():
            await asyncio.sleep(0.005)
        close = asyncio.create_task(db.close())
        await asyncio.sleep(0.01)
        close.cancel()
        await asyncio.sleep(0.01)
        close.cancel()
        try:
            self.assertFalse(close.done())
            with self.assertRaises(RuntimeError):
                await db.call("write")
        finally:
            release.set()
            results = await asyncio.gather(write, close, return_exceptions=True)
            await db.close()
        self.assertIsInstance(results[1], asyncio.CancelledError)
        self.assertEqual(store.get_object("demo", "drained.txt")[0], b"drained")

    async def test_cancelled_startup_closes_workers_and_keeps_lock_until_recovery_finishes(self):
        entered, release = threading.Event(), threading.Event()

        class DelayedRecoveryStore(Store):
            def recover(self):
                super().recover()
                entered.set()
                release.wait(2)

        runtime = Runtime(DelayedRecoveryStore(self.settings), self.settings)
        start = asyncio.create_task(runtime.start())
        while not entered.is_set():
            await asyncio.sleep(0.005)
        start.cancel()
        await asyncio.sleep(0.02)
        competitor = RuntimeLock(self.settings.db_path)
        try:
            self.assertFalse(start.done())
            with self.assertRaises(RuntimeError):
                competitor.acquire()
        finally:
            release.set()
            results = await asyncio.gather(start, return_exceptions=True)
        self.assertIsInstance(results[0], asyncio.CancelledError)
        with self.assertRaises(RuntimeError):
            await runtime.db.call("list_runs", "demo")
        competitor.acquire()
        competitor.release()

    async def test_cancellation_after_node_success_commit_keeps_valid_checkpoint(self):
        entered, release = threading.Event(), threading.Event()

        class DelayedFinishStore(Store):
            def finish_node(self, run_id, node_id, status, output=None, error=None):
                result = super().finish_node(run_id, node_id, status, output, error)
                if status == "succeeded":
                    entered.set()
                    release.wait(2)
                return result

        store = DelayedFinishStore(self.settings)
        run = store.submit("demo", {"workflow": {"name": "checkpoint", "nodes": [
            {"id": "text", "type": "text", "depends_on": [], "config": {"template": "hello"}}]},
            "inputs": {}}, "drain-finish")
        runtime = Runtime(store, self.settings)
        await runtime.start()
        while not entered.is_set():
            await asyncio.sleep(0.005)
        stop = asyncio.create_task(runtime.stop())
        await asyncio.sleep(0.03)
        release.set()
        await stop
        final = store.get_run("demo", run["id"])
        self.assertEqual(final["status"], "interrupted")
        self.assertEqual(final["nodes"][0]["status"], "succeeded")
        self.assertEqual(final["nodes"][0]["output"], "hello")
        self.assertIsNone(final["nodes"][0]["error"])


if __name__ == "__main__":
    unittest.main()

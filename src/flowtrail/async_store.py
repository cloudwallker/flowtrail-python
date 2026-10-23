"""Bounded offload of complete Store operations, including their transactions.

Connections stay inside synchronous Store methods. Cancelling a caller does not
cancel an already submitted transaction: its result is drained before cancellation
is delivered, so shutdown and capacity accounting reflect real database work.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial


async def finish_before_cancel(future):
    """Wait through repeated cancellation and only then re-raise cancellation."""
    cancelled = False
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            # Observe the outcome below, after checking pending cancellation.
            break
    if cancelled:
        if not future.cancelled():
            future.exception()
        raise asyncio.CancelledError
    return future.result()


class AsyncStore:
    def __init__(self, store, *, max_workers: int = 4, max_queue: int = 32):
        if max_workers < 1 or max_queue < 0:
            raise ValueError("Database workers must be positive and queue capacity non-negative")
        self.store = store
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="flowtrail-db")
        self._capacity = asyncio.Semaphore(max_workers + max_queue)
        self._pending: set[asyncio.Future] = set()
        self._closing = False
        self._close_task = None

    async def call(self, method_name: str, *args, **kwargs):
        if self._closing:
            raise RuntimeError("Database worker pool is closed")
        method = getattr(self.store, method_name)
        await self._capacity.acquire()
        if self._closing:
            self._capacity.release()
            raise RuntimeError("Database worker pool is closed")
        try:
            # This is the only submission point. Capacity bounds the executor's
            # running plus queued operations, despite its internal unbounded queue.
            future = asyncio.get_running_loop().run_in_executor(
                self._executor, partial(method, *args, **kwargs))
            self._pending.add(future)
            try:
                return await finish_before_cancel(future)
            finally:
                self._pending.discard(future)
        finally:
            self._capacity.release()

    async def close(self):
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._drain(), name="flowtrail-db-drain")
        await finish_before_cancel(self._close_task)

    async def _drain(self):
        if self._pending:
            await asyncio.gather(*tuple(self._pending), return_exceptions=True)
        # All transactions are now complete. Idle worker shutdown cannot create
        # late writes and does not need a blocking join on the event loop.
        self._executor.shutdown(wait=False, cancel_futures=True)

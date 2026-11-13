"""Bounded readiness sampling and content-free ASGI request diagnostics."""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from starlette.responses import JSONResponse

from .async_store import finish_before_cancel


class ReadinessMonitor:
    """One independent read-only SQLite probe at a time; requests read a snapshot.

    A probe checks schema readability, not write availability, capacity or remote
    model health. WAL writers need not make the process unready.
    """

    def __init__(self, db_path, runtime, *, interval=1.0, timeout=0.2, stale_after=3.0):
        self.db_path, self.runtime = db_path, runtime
        self.interval, self.timeout, self.stale_after = interval, timeout, stale_after
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='flowtrail-readiness')
        self._task = self._close_task = None
        self._closing = False
        self._checked_at = 0.0
        self._database_ok = False

    async def start(self):
        if self._task is None and not self._closing:
            self._task = asyncio.create_task(self._sample(), name='flowtrail-readiness')

    def ready(self):
        scheduler = self.runtime.scheduler
        return bool(not self._closing and self._task and not self._task.done()
                    and scheduler and not scheduler.done() and self.runtime._stop_task is None
                    and self._database_ok and time.monotonic() - self._checked_at < self.stale_after)

    def _probe(self):
        deadline = time.monotonic() + self.timeout
        try:
            # mode=ro prevents a missing database from being silently recreated.
            connection = sqlite3.connect(self.db_path.resolve().as_uri() + '?mode=ro',
                                         uri=True, timeout=self.timeout, isolation_level=None)
            try:
                connection.execute(f'PRAGMA busy_timeout={int(self.timeout * 1000)}')
                connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
                connection.execute('SELECT id FROM tenants LIMIT 1').fetchone()
                return True
            finally:
                connection.close()
        except (sqlite3.Error, OSError):
            return False

    async def _sample(self):
        while True:
            result = await finish_before_cancel(
                asyncio.get_running_loop().run_in_executor(self._executor, self._probe))
            self._database_ok = result
            self._checked_at = time.monotonic()
            await asyncio.sleep(self.interval)

    async def close(self):
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._close(), name='flowtrail-readiness-close')
        await finish_before_cancel(self._close_task)

    async def _close(self):
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        # The sole submitted probe was drained before cancellation propagated.
        self._executor.shutdown(wait=False, cancel_futures=True)


class RequestDiagnostics:
    """Pure ASGI middleware: never consume/buffer response bodies or log input."""

    def __init__(self, app):
        self.app = app
        self.logger = logging.getLogger('flowtrail.requests')
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        request_id = uuid.uuid4().hex
        scope.setdefault('state', {})['request_id'] = request_id
        started_at = time.monotonic()
        status, started, completed, stream, disconnected = 500, False, False, False, False
        outcome = 'interrupted'
        method = scope.get('method', '')
        if method not in {'GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS', 'CONNECT', 'TRACE'}:
            method = 'OTHER'

        def record(event, result):
            # The matched route is application metadata; never fall back to raw path.
            route = getattr(scope.get('route'), 'path', None) or 'unmatched'
            self.logger.info(json.dumps({
                'event': event, 'request_id': request_id, 'method': method,
                'route': route, 'status': status, 'stream': stream,
                'outcome': result, 'elapsed_ms': round((time.monotonic() - started_at) * 1000, 3),
            }, separators=(',', ':')))

        async def observed_receive():
            nonlocal disconnected
            message = await receive()
            if message['type'] == 'http.disconnect':
                disconnected = True
            return message

        async def observed_send(message):
            nonlocal status, started, completed, stream
            if message['type'] == 'http.response.start':
                status = message['status']
                headers = [(k, v) for k, v in message.get('headers', []) if k.lower() != b'x-request-id']
                stream = any(k.lower() == b'content-type' and v.lower().startswith(b'text/event-stream')
                             for k, v in headers)
                message = {**message, 'headers': headers + [(b'x-request-id', request_id.encode('ascii'))]}
                started = True
                await send(message)
                if stream:
                    record('response_started', 'streaming')
                return
            await send(message)
            if message['type'] == 'http.response.body' and not message.get('more_body', False):
                completed = True

        try:
            await self.app(scope, observed_receive, observed_send)
            outcome = 'completed' if completed else 'disconnected' if disconnected else 'interrupted'
        except Exception:
            # Do not pass exception messages/tracebacks into access logs or responses.
            outcome = 'error'
            try:
                if not started:
                    await JSONResponse({'detail': 'Internal server error'}, status_code=500)(
                        scope, observed_receive, observed_send)
                elif not completed and stream:
                    await observed_send({'type': 'http.response.body', 'more_body': False,
                                         'body': b'event: error\ndata: {"code":"internal_error"}\n\n'})
                # Do not fabricate a normal end marker for partial non-SSE
                # responses. The ASGI server closes an incomplete response;
                # no exception text or traceback is passed across this boundary.
            except Exception:
                pass  # A disconnected transport cannot receive a sanitized error.
        finally:
            record('request_finished', outcome)

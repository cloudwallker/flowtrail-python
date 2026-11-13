"""Operational contracts exercise the ASGI app and real tenant SQLite data."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx
from fastapi.testclient import TestClient

from flowtrail.app import create_app
from flowtrail.config import Settings
from flowtrail.observability import ReadinessMonitor, RequestDiagnostics
from flowtrail.store import Store, StoreError


class OperationalApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(db_path=Path(self.tmp.name) / 'ops.sqlite3',
                                 quota_bytes=128, poll_seconds=0.01)
        self.store = Store(self.settings)
        self.admin = self.store.issue_token('alpha', 'admin')
        self.other = self.store.issue_token('beta', 'admin')
        self.operator = self.store.issue_token('alpha', 'operator')
        self.viewer = self.store.issue_token('alpha', 'viewer')
        self.app = create_app(self.settings)
        self.context = TestClient(self.app)
        self.client = self.context.__enter__()

    def tearDown(self):
        self.context.__exit__(None, None, None)
        self.tmp.cleanup()

    def headers(self, token=None):
        return {'Authorization': 'Bearer ' + (token or self.admin)}

    def submit(self, token, count=1):
        body = {'workflow': {'name': 'private-workflow-name', 'nodes': [
            {'id': f'n{i}', 'type': 'text', 'config': {'template': 'private-output'}}
            for i in range(count)]}, 'inputs': {}}
        response = self.client.post('/api/runs', json=body, headers=self.headers(token))
        self.assertEqual(response.status_code, 202, response.text)
        run_id = response.json()['id']
        until = time.monotonic() + 3
        while time.monotonic() < until:
            result = self.client.get('/api/runs/' + run_id, headers=self.headers(token)).json()
            if result['status'] == 'succeeded':
                return run_id
            time.sleep(0.01)
        self.fail('Fixture did not complete')

    def test_readiness_is_distinct_from_liveness(self):
        until = time.monotonic() + 2
        while time.monotonic() < until:
            response = self.client.get('/readyz')
            if response.status_code == 200:
                break
            time.sleep(0.01)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'status': 'ready'})
        self.assertEqual(self.client.get('/healthz').status_code, 200)
        self.client.portal.call(self.app.state.readiness.close)
        self.assertEqual(self.client.get('/readyz').status_code, 503)
        self.assertEqual(self.client.get('/readyz').json(), {'status': 'not_ready'})
        self.assertEqual(self.client.get('/healthz').status_code, 200)

    def test_summary_requires_admin_and_counts_only_its_tenant(self):
        self.submit(self.admin, 2)
        self.submit(self.other, 1)
        self.client.put('/api/objects/alpha.bin', content=b'12345', headers=self.headers())
        self.client.put('/api/objects/beta.bin', content=b'x' * 20, headers=self.headers(self.other))
        self.assertEqual(self.client.get('/api/ops/summary').status_code, 401)
        for token in (self.operator, self.viewer):
            self.assertEqual(self.client.get('/api/ops/summary', headers=self.headers(token)).status_code, 403)
        response = self.client.get('/api/ops/summary', headers=self.headers())
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result['runs']['succeeded'], 1)
        self.assertEqual(sum(result['runs'].values()), 1)
        self.assertEqual(result['nodes']['succeeded'], 2)
        self.assertEqual(sum(result['nodes'].values()), 2)
        self.assertEqual(result['objects'], {'count': 1, 'used_bytes': 5, 'limit_bytes': 128, 'remaining_bytes': 123})
        self.assertIn('generated_at', result)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertNotIn('private-output', response.text)
        other = self.client.get('/api/ops/summary', headers=self.headers(self.other)).json()
        self.assertEqual(other['nodes']['succeeded'], 1)
        self.assertEqual(other['objects']['used_bytes'], 20)
        self.assertEqual(self.client.get('/api/ops/summary?tenant_id=beta', headers=self.headers()).status_code, 422)

    def test_request_ids_are_generated_and_logs_use_templates_not_input(self):
        with self.assertLogs('flowtrail.requests', level='INFO') as logs:
            response = self.client.put('/api/objects/secret-file.txt?password=query-secret',
                                       content=b'private-body', headers={**self.headers(), 'X-Request-ID': 'spoof-secret'})
            missing = self.client.get('/secret-unmatched-path?token=other-secret')
            invalid = self.client.get('/api/me', headers=self.headers('invalid-secret'))
        request_id = response.headers.get('x-request-id', '')
        self.assertRegex(request_id, r'^[0-9a-f]{32}$')
        self.assertNotEqual(request_id, missing.headers.get('x-request-id'))
        self.assertRegex(invalid.headers.get('x-request-id', ''), r'^[0-9a-f]{32}$')
        events = [json.loads(record.getMessage()) for record in logs.records]
        own = [event for event in events if event['request_id'] == request_id]
        self.assertEqual(own[-1]['route'], '/api/objects/{name}')
        self.assertEqual(own[-1]['status'], 201)
        self.assertEqual(own[-1]['event'], 'request_finished')
        self.assertEqual(own[-1]['outcome'], 'completed')
        self.assertTrue(any(event['route'] == 'unmatched' for event in events))
        serialized = json.dumps(events)
        for secret in (self.admin, 'secret-file', 'query-secret', 'private-body', 'spoof-secret',
                       'secret-unmatched', 'other-secret', 'invalid-secret', str(self.settings.db_path)):
            self.assertNotIn(secret, serialized)

    def test_sse_logs_response_start_separately_from_stream_finish(self):
        run_id = self.submit(self.admin)
        with self.assertLogs('flowtrail.requests', level='INFO') as logs:
            response = self.client.get(f'/api/runs/{run_id}/events?follow=false', headers=self.headers())
        self.assertIn('RUN_SUCCEEDED', response.text)
        events = [json.loads(record.getMessage()) for record in logs.records]
        self.assertEqual([e['event'] for e in events], ['response_started', 'request_finished'])
        self.assertTrue(all(e['stream'] for e in events))
        self.assertEqual(events[0]['request_id'], events[1]['request_id'])
        self.assertGreaterEqual(events[1]['elapsed_ms'], events[0]['elapsed_ms'])
        self.assertEqual(events[1]['outcome'], 'completed')
        self.assertNotIn(run_id, json.dumps(events))

    def test_body_limit_errors_also_carry_a_generated_request_id(self):
        response = self.client.post('/api/runs', content=b'x' * (self.settings.max_request_bytes + 1))
        self.assertEqual(response.status_code, 413)
        self.assertRegex(response.headers.get('x-request-id', ''), r'^[0-9a-f]{32}$')


class SummarySnapshotTests(unittest.TestCase):
    def test_concurrent_commits_cannot_mix_run_and_node_snapshots(self):
        with tempfile.TemporaryDirectory() as folder:
            settings = Settings(db_path=Path(folder) / 'summary.sqlite3')
            store, writer = Store(settings), Store(settings)
            token = store.issue_token('alpha', 'admin')
            actor = store.authenticate(token)
            original = store.connection
            inserted = []

            def insert_between_reads(statement):
                if 'FROM runs' in statement and 'GROUP BY' in statement and not inserted:
                    inserted.append(writer.submit('alpha', {'workflow': {'name': 'concurrent', 'nodes': [
                        {'id': 'n1', 'type': 'text'}, {'id': 'n2', 'type': 'text'}]}, 'inputs': {}}, None))

            @contextmanager
            def observed_connection(write=False):
                with original(write) as connection:
                    connection.set_trace_callback(insert_between_reads)
                    yield connection

            store.connection = observed_connection
            result = store.operations_summary(actor)
            self.assertEqual(len(inserted), 1)
            self.assertEqual(sum(result['runs'].values()), 0)
            self.assertEqual(sum(result['nodes'].values()), 0)
            self.assertEqual(len(writer.list_runs('alpha')), 1)

    def test_summary_rechecks_a_revoked_admin_at_snapshot_time(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Store(Settings(db_path=Path(folder) / 'summary.sqlite3'))
            token = store.issue_token('alpha', 'admin')
            actor = store.authenticate(token)
            store.revoke_token(actor, actor['token_id'])
            with self.assertRaises(StoreError) as raised:
                store.operations_summary(actor)
            self.assertEqual(raised.exception.status, 401)


class ReadinessTests(unittest.IsolatedAsyncioTestCase):
    async def wait_for(self, predicate, seconds=1):
        async with asyncio.timeout(seconds):
            while not predicate():
                await asyncio.sleep(0.01)

    async def test_missing_database_is_not_created_and_schema_failure_changes_readiness(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'probe.sqlite3'
            scheduler = asyncio.create_task(asyncio.Event().wait())
            runtime = SimpleNamespace(scheduler=scheduler, _stop_task=None)
            monitor = ReadinessMonitor(path, runtime, interval=0.01, timeout=0.05)
            self.assertFalse(monitor.ready())
            await monitor.start()
            try:
                await self.wait_for(lambda: monitor._checked_at > 0)
                self.assertFalse(path.exists())
                self.assertFalse(monitor.ready())
                with closing(sqlite3.connect(path, isolation_level=None)) as db:
                    db.execute('CREATE TABLE tenants(id TEXT)')
                await self.wait_for(monitor.ready)
                with closing(sqlite3.connect(path, isolation_level=None)) as db:
                    db.execute('DROP TABLE tenants')
                await self.wait_for(lambda: not monitor.ready())
                with closing(sqlite3.connect(path, isolation_level=None)) as db:
                    db.execute('CREATE TABLE tenants(id TEXT)')
                await self.wait_for(monitor.ready)
                scheduler.cancel()
                await asyncio.gather(scheduler, return_exceptions=True)
                self.assertFalse(monitor.ready())
            finally:
                await monitor.close()
                scheduler.cancel()
                await asyncio.gather(scheduler, return_exceptions=True)
            self.assertFalse(monitor.ready())

    async def test_blocked_probe_is_single_flight_and_does_not_block_health_or_shutdown_status(self):
        with tempfile.TemporaryDirectory() as folder:
            app = create_app(Settings(db_path=Path(folder) / 'health.sqlite3', poll_seconds=0.01))
            entered, release = threading.Event(), threading.Event()
            calls = []

            def slow_probe():
                calls.append(1)
                entered.set()
                release.wait(2)
                return True

            app.state.readiness._probe = slow_probe
            app.state.readiness.interval = 0.01
            async with app.router.lifespan_context(app):
                try:
                    self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                        response = await asyncio.wait_for(client.get('/healthz'), 0.5)
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual((await client.get('/readyz')).status_code, 503)
                        await asyncio.sleep(0.05)
                        self.assertEqual(len(calls), 1)
                        closing = asyncio.create_task(app.state.readiness.close())
                        await asyncio.sleep(0)
                        self.assertEqual((await client.get('/readyz')).status_code, 503)
                        self.assertFalse(closing.done())
                        release.set()
                        await closing
                finally:
                    release.set()

    async def test_sqlite_exclusive_lock_probe_returns_within_its_short_timeout(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'locked.sqlite3'
            with closing(sqlite3.connect(path, isolation_level=None)) as setup:
                setup.execute('CREATE TABLE tenants(id TEXT)')
            lock = sqlite3.connect(path, isolation_level=None)
            lock.execute('BEGIN EXCLUSIVE')
            runtime = SimpleNamespace(scheduler=None, _stop_task=None)
            monitor = ReadinessMonitor(path, runtime, timeout=0.05)
            try:
                result = await asyncio.wait_for(asyncio.to_thread(monitor._probe), 0.5)
                self.assertFalse(result)
            finally:
                lock.rollback()
                lock.close()
                await monitor.close()

    async def test_a_previous_success_expires_while_the_next_probe_is_blocked(self):
        with tempfile.TemporaryDirectory() as folder:
            scheduler = asyncio.create_task(asyncio.Event().wait())
            runtime = SimpleNamespace(scheduler=scheduler, _stop_task=None)
            monitor = ReadinessMonitor(Path(folder) / 'unused.sqlite3', runtime,
                                       interval=0.01, stale_after=0.05)
            entered, release = threading.Event(), threading.Event()
            calls = []

            def eventually_blocked_probe():
                calls.append(1)
                if len(calls) > 1:
                    entered.set()
                    release.wait(2)
                return True

            monitor._probe = eventually_blocked_probe
            await monitor.start()
            try:
                await self.wait_for(monitor.ready)
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                await self.wait_for(lambda: not monitor.ready())
                self.assertEqual(len(calls), 2)
                release.set()
                await self.wait_for(monitor.ready)
                runtime._stop_task = asyncio.create_task(asyncio.sleep(0))
                self.assertFalse(monitor.ready())
                await runtime._stop_task
            finally:
                release.set()
                await monitor.close()
                scheduler.cancel()
                await asyncio.gather(scheduler, return_exceptions=True)


class RequestFailureTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, *, stream=False, disconnect=False, partial=False):
        sent = []

        async def application(scope, receive, send):
            scope['route'] = SimpleNamespace(path='/fixture/{id}')
            if stream or partial:
                await send({'type': 'http.response.start', 'status': 200,
                            'headers': [(b'content-type', b'text/event-stream' if stream else b'application/json')]})
                await send({'type': 'http.response.body', 'body': b'data: partial\n\n', 'more_body': True})
                if disconnect:
                    await receive()
                    return
            raise ValueError('private-path/token/body/exception-secret')

        async def receive():
            return {'type': 'http.disconnect'}

        async def send(message):
            sent.append(message)

        with self.assertLogs('flowtrail.requests', level='INFO') as captured:
            await RequestDiagnostics(application)({'type': 'http', 'method': 'GET'}, receive, send)
        logs = [json.loads(record.getMessage()) for record in captured.records]
        self.assertNotIn('exception-secret', str(sent) + json.dumps(logs))
        return sent, logs

    async def test_unhandled_exception_has_safe_500_id_and_error_log(self):
        sent, logs = await self.exercise()
        self.assertEqual(sent[0]['status'], 500)
        self.assertIn(b'x-request-id', dict(sent[0]['headers']))
        self.assertEqual(logs[-1]['outcome'], 'error')
        self.assertEqual(logs[-1]['status'], 500)

    async def test_midstream_error_is_not_logged_as_complete(self):
        sent, logs = await self.exercise(stream=True)
        self.assertEqual([log['event'] for log in logs], ['response_started', 'request_finished'])
        self.assertEqual(logs[-1]['outcome'], 'error')
        self.assertIn(b'event: error', sent[-1]['body'])
        self.assertFalse(sent[-1]['more_body'])

    async def test_client_disconnect_is_not_logged_as_complete(self):
        sent, logs = await self.exercise(stream=True, disconnect=True)
        self.assertEqual(logs[-1]['outcome'], 'disconnected')

    async def test_partial_non_sse_failure_never_sends_a_successful_end_marker(self):
        sent, logs = await self.exercise(partial=True)
        self.assertTrue(sent[-1]['more_body'])
        self.assertEqual(logs[-1]['outcome'], 'error')
        self.assertFalse(logs[-1]['stream'])


if __name__ == '__main__':
    unittest.main()

"""Completed histories must flush every storage page in follow mode as well."""
import json
import tempfile
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from flowtrail.app import create_app
from flowtrail.config import Settings
from flowtrail.store import Store


class EventHistoryTests(unittest.TestCase):
    def test_completed_follow_history_flushes_more_than_two_storage_pages(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = Settings(db_path=Path(temporary) / 'history.sqlite3', poll_seconds=0.01)
            store = Store(settings)
            token = store.issue_token('history', 'operator')
            headers = {'Authorization': 'Bearer ' + token}
            with TestClient(create_app(settings)) as client:
                response = client.post('/api/runs', headers=headers, json={'workflow': {'name': 'history', 'nodes': [
                    {'id': 'one', 'type': 'text', 'config': {'template': 'done'}}]}, 'inputs': {}})
                self.assertEqual(response.status_code, 202)
                run_id = response.json()['id']
                deadline = time.monotonic() + 3
                while client.get('/api/runs/' + run_id, headers=headers).json()['status'] != 'succeeded':
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)
                # Histories can grow across repeated explicit resumes. Populate
                # real persisted pages without running 1,500 workflows.
                with store.connection(True) as db:
                    db.execute('DELETE FROM events WHERE run_id=?', (run_id,))
                    for _ in range(1500):
                        Store.event(db, run_id, 'RUN_SUCCEEDED')
                history = client.get('/api/runs/' + run_id + '/events', headers=headers)
                self.assertEqual(history.status_code, 200)
                events = [json.loads(line[6:]) for line in history.text.splitlines() if line.startswith('data: ')]
                self.assertEqual([item['seq'] for item in events], list(range(1, 1501)))

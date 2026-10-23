"""Token lifecycle and tenant administration use real SQLite and HTTP routes."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from flowtrail.app import create_app
from flowtrail.config import Settings
from flowtrail.store import Store, StoreError


class TokenLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(db_path=Path(self.tmp.name) / 'tokens.sqlite3', poll_seconds=0.01)
        self.store = Store(self.settings)
        self.admin = self.store.issue_token('alpha', 'admin')
        self.operator = self.store.issue_token('alpha', 'operator')
        self.viewer = self.store.issue_token('alpha', 'viewer')
        self.other = self.store.issue_token('beta', 'admin')
        self.context = TestClient(create_app(self.settings))
        self.client = self.context.__enter__()

    def tearDown(self):
        self.context.__exit__(None, None, None)
        self.tmp.cleanup()

    def headers(self, token=None):
        return {'Authorization': 'Bearer ' + (token or self.admin)}

    def issue(self, role='operator', ttl=3600):
        response = self.client.post('/api/tokens', json={'role': role, 'ttl_seconds': ttl}, headers=self.headers())
        self.assertEqual(response.status_code, 201, response.text)
        return response.json()

    def test_expired_token_is_rejected_at_authentication(self):
        # A token created long ago must not authenticate even if its digest exists.
        with patch('flowtrail.store.now', return_value='2000-01-01T00:00:00.000+00:00'):
            expired = self.store.issue_token('alpha', 'viewer')
        response = self.client.get('/api/me', headers=self.headers(expired))
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.headers['www-authenticate'], 'Bearer')

    def test_admin_can_issue_revoke_and_audit_without_persisting_secrets(self):
        issued = self.issue()
        secret, token_id = issued['token'], issued['id']
        self.assertEqual(issued['role'], 'operator')
        self.assertIn('expires_at', issued)
        self.assertEqual(self.client.get('/api/me', headers=self.headers(secret)).status_code, 200)
        listing = self.client.get('/api/tokens', headers=self.headers())
        self.assertEqual(listing.status_code, 200)
        self.assertNotIn(secret, listing.text)
        self.assertNotIn(hashlib.sha256(secret.encode()).hexdigest(), listing.text)
        self.assertIn(token_id, [item['id'] for item in listing.json()['items']])
        revoked = self.client.delete('/api/tokens/' + token_id, headers=self.headers())
        self.assertEqual(revoked.status_code, 204)
        self.assertEqual(self.client.get('/api/me', headers=self.headers(secret)).status_code, 401)
        audit = self.client.get('/api/audit', headers=self.headers())
        self.assertEqual(audit.status_code, 200)
        actions = [item['action'] for item in audit.json()['items'] if item['target_token_id'] == token_id]
        self.assertEqual(sorted(actions), ['TOKEN_ISSUED', 'TOKEN_REVOKED'])
        self.assertNotIn(secret, audit.text)
        self.assertNotIn(hashlib.sha256(secret.encode()).hexdigest(), audit.text)
        with self.store.connection() as db:
            for table in ('tokens', 'audit_log'):
                serialized = json.dumps([dict(row) for row in db.execute('SELECT * FROM ' + table)])
                self.assertNotIn(secret, serialized)

    def test_operator_business_writes_but_only_admin_manages_tokens_and_audit(self):
        for token in (self.operator, self.viewer):
            for path in ('/api/tokens', '/api/audit'):
                self.assertEqual(self.client.get(path, headers=self.headers(token)).status_code, 403)
            self.assertEqual(self.client.post('/api/tokens', json={'role': 'viewer'}, headers=self.headers(token)).status_code, 403)
            self.assertEqual(self.client.delete('/api/tokens/' + 'a' * 32, headers=self.headers(token)).status_code, 403)
        self.assertEqual(self.client.put('/api/objects/demo', content=b'one', headers=self.headers(self.operator)).status_code, 201)
        self.assertEqual(self.client.put('/api/objects/demo', content=b'two', headers=self.headers(self.viewer)).status_code, 403)

    def test_tenant_admin_cannot_list_revoke_or_create_another_tenants_token(self):
        foreign = self.store.authenticate(self.other)['token_id']
        response = self.client.get('/api/tokens', headers=self.headers())
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(foreign, [item['id'] for item in response.json()['items']])
        self.assertEqual(self.client.delete('/api/tokens/' + foreign, headers=self.headers()).status_code, 404)
        self.assertEqual(self.client.post('/api/tokens', json={'role': 'viewer', 'tenant_id': 'beta'}, headers=self.headers()).status_code, 422)
        self.assertEqual(self.client.get('/api/me', headers=self.headers(self.other)).status_code, 200)
        self.assertTrue(all(item['tenant_id'] == 'alpha' for item in self.client.get('/api/audit', headers=self.headers()).json()['items']))

    def test_revocation_is_idempotent_and_not_logged_twice(self):
        issued = self.issue('viewer')
        for _ in range(2):
            self.assertEqual(self.client.delete('/api/tokens/' + issued['id'], headers=self.headers()).status_code, 204)
        records = self.client.get('/api/audit', headers=self.headers()).json()['items']
        self.assertEqual(sum(item['action'] == 'TOKEN_REVOKED' and item['target_token_id'] == issued['id'] for item in records), 1)

    def test_ttl_validation_and_rejected_input_never_echoes_secrets(self):
        for ttl in (0, -1, True, 1.2, '60', 2592001):
            response = self.client.post('/api/tokens', json={'role': 'viewer', 'ttl_seconds': ttl}, headers=self.headers())
            self.assertEqual(response.status_code, 422)
        response = self.client.post('/api/tokens', json={'role': 'viewer', 'private': 'DO_NOT_ECHO'}, headers=self.headers())
        self.assertEqual(response.status_code, 422)
        self.assertNotIn('DO_NOT_ECHO', response.text)
        for ttl in (1, 2592000):
            issued = self.issue('viewer', ttl)
            self.assertEqual(issued['role'], 'viewer')
        for ttl in (0, True, 2592001):
            with self.assertRaises(ValueError):
                self.store.issue_token('alpha', 'viewer', ttl_seconds=ttl)

    def test_revoked_admin_cannot_mutate_after_an_earlier_authentication(self):
        actor = self.store.authenticate(self.admin)
        second_admin = self.issue('admin')['token']
        second_actor = self.store.authenticate(second_admin)
        self.store.revoke_token(second_actor, actor['token_id'])
        with self.assertRaises(StoreError) as captured:
            self.store.issue_managed_token(actor, 'admin')
        self.assertEqual(captured.exception.status, 401)
        self.assertEqual(self.client.post('/api/tokens', json={'role': 'admin'}, headers=self.headers()).status_code, 401)

    def test_audit_cursor_pages_are_scoped_and_disjoint(self):
        for _ in range(103):
            self.store.issue_token('alpha', 'viewer')
        first = self.client.get('/api/audit', headers=self.headers()).json()['items']
        second = self.client.get('/api/audit', params={'before': first[-1]['id']}, headers=self.headers()).json()['items']
        self.assertEqual(len(first), 100)
        self.assertTrue(second)
        self.assertTrue({item['id'] for item in first}.isdisjoint(item['id'] for item in second))
        self.assertTrue(all(item['tenant_id'] == 'alpha' for item in first + second))
        self.assertEqual(self.client.get('/api/audit?before=0', headers=self.headers()).status_code, 422)

    def test_expiry_boundary_rejects_token_without_extending_its_lifetime(self):
        with patch('flowtrail.store.now', return_value='2000-01-01T00:00:00.000+00:00'):
            token = self.store.issue_token('alpha', 'viewer', ttl_seconds=1)
        with patch('flowtrail.store.now', return_value='2000-01-01T00:00:00.999+00:00'):
            self.assertIsNotNone(self.store.authenticate(token))
        with patch('flowtrail.store.now', return_value='2000-01-01T00:00:01.000+00:00'):
            self.assertIsNone(self.store.authenticate(token))


class LegacyTokenMigrationTests(unittest.TestCase):
    def test_old_token_keeps_identity_and_gains_finite_expiry_on_original_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'legacy.sqlite3'
            token = 'legacy-fixture-not-a-real-credential'
            digest = hashlib.sha256(token.encode()).hexdigest()
            with closing(sqlite3.connect(path)) as db:
                with db:
                    db.executescript("CREATE TABLE tenants(id TEXT PRIMARY KEY,quota_bytes INTEGER NOT NULL);"
                                     "CREATE TABLE tokens(digest TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,role TEXT NOT NULL,created_at TEXT NOT NULL);")
                    db.execute('INSERT INTO tenants VALUES(?,?)', ('legacy', 1024))
                    db.execute('INSERT INTO tokens VALUES(?,?,?,?)', (digest, 'legacy', 'viewer', '2000-01-01T00:00:00.000+00:00'))
            store = Store(Settings(db_path=path))
            self.assertIsNone(store.authenticate(token))
            with store.connection() as db:
                row = dict(db.execute('SELECT * FROM tokens').fetchone())
                self.assertEqual(row['expires_at'], '2000-01-02T00:00:00.000+00:00')
                self.assertEqual(len(row['token_id']), 32)
                token_id = row['token_id']
            Store(Settings(db_path=path))
            with store.connection() as db:
                self.assertEqual(db.execute('SELECT token_id FROM tokens').fetchone()[0], token_id)


if __name__ == '__main__':
    unittest.main()

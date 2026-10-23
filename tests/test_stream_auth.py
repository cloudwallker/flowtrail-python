from __future__ import annotations

import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import httpx

from flowtrail.app import create_app
from flowtrail.config import Settings


class StreamAuthenticationTests(IsolatedAsyncioTestCase):
    async def test_revocation_between_history_batches_emits_explicit_error(self):
        await self.check_invalid_authentication("revoked")

    async def test_expiry_between_history_batches_emits_explicit_error(self):
        await self.check_invalid_authentication("expired")

    async def check_invalid_authentication(self, mode):
        with tempfile.TemporaryDirectory() as temporary:
            app = create_app(Settings(db_path=Path(temporary) / "history.sqlite3"))
            store = app.state.store
            admin = store.issue_token("demo", "admin")
            viewer = store.issue_token("demo", "viewer")
            actor = store.authenticate(admin)
            token_id = store.authenticate(viewer)["token_id"]
            run = store.submit("demo", {"workflow": {"name": "history", "nodes": [
                {"id": "one", "type": "text", "depends_on": [], "config": {"template": "hello"}}]},
                "inputs": {}}, "stream-auth-history")
            with store.connection(True) as db:
                for _ in range(529):
                    store.event(db, run["id"], "RUN_RESUMED")
            original_authenticate = store.authenticate
            checks = 0

            def invalidate_before_second_batch(token):
                nonlocal checks
                if token == viewer:
                    checks += 1
                    # Authentication #1 is the request dependency, #2 permits
                    # the first 512 records. The next batch sees real revocation
                    # or the real expiry predicate evaluated with a future clock.
                    if checks == 3:
                        if mode == "revoked":
                            store.revoke_token(actor, token_id)
                        else:
                            with patch("flowtrail.store.now", return_value="2100-01-01T00:00:00.000+00:00"):
                                return original_authenticate(token)
                return original_authenticate(token)

            store.authenticate = invalidate_before_second_batch
            try:
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                    response = await client.get("/api/runs/" + run["id"] + "/events?follow=false",
                                                headers={"Authorization": "Bearer " + viewer})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.text.count("\nid: ") + response.text.startswith("id: "), 512)
                self.assertTrue(response.text.endswith('event: error\ndata: {"code":"authentication_invalid"}\n\n'))
                self.assertNotIn(viewer, response.text)
                self.assertNotIn(admin, response.text)
                with store.connection() as db:
                    count = db.execute("SELECT COUNT(*) FROM events WHERE run_id=?", (run["id"],)).fetchone()[0]
                self.assertEqual(count, 530)
                if mode == "revoked":
                    self.assertIsNone(original_authenticate(viewer))
            finally:
                await app.state.db.close()

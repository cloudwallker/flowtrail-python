from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from .config import Settings


class StoreError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def encode(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


class Store:
    def __init__(self, settings: Settings):
        self.settings = settings
        settings.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS tenants(
                    id TEXT PRIMARY KEY, quota_bytes INTEGER NOT NULL CHECK(quota_bytes>0));
                CREATE TABLE IF NOT EXISTS tokens(
                    digest TEXT PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(id),
                    role TEXT NOT NULL CHECK(role IN ('admin','operator','viewer')), created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS runs(
                    id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(id),
                    name TEXT NOT NULL, status TEXT NOT NULL, definition TEXT NOT NULL,
                    idempotency_key TEXT, request_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, error TEXT,
                    UNIQUE(tenant_id,idempotency_key));
                CREATE INDEX IF NOT EXISTS runs_tenant ON runs(tenant_id,created_at);
                CREATE TABLE IF NOT EXISTS nodes(
                    run_id TEXT NOT NULL REFERENCES runs(id), id TEXT NOT NULL,
                    position INTEGER NOT NULL, type TEXT NOT NULL, status TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 0, output TEXT, error TEXT,
                    started_at TEXT, finished_at TEXT, PRIMARY KEY(run_id,id));
                CREATE TABLE IF NOT EXISTS events(
                    run_id TEXT NOT NULL REFERENCES runs(id), seq INTEGER NOT NULL,
                    type TEXT NOT NULL, node_id TEXT, attempt INTEGER, data TEXT NOT NULL,
                    created_at TEXT NOT NULL, PRIMARY KEY(run_id,seq));
                CREATE TABLE IF NOT EXISTS objects(
                    tenant_id TEXT NOT NULL REFERENCES tenants(id), name TEXT NOT NULL,
                    content BLOB NOT NULL, size_bytes INTEGER NOT NULL, sha256 TEXT NOT NULL,
                    updated_at TEXT NOT NULL, PRIMARY KEY(tenant_id,name));
            """)

    @contextmanager
    def connection(self, write=False):
        db = sqlite3.connect(self.settings.db_path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=10000")
        # FULL makes each committed checkpoint durable subject to filesystem/hardware guarantees.
        db.execute("PRAGMA synchronous=FULL")
        try:
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.commit()
        except BaseException:
            if write:
                db.rollback()
            raise
        finally:
            db.close()

    def issue_token(self, tenant_id: str, role: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", tenant_id):
            raise ValueError("Invalid tenant identifier")
        if role not in {"admin", "operator", "viewer"}:
            raise ValueError("Invalid role")
        token = secrets.token_urlsafe(32)
        with self.connection(True) as db:
            db.execute("INSERT OR IGNORE INTO tenants VALUES(?,?)", (tenant_id, self.settings.quota_bytes))
            db.execute("INSERT INTO tokens VALUES(?,?,?,?)",
                       (hashlib.sha256(token.encode()).hexdigest(), tenant_id, role, now()))
        return token

    def authenticate(self, token: str):
        if len(token) > 256:
            return None
        digest = hashlib.sha256(token.encode()).hexdigest()
        with self.connection() as db:
            row = db.execute("SELECT tenant_id,role FROM tokens WHERE digest=?", (digest,)).fetchone()
            return dict(row) if row else None

    @staticmethod
    def event(db, run_id, event_type, node_id=None, attempt=None, data=None):
        seq = db.execute("SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE run_id=?", (run_id,)).fetchone()[0]
        db.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?)",
                   (run_id, seq, event_type, node_id, attempt, encode(data or {}), now()))

    @staticmethod
    def require_run(db, tenant_id, run_id):
        row = db.execute("SELECT * FROM runs WHERE id=? AND tenant_id=?", (run_id, tenant_id)).fetchone()
        if row is None:
            raise StoreError(404, "Run not found")
        return row

    def submit(self, tenant_id, definition: dict, idempotency_key: str | None):
        serialized = encode(definition)
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        with self.connection(True) as db:
            if idempotency_key is not None:
                row = db.execute("SELECT id,request_hash FROM runs WHERE tenant_id=? AND idempotency_key=?",
                                 (tenant_id, idempotency_key)).fetchone()
                if row:
                    if row["request_hash"] != digest:
                        raise StoreError(409, "Idempotency key already used with a different request")
                    run_id = row["id"]
                    return self._detail(db, tenant_id, run_id)
            count = db.execute("SELECT COUNT(*) FROM runs WHERE status IN ('queued','running')").fetchone()[0]
            if count >= self.settings.max_pending_runs:
                raise StoreError(429, "The run queue is full; retry later")
            run_id = uuid.uuid4().hex
            stamp = now()
            db.execute("INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?,NULL)",
                       (run_id, tenant_id, definition["workflow"]["name"], "queued", serialized,
                        idempotency_key, digest, stamp, stamp))
            for position, node in enumerate(definition["workflow"]["nodes"]):
                db.execute("INSERT INTO nodes(run_id,id,position,type,status) VALUES(?,?,?,?, 'pending')",
                           (run_id, node["id"], position, node["type"]))
            self.event(db, run_id, "RUN_QUEUED")
            return self._detail(db, tenant_id, run_id)

    def list_runs(self, tenant_id):
        with self.connection() as db:
            return [dict(row) for row in db.execute(
                "SELECT id,name,status,created_at,updated_at,error FROM runs WHERE tenant_id=? ORDER BY created_at DESC,id DESC LIMIT 100",
                (tenant_id,))]

    def _detail(self, db, tenant_id, run_id):
        row = self.require_run(db, tenant_id, run_id)
        result = {key: row[key] for key in ("id", "name", "status", "created_at", "updated_at", "error")}
        result.update(json.loads(row["definition"]))
        result["nodes"] = [dict(item) for item in db.execute(
            "SELECT id,type,status,attempt,output,error,started_at,finished_at FROM nodes WHERE run_id=? ORDER BY position", (run_id,))]
        return result

    def get_run(self, tenant_id, run_id):
        with self.connection() as db:
            return self._detail(db, tenant_id, run_id)

    def resume(self, tenant_id, run_id):
        with self.connection(True) as db:
            run = self.require_run(db, tenant_id, run_id)
            if run["status"] not in {"failed", "interrupted"}:
                raise StoreError(409, "Only failed or interrupted runs can be resumed")
            count = db.execute("SELECT COUNT(*) FROM runs WHERE status IN ('queued','running')").fetchone()[0]
            if count >= self.settings.max_pending_runs:
                raise StoreError(429, "The run queue is full; retry later")
            db.execute("UPDATE nodes SET status='pending',output=NULL,error=NULL,started_at=NULL,finished_at=NULL WHERE run_id=? AND status!='succeeded'", (run_id,))
            db.execute("UPDATE runs SET status='queued',error=NULL,updated_at=? WHERE id=?", (now(), run_id))
            self.event(db, run_id, "RUN_RESUMED")
            return self._detail(db, tenant_id, run_id)

    def recover(self):
        """Only called after acquiring the exclusive process lock."""
        with self.connection(True) as db:
            for run in db.execute("SELECT id FROM runs WHERE status IN ('running','interrupted')").fetchall():
                run_id = run["id"]
                db.execute("UPDATE nodes SET status='pending',output=NULL,error=NULL,started_at=NULL,finished_at=NULL WHERE run_id=? AND status!='succeeded'", (run_id,))
                db.execute("UPDATE runs SET status='queued',error=NULL,updated_at=? WHERE id=?", (now(), run_id))
                self.event(db, run_id, "RUN_RECOVERED")

    def queued(self, limit):
        with self.connection() as db:
            return [dict(row) for row in db.execute("SELECT id,tenant_id FROM runs WHERE status='queued' ORDER BY created_at,id LIMIT ?", (limit,))]

    def start_run(self, run_id):
        with self.connection(True) as db:
            changed = db.execute("UPDATE runs SET status='running',updated_at=? WHERE id=? AND status='queued'", (now(), run_id)).rowcount
            if changed:
                self.event(db, run_id, "RUN_STARTED")
            return bool(changed)

    def start_node(self, run_id, node_id):
        with self.connection(True) as db:
            db.execute("UPDATE nodes SET status='running',attempt=attempt+1,started_at=?,finished_at=NULL,error=NULL WHERE run_id=? AND id=? AND status='pending'", (now(), run_id, node_id))
            attempt = db.execute("SELECT attempt FROM nodes WHERE run_id=? AND id=?", (run_id, node_id)).fetchone()[0]
            self.event(db, run_id, "NODE_STARTED", node_id, attempt)
            return attempt

    def finish_node(self, run_id, node_id, status, output=None, error=None):
        with self.connection(True) as db:
            attempt = db.execute("SELECT attempt FROM nodes WHERE run_id=? AND id=?", (run_id, node_id)).fetchone()[0]
            db.execute("UPDATE nodes SET status=?,output=?,error=?,finished_at=? WHERE run_id=? AND id=?",
                       (status, output, error, now(), run_id, node_id))
            db.execute("UPDATE runs SET updated_at=? WHERE id=?", (now(), run_id))
            self.event(db, run_id, "NODE_" + status.upper(), node_id, attempt,
                       {"output": output} if status == "succeeded" else {"error": error})

    def finish_run(self, run_id, status, error=None):
        with self.connection(True) as db:
            db.execute("UPDATE runs SET status=?,error=?,updated_at=? WHERE id=?", (status, error, now(), run_id))
            if status == "failed":
                db.execute("UPDATE nodes SET status='skipped',error='Stopped after node failure' WHERE run_id=? AND status='pending'", (run_id,))
            if status == "interrupted":
                db.execute("UPDATE nodes SET status='interrupted',error='Server stopped during execution',finished_at=? WHERE run_id=? AND status='running'", (now(), run_id))
            self.event(db, run_id, "RUN_" + status.upper(), data={"error": error} if error else {})

    def events(self, tenant_id, run_id, after=0):
        with self.connection() as db:
            self.require_run(db, tenant_id, run_id)
            result = []
            for row in db.execute("SELECT seq,type,node_id,attempt,data,created_at FROM events WHERE run_id=? AND seq>? ORDER BY seq LIMIT 512", (run_id, after)):
                item = dict(row)
                item["data"] = json.loads(item["data"])
                result.append(item)
            return result

    def quota(self, tenant_id):
        with self.connection() as db:
            limit = db.execute("SELECT quota_bytes FROM tenants WHERE id=?", (tenant_id,)).fetchone()[0]
            used = db.execute("SELECT COALESCE(SUM(size_bytes),0) FROM objects WHERE tenant_id=?", (tenant_id,)).fetchone()[0]
            return {"used_bytes": used, "limit_bytes": limit, "remaining_bytes": limit - used}

    def list_objects(self, tenant_id):
        with self.connection() as db:
            return [dict(row) for row in db.execute("SELECT name,size_bytes,sha256,updated_at FROM objects WHERE tenant_id=? ORDER BY name", (tenant_id,))]

    def put_object(self, tenant_id, name, content):
        digest = hashlib.sha256(content).hexdigest()
        stamp = now()
        with self.connection(True) as db:
            limit = db.execute("SELECT quota_bytes FROM tenants WHERE id=?", (tenant_id,)).fetchone()[0]
            used = db.execute("SELECT COALESCE(SUM(size_bytes),0) FROM objects WHERE tenant_id=? AND name!=?", (tenant_id, name)).fetchone()[0]
            if used + len(content) > limit:
                raise StoreError(413, "Tenant object quota exceeded")
            db.execute("INSERT INTO objects VALUES(?,?,?,?,?,?) ON CONFLICT(tenant_id,name) DO UPDATE SET content=excluded.content,size_bytes=excluded.size_bytes,sha256=excluded.sha256,updated_at=excluded.updated_at",
                       (tenant_id, name, content, len(content), digest, stamp))
        return {"name": name, "size_bytes": len(content), "sha256": digest, "updated_at": stamp}

    def get_object(self, tenant_id, name):
        with self.connection() as db:
            row = db.execute("SELECT content,sha256 FROM objects WHERE tenant_id=? AND name=?", (tenant_id, name)).fetchone()
            if row is None:
                raise StoreError(404, "Object not found")
            return bytes(row["content"]), row["sha256"]

    def delete_object(self, tenant_id, name):
        with self.connection(True) as db:
            if not db.execute("DELETE FROM objects WHERE tenant_id=? AND name=?", (tenant_id, name)).rowcount:
                raise StoreError(404, "Object not found")

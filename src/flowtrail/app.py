import asyncio
import re
from contextlib import asynccontextmanager
from typing import Annotated
from urllib.parse import parse_qs

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles

from .config import Settings
from .models import SubmitRun
from .runtime import Runtime
from .store import Store, StoreError, encode


class BodyLimit:
    def __init__(self, app, settings):
        self.app, self.settings = app, settings

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        query = parse_qs(scope.get("query_string", b"").decode("latin-1"), keep_blank_values=True)
        if any(key in query for key in ("tenant", "tenant_id")):
            await JSONResponse({"detail": "Tenant identity comes from the access token"}, status_code=422)(scope, receive, send)
            return
        limit = self.settings.max_object_bytes if scope["path"].startswith("/api/objects/") else self.settings.max_request_bytes
        parts, size = [], 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > limit:
                await JSONResponse({"detail": "Request body exceeds the configured byte limit"}, status_code=413)(scope, receive, send)
                return
            parts.append(chunk)
            if not message.get("more_body", False):
                break
        delivered = False

        async def buffered_receive():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": b"".join(parts), "more_body": False}
            return await receive()
        await self.app(scope, buffered_receive, send)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    store = Store(settings)
    runtime = Runtime(store, settings)

    @asynccontextmanager
    async def lifespan(app):
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(title="FlowTrail Python", version="0.1.0", lifespan=lifespan)
    app.add_middleware(BodyLimit, settings=settings)
    app.state.store, app.state.runtime = store, runtime
    bearer = HTTPBearer(auto_error=False)

    async def identity(credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]):
        current = store.authenticate(credentials.credentials) if credentials else None
        if current is None:
            raise HTTPException(401, "A valid bearer token is required", headers={"WWW-Authenticate": "Bearer"})
        return current

    async def writer(current: Annotated[dict, Depends(identity)]):
        if current["role"] not in {"admin", "operator"}:
            raise HTTPException(403, "The viewer role is read-only")
        return current

    @app.exception_handler(StoreError)
    async def store_error(request, error):
        return JSONResponse({"detail": str(error)}, status_code=error.status)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, error):
        # Pydantic's default response echoes rejected input; omit it (and the request body).
        return JSONResponse({"detail": [{"loc": list(item["loc"]), "msg": item["msg"], "type": item["type"]}
                                         for item in error.errors()]}, status_code=422)

    @app.get("/healthz")
    def health():
        return {"status": "ok", "service": "flowtrail-python"}

    @app.get("/api/me")
    def me(current: Annotated[dict, Depends(identity)]):
        return current

    @app.get("/api/runs")
    def list_runs(current: Annotated[dict, Depends(identity)]):
        return {"items": store.list_runs(current["tenant_id"])}

    @app.post("/api/runs", status_code=202)
    def submit(body: SubmitRun, current: Annotated[dict, Depends(writer)],
               idempotency_key: Annotated[str | None, Header()] = None):
        if idempotency_key is not None and not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", idempotency_key):
            raise HTTPException(422, "Invalid Idempotency-Key format")
        for node in body.workflow.nodes:
            if node.type == "http" and node.config["connector"] not in settings.connectors:
                raise HTTPException(422, "HTTP node requires a configured server connector")
        result = store.submit(current["tenant_id"], body.model_dump(), idempotency_key)
        return JSONResponse(result, status_code=202, headers={"Location": "/api/runs/" + result["id"]})

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str, current: Annotated[dict, Depends(identity)]):
        return store.get_run(current["tenant_id"], run_id)

    @app.post("/api/runs/{run_id}/resume", status_code=202)
    def resume(run_id: str, current: Annotated[dict, Depends(writer)]):
        return store.resume(current["tenant_id"], run_id)

    @app.get("/api/runs/{run_id}/events")
    async def events(run_id: str, request: Request, current: Annotated[dict, Depends(identity)],
                     follow: bool = True, last_event_id: Annotated[str | None, Header()] = None):
        store.get_run(current["tenant_id"], run_id)
        if last_event_id is not None and (not last_event_id.isdigit() or len(last_event_id) > 18):
            raise HTTPException(422, "Last-Event-ID must be a nonnegative event sequence")
        cursor = int(last_event_id or "0")

        async def stream():
            nonlocal cursor
            for _ in range(600):  # Follow connections have a bounded one-minute lifetime.
                if await request.is_disconnected():
                    return
                batch = store.events(current["tenant_id"], run_id, cursor)
                for event in batch:
                    cursor = event["seq"]
                    yield f"id: {cursor}\nevent: {event['type']}\ndata: {encode(event)}\n\n"
                if not follow:
                    if len(batch) < 512:
                        return
                    continue
                # Reconnect with the cursor to observe an explicit future resume.
                if store.get_run(current["tenant_id"], run_id)["status"] in {"succeeded", "failed", "interrupted"}:
                    # Flush events committed between the first event query and terminal observation.
                    for event in store.events(current["tenant_id"], run_id, cursor):
                        cursor = event["seq"]
                        yield f"id: {cursor}\nevent: {event['type']}\ndata: {encode(event)}\n\n"
                    return
                if not batch:
                    yield ": keepalive\n\n"
                await asyncio.sleep(0.1)

        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

    @app.get("/api/quota")
    def quota(current: Annotated[dict, Depends(identity)]):
        return store.quota(current["tenant_id"])

    @app.get("/api/objects")
    def objects(current: Annotated[dict, Depends(identity)]):
        return {"items": store.list_objects(current["tenant_id"])}

    def object_name(name):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", name):
            raise HTTPException(422, "Object names must use ASCII letters, digits, dot, dash or underscore")

    @app.put("/api/objects/{name}", status_code=201)
    async def put_object(name: str, request: Request, current: Annotated[dict, Depends(writer)]):
        object_name(name)
        return store.put_object(current["tenant_id"], name, await request.body())

    @app.get("/api/objects/{name}")
    def get_object(name: str, current: Annotated[dict, Depends(identity)]):
        object_name(name)
        content, digest = store.get_object(current["tenant_id"], name)
        return Response(content, media_type="application/octet-stream", headers={
            "Content-Disposition": f'attachment; filename="{name}"', "ETag": '"' + digest + '"',
            "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"})

    @app.delete("/api/objects/{name}", status_code=204)
    def delete_object(name: str, current: Annotated[dict, Depends(writer)]):
        object_name(name)
        store.delete_object(current["tenant_id"], name)
        return Response(status_code=204)

    if settings.frontend_dir.is_dir() and (settings.frontend_dir / "index.html").is_file():
        assets = settings.frontend_dir / "assets"
        if assets.is_dir():
            app.mount("/assets", StaticFiles(directory=assets), name="assets")

        @app.get("/")
        def index():
            return FileResponse(settings.frontend_dir / "index.html")

    return app

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit


def validate_url(value: str) -> str:
    parsed = urlsplit(value)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username or parsed.password or parsed.fragment):
        raise ValueError("Server endpoint must be an HTTP(S) URL without credentials or fragments")
    return value


@dataclass(frozen=True)
class Settings:
    db_path: Path = Path("data/flowtrail.sqlite3")
    quota_bytes: int = 10 * 1024 * 1024
    max_object_bytes: int = 5 * 1024 * 1024
    max_request_bytes: int = 256 * 1024
    max_output_bytes: int = 256 * 1024
    max_pending_runs: int = 100
    max_parallel_runs: int = 4
    max_parallel_nodes: int = 8
    per_run_parallelism: int = 4
    poll_seconds: float = 0.1
    connectors: dict[str, dict] = field(default_factory=dict)
    model_base_url: str | None = None
    model_name: str | None = None
    model_api_key: str | None = field(default=None, repr=False)
    frontend_dir: Path = Path("frontend/dist")

    def __post_init__(self):
        object.__setattr__(self, "db_path", Path(self.db_path))
        object.__setattr__(self, "frontend_dir", Path(self.frontend_dir))
        for name in ("quota_bytes", "max_object_bytes", "max_request_bytes", "max_output_bytes",
                     "max_pending_runs", "max_parallel_runs", "max_parallel_nodes", "per_run_parallelism"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        if not 0.005 <= self.poll_seconds <= 10:
            raise ValueError("poll_seconds must be between 0.005 and 10")
        for name, connector in self.connectors.items():
            if not isinstance(name, str) or not isinstance(connector, dict) or set(connector) - {"url", "headers"}:
                raise ValueError("Invalid server connector configuration")
            validate_url(connector.get("url", ""))
            headers = connector.get("headers", {})
            if not isinstance(headers, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in headers.items()):
                raise ValueError("Connector headers must be a string mapping")
        if self.model_base_url:
            validate_url(self.model_base_url)

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=Path(os.getenv("FLOWTRAIL_DB", "data/flowtrail.sqlite3")),
            quota_bytes=int(os.getenv("FLOWTRAIL_QUOTA_BYTES", str(10 * 1024 * 1024))),
            max_object_bytes=int(os.getenv("FLOWTRAIL_MAX_OBJECT_BYTES", str(5 * 1024 * 1024))),
            max_pending_runs=int(os.getenv("FLOWTRAIL_MAX_PENDING_RUNS", "100")),
            max_parallel_runs=int(os.getenv("FLOWTRAIL_MAX_PARALLEL_RUNS", "4")),
            max_parallel_nodes=int(os.getenv("FLOWTRAIL_MAX_PARALLEL_NODES", "8")),
            per_run_parallelism=int(os.getenv("FLOWTRAIL_PER_RUN_PARALLELISM", "4")),
            connectors=json.loads(os.getenv("FLOWTRAIL_CONNECTORS_JSON", "{}")),
            model_base_url=os.getenv("FLOWTRAIL_MODEL_BASE_URL"),
            model_name=os.getenv("FLOWTRAIL_MODEL_NAME"),
            model_api_key=os.getenv("FLOWTRAIL_MODEL_API_KEY"),
            frontend_dir=Path(os.getenv("FLOWTRAIL_FRONTEND_DIR", "frontend/dist")),
        )

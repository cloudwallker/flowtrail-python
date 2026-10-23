from __future__ import annotations

import argparse
import sys

from .config import Settings
from .store import DEFAULT_TOKEN_TTL, Store


def main() -> int:
    parser = argparse.ArgumentParser(description="FlowTrail single-process AI task platform")
    commands = parser.add_subparsers(dest="command", required=True)
    bootstrap = commands.add_parser("bootstrap", help="Issue one random tenant access token; printed once")
    bootstrap.add_argument("--tenant", required=True)
    bootstrap.add_argument("--role", choices=["admin", "operator", "viewer"], default="admin")
    bootstrap.add_argument('--ttl-seconds', type=int, default=DEFAULT_TOKEN_TTL,
                           help='Token lifetime in seconds (1 to 2592000; default 86400)')
    serve = commands.add_parser("serve", help="Run one server process for this SQLite database")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    try:
        settings = Settings.from_env()
        if args.command == "bootstrap":
            token = Store(settings).issue_token(args.tenant, args.role, ttl_seconds=args.ttl_seconds)
            print(f"Save this access token securely; it expires after {args.ttl_seconds} seconds. Only its SHA-256 hash is stored.", file=sys.stderr)
            print(token)
            return 0
        import uvicorn
        from .app import create_app
        uvicorn.run(create_app(settings), host=args.host, port=args.port, workers=1, access_log=False)
        return 0
    except (ValueError, OSError, RuntimeError):
        print("Configuration or startup failed; check the server environment and database ownership.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

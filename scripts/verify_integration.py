#!/usr/bin/env python
"""
Verify Turso connectivity using environment variables.

Usage:
  python scripts/verify_integration.py --sync
  python scripts/verify_integration.py --async
  python scripts/verify_integration.py            # runs both

This script does not print or log secrets. It only reports pass/fail statuses.
"""
import argparse
import os
import sys
from pathlib import Path

# Ensure project root is on sys.path for local imports when running from scripts/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SYNC_OK = None
ASYNC_OK = None


def check_env() -> list[str]:
    required = ["TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN"]
    return [k for k in required if not os.getenv(k)]


def run_sync() -> bool:
    try:
        from turso_python.connection import TursoConnection
        c = TursoConnection()
        r = c.execute_query("SELECT 1")
        return isinstance(r, dict) and "results" in r
    except Exception as e:
        print(f"SYNC_ERROR:{e}")
        return False


def run_async() -> bool:
    try:
        import anyio
        from turso_python.async_connection import AsyncTursoConnection
        from turso_python.async_crud import AsyncTursoCRUD  # exercise CRUD import too
    except Exception as e:
        print(f"ASYNC_IMPORT_ERROR:{e}")
        return False

    async def _main():
        try:
            async with AsyncTursoConnection() as conn:
                # minimal execute
                resp = await conn.execute_query("SELECT 1")
                return isinstance(resp, dict) and "results" in resp
        except Exception as e:
            print(f"ASYNC_ERROR:{e}")
            return False

    return anyio.run(_main)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sync", action="store_true", help="Run sync verification only")
    parser.add_argument("--async", dest="do_async", action="store_true", help="Run async verification only")
    args = parser.parse_args()

    missing = check_env()
    if missing:
        print("MISSING:" + ",".join(missing))
        sys.exit(0)

    global SYNC_OK, ASYNC_OK

    if args.sync:
        SYNC_OK = run_sync()
        print("SYNC_OK:" + ("true" if SYNC_OK else "false"))
        return

    if args.do_async:
        ASYNC_OK = run_async()
        print("ASYNC_OK:" + ("true" if ASYNC_OK else "false"))
        return

    # default: run both
    SYNC_OK = run_sync()
    ASYNC_OK = run_async()
    print("SYNC_OK:" + ("true" if SYNC_OK else "false"))
    print("ASYNC_OK:" + ("true" if ASYNC_OK else "false"))


if __name__ == "__main__":
    main()


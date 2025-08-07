#!/usr/bin/env python
"""
Basic CRUD smoke test against Turso using TursoConnection.
This script avoids printing secrets and uses environment variables for credentials.
"""
import os
import sys
import uuid
from pathlib import Path

# Ensure project root is on sys.path for local imports when running from scripts/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from turso_python.connection import TursoConnection  # noqa: E402


def _extract_rows(resp):
    try:
        return resp.get("results", [])[0].get("response", {}).get("result", {}).get("rows", [])
    except Exception:
        return []


def _cell_value(cell):
    # Cell may be dict with {type, value} or a raw value
    if isinstance(cell, dict):
        return cell.get("value")
    return cell


def main() -> int:
    missing = [k for k in ("TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN") if not os.getenv(k)]
    if missing:
        print("MISSING:" + ",".join(missing))
        return 0

    # Unique table per run to avoid conflicts
    suffix = uuid.uuid4().hex[:8]
    table = f"crud_demo_{suffix}"
    conn = TursoConnection()

    try:
        # Create table
        conn.execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id TEXT PRIMARY KEY,
                name TEXT,
                age INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        print("CREATE:ok")

        # Insert row
        row_id = uuid.uuid4().hex
        conn.execute_query(
            f"INSERT INTO {table} (id, name, age) VALUES (?, ?, ?)",
            [row_id, "Alice", 30],
        )
        print("INSERT:ok")

        # Read row
        res = conn.execute_query(
            f"SELECT id, name, age FROM {table} WHERE id = ?", [row_id]
        )
        rows = _extract_rows(res)
        print("SELECT1:rows=", len(rows))

        # Update row
        conn.execute_query(
            f"UPDATE {table} SET age = ? WHERE id = ?", [31, row_id]
        )
        print("UPDATE:ok")

        # Read after update
        res2 = conn.execute_query(
            f"SELECT age FROM {table} WHERE id = ?", [row_id]
        )
        rows2 = _extract_rows(res2)
        if rows2:
            first_row = rows2[0]
            # Row may be {"values": [...]} or a raw list
            if isinstance(first_row, dict) and "values" in first_row:
                cells = first_row["values"]
                new_age = _cell_value(cells[0]) if cells else "?"
            elif isinstance(first_row, list | tuple):
                new_age = _cell_value(first_row[0]) if first_row else "?"
            else:
                new_age = str(first_row)
        else:
            new_age = "?"
        print("SELECT2:age=", new_age)

        # Delete row
        conn.execute_query(
            f"DELETE FROM {table} WHERE id = ?", [row_id]
        )
        print("DELETE:ok")

        # Verify deletion
        res3 = conn.execute_query(
            f"SELECT COUNT(*) FROM {table} WHERE id = ?", [row_id]
        )
        rows3 = _extract_rows(res3)
        if rows3:
            first_row = rows3[0]
            if isinstance(first_row, dict) and "values" in first_row:
                cells = first_row["values"]
                remaining = _cell_value(cells[0]) if cells else "?"
            elif isinstance(first_row, list | tuple):
                remaining = _cell_value(first_row[0]) if first_row else "?"
            else:
                remaining = str(first_row)
        else:
            remaining = "?"
        print("VERIFY_DELETE:remaining=", remaining)

        # Drop table to clean up
        conn.execute_query(f"DROP TABLE IF EXISTS {table}")
        print("DROP:ok")

        print("CRUD_SMOKE:success")
        return 0
    except Exception as e:
        print("CRUD_SMOKE:error:", e)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())


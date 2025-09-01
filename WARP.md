# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

Project: TursoPy — a minimal Python client for Turso/libsql HTTP API with sync and async support.

Core commands (pwsh/bash compatible)
- Create a virtual environment (uv) and install
  - Create venv: uv venv
  - Prod deps only: uv pip install -e .
  - With dev extras (tests, lint): uv pip install -e .[dev]
- Lint
  - Ruff check: uv run ruff check .
- Tests (pytest)
  - Run all tests: uv run pytest -q
  - Run a single test file: uv run pytest -q tests/path/to/test_file.py
  - Run a single test node: uv run pytest -q tests/path/to/test_file.py::TestClass::test_case
- Build and validate artifacts
  - Build sdist+wheel: uv build
  - Validate metadata: uvx twine check dist/*
- Quick import sanity check (matches CI): uv run python -c "import turso_python; print('import_ok')"
- Environment configuration (required at runtime)
  - TURSO_DATABASE_URL (accepts libsql:// and is normalized to https://)
  - TURSO_AUTH_TOKEN

Tooling and config
- Build backend: hatchling (pyproject.toml). Do not use setup.py (it intentionally exits).
- Lint: Ruff (configured via pyproject: line-length 100; lint rules E,F,I,UP; ignores E501).
- Tests: Pytest with asyncio_mode=auto and -q addopts in pyproject.
- CI (GitHub Actions): Matrix on Python 3.10–3.13. Steps: uv venv → install prod deps → import-check → install dev deps → uv build → twine check → ruff check → pytest.

High-level architecture
- Package: turso_python/
  - Connection layers
    - connection.TursoConnection (sync):
      - Normalizes libsql:// → https://, strips /v2/pipeline suffix, enforces https.
      - Uses requests.Session with optional retries/backoff.
      - Methods: execute_query(sql, args), batch(queries), execute_pipeline(queries), close.
      - Errors: raises TursoHTTPError or TursoRateLimitError (with optional retry_after).
    - async_connection.AsyncTursoConnection (async):
      - aiohttp ClientSession with fine-grained timeouts; optional retries/backoff using anyio sleep.
      - Methods mirror sync version: execute_query, execute_pipeline; context-managed session lifecycle.
  - CRUD and helpers
    - crud.TursoCRUD (sync, thin SQL helpers): create/read/update/delete using positional args.
    - crud.TursoClient / TursoSchemaManager / TursoDataManager: legacy/simple sync helpers for database creation, table DDL, and basic DML via HTTP pipeline.
    - async_crud.AsyncTursoCRUD: asynchronous CRUD API returning normalized results; includes helpers like read_all_rows/read_first_row/read_count and feature queries (join/aggregate/order/complex where), plus foreign-key PRAGMA toggles.
    - batch.TursoBatch: constructs and posts a pipeline of execute requests for bulk inserts.
  - Query result handling
    - response_parser.TursoResponseParser: converts raw Turso pipeline responses into a normalized dict { rows, columns, count } used by async CRUD.
    - result.Result: lightweight accessor for raw payloads with helpers like rows() and first_value().
  - Errors and logging
    - exceptions: TursoError base, TursoHTTPError, TursoRateLimitError.
    - logger.TursoLogger: simple logging hooks for query/response (uses logging.basicConfig).
  - Vectors
    - turso_vector.TursoVector: utilities for creating vector tables/indexes and performing vector operations (insert, top-k, cosine distance, extract/update, index maintenance). Uses sync connection.
  - Public API
    - __init__.py re-exports core classes. Async exports are optional: import attempts are guarded so missing aiohttp won’t hard-fail at import time.

Development notes specific to this repo
- Runtime credentials must be provided via environment variables or passed directly to connection constructors (no dotenv usage in code).
- The package name for distribution is "tursopy" (pyproject), but the import namespace is "turso_python".
- Hatch build config explicitly packages turso_python/ and includes README.md and LICENSE in sdist.

References in repo
- pyproject.toml: authoritative source for dependencies, lint, pytest, and build configuration.
- .github/workflows/ci.yml: shows the canonical CI steps and Python versions.
- README.md: feature overview and examples for sync/async CRUD, batch, advanced queries, schema management, and database creation.

Conventions for future Warp runs
- Prefer uv for all Python environment, run, and build steps.
- Use the sync connection for simple scripts and the async connection/CRUD for concurrent workloads.
- When shaping SQL args, pass plain Python values; the connection layer formats them for the Turso pipeline.


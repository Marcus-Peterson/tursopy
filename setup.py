# This project uses PEP 621 metadata in pyproject.toml and a PEP 517 build backend.
# setup.py is intentionally unused. Building should be done via:
#   uv build  (or)  python -m build
# Installing for development:
#   uv pip install -e .[dev]

raise SystemExit(
    "This project does not use setup.py. Use pyproject.toml with hatchling: "
    "build with 'uv build' or 'python -m build'."
)

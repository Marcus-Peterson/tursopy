# Package exports
from .advanced_queries import TursoAdvancedQueries
from .batch import TursoBatch
from .crud import (
    TursoClient,
    TursoSchemaManager,
    TursoDataManager,
    TursoCRUD,
)
from .schema_validator import SchemaValidator
from .logger import TursoLogger
from .turso_vector import TursoVector
from .connection import TursoConnection

# Optional async exports; do not hard-fail if aiohttp is not installed yet
try:
    from .async_connection import AsyncTursoConnection  # type: ignore
    from .async_crud import AsyncTursoCRUD  # type: ignore
    _ASYNC_AVAILABLE = True
except Exception:  # ImportError or runtime issues
    AsyncTursoConnection = None  # type: ignore
    AsyncTursoCRUD = None  # type: ignore
    _ASYNC_AVAILABLE = False

__all__ = [
    "TursoAdvancedQueries",
    "TursoBatch",
    "TursoClient",
    "TursoSchemaManager",
    "TursoDataManager",
    "TursoCRUD",
    "SchemaValidator",
    "TursoLogger",
    "TursoVector",
    "TursoConnection",
]
if _ASYNC_AVAILABLE:
    __all__ += [
        "AsyncTursoConnection",
        "AsyncTursoCRUD",
    ]

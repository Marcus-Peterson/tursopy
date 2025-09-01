# Package exports
from .advanced_queries import TursoAdvancedQueries
from .batch import TursoBatch
from .connection import TursoConnection
from .crud import (
    TursoClient,
    TursoCRUD,
    TursoDataManager,
    TursoSchemaManager,
)
from .exceptions import TursoError, TursoHTTPError, TursoRateLimitError
from .logger import TursoLogger
from .result import Result
from .schema_validator import SchemaValidator
from .turso_vector import TursoVector

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
    # Exceptions and result types
    "TursoError",
    "TursoHTTPError",
    "TursoRateLimitError",
    "Result",
]
if _ASYNC_AVAILABLE:
    __all__ += [
        "AsyncTursoConnection",
        "AsyncTursoCRUD",
    ]

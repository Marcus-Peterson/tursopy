class TursoError(Exception):
    """Base exception for Turso client errors."""


class TursoHTTPError(TursoError):
    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.message = message


class TursoRateLimitError(TursoHTTPError):
    def __init__(self, status: int, message: str, retry_after: float | None = None):
        super().__init__(status, message)
        self.retry_after = retry_after


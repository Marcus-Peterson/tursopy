# Handles Turso API communication (synchronous version).
# Note: For new development, prefer the asynchronous client in async_connection.py
# This module intentionally does not use python-dotenv. Provide credentials via
# environment variables or pass them explicitly.

import os
import requests
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Union
import random
import time
from .exceptions import TursoHTTPError, TursoRateLimitError


def _normalize_url(url: str) -> str:
    """Convert libsql:// to https://, strip trailing pipeline suffix, and validate. Enforce https."""
    if url.startswith('libsql://'):
        url = 'https://' + url[len('libsql://'):]
    # Remove any existing /v2/pipeline suffix and trailing slash
    url = url.rstrip('/').replace('/v2/pipeline', '')
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid database URL: {url}")
    if parsed.scheme != 'https':
        raise ValueError("Only https scheme is supported after normalization")
    return url


class TursoConnection:
    def __init__(
        self,
        database_url: Optional[str] = None,
        auth_token: Optional[str] = None,
        *,
        timeout: int = 30,
        retries: int = 0,
        backoff_base: float = 0.2,
        debug_sql: bool = False,
    ):
        env_url = os.getenv("TURSO_DATABASE_URL")
        env_token = os.getenv("TURSO_AUTH_TOKEN")
        if not (database_url or env_url):
            raise ValueError(
                "database_url not provided and TURSO_DATABASE_URL is not set"
            )
        if not (auth_token or env_token):
            raise ValueError(
                "auth_token not provided and TURSO_AUTH_TOKEN is not set"
            )

        self.database_url = _normalize_url(
            database_url or env_url
        )  # type: ignore[arg-type]
        self.auth_token = auth_token or env_token  # type: ignore[assignment]
        self.timeout = timeout
        self.retries = max(0, int(retries))
        self.backoff_base = float(backoff_base)
        self.debug_sql = bool(debug_sql)
        self.headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json',
        }
        # Use a persistent session for connection reuse and performance
        self.session = requests.Session()

    def execute_query(
        self, sql: str, args: Optional[Union[List[Any], tuple]] = None
    ) -> Dict[str, Any]:
        """Execute a single SQL statement with optional positional arguments."""
        payload = {
            'requests': [
                {
                    'type': 'execute',
                    'stmt': {'sql': sql, 'args': self._format_args(args)},
                },
                {'type': 'close'},
            ]
        }
        attempt = 0
        while True:
            try:
                response = self.session.post(
                    f'{self.database_url}/v2/pipeline',
                    json=payload,
                    headers=self.headers,
                    timeout=self.timeout,
                )
                return self._handle_response(response)
            except requests.exceptions.RequestException as e:
                if attempt >= self.retries:
                    raise TursoHTTPError(-1, f"Request failed: {str(e)}")
            # backoff with jitter
            delay = self.backoff_base * (2 ** attempt) + random.uniform(0, self.backoff_base)
            time.sleep(delay)
            attempt += 1

    def batch(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute multiple SQL statements in a single transaction.

        Args:
            queries: List of dicts with 'sql' and optional 'args'.
        """
        reqs: List[Dict[str, Any]] = []
        for q in queries:
            reqs.append(
                {
                    'type': 'execute',
                    'stmt': {
                        'sql': q['sql'],
                        'args': self._format_args(q.get('args')),
                    },
                }
            )
        reqs.append({'type': 'close'})
        attempt = 0
        while True:
            try:
                response = self.session.post(
                    f'{self.database_url}/v2/pipeline',
                    json={'requests': reqs},
                    headers=self.headers,
                    timeout=self.timeout,
                )
                return self._handle_response(response)
            except requests.exceptions.RequestException as e:
                if attempt >= self.retries:
                    raise TursoHTTPError(-1, f"Batch request failed: {str(e)}")
            delay = self.backoff_base * (2 ** attempt) + random.uniform(0, self.backoff_base)
            time.sleep(delay)
            attempt += 1

    def execute_pipeline(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute a series of SQL statements (pre-built request objects)."""
        payload = {'requests': queries + [{'type': 'close'}]}
        response = self.session.post(
            f'{self.database_url}/v2/pipeline',
            json=payload,
            headers=self.headers,
            timeout=self.timeout,
        )
        return self._handle_response(response)

    @staticmethod
    def _handle_response(response: requests.Response) -> Dict[str, Any]:
        """Process API response and handle errors."""
        if response.status_code == 200:
            return response.json()
        retry_after = None
        if response.status_code == 429:
            retry_after_hdr = response.headers.get('Retry-After')
            try:
                retry_after = float(retry_after_hdr) if retry_after_hdr else None
            except ValueError:
                retry_after = None
        try:
            error_data = response.json()
            error_msg = error_data.get('error', response.text)
        except ValueError:
            error_msg = response.text
        if response.status_code == 429:
            raise TursoRateLimitError(response.status_code, error_msg, retry_after)
        raise TursoHTTPError(response.status_code, error_msg)

    @staticmethod
    def _format_args(args: Optional[Union[List[Any], tuple]]) -> List[Dict[str, Any]]:
        if not args:
            return []
        formatted: List[Dict[str, Any]] = []
        for a in args:
            if isinstance(a, str):
                formatted.append({"type": "text", "value": a})
            elif isinstance(a, int):
                formatted.append({"type": "integer", "value": str(a)})
            elif isinstance(a, float):
                formatted.append({"type": "float", "value": str(a)})
            elif a is None:
                formatted.append({"type": "null"})
            elif isinstance(a, bool):
                formatted.append({"type": "integer", "value": "1" if a else "0"})
            else:
                raise ValueError(f"Unsupported argument type: {type(a)}")
        return formatted

    def close(self) -> None:
        """Close the HTTP session."""
        try:
            self.session.close()
        except Exception:
            pass

    # Context manager support for symmetry with async
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.session.close()
        except Exception:
            pass
        return False

# # Example usage
# if __name__ == "__main__":
#     # Initialize client (reads from .env by default)
#     client = TursoClient()
#
#     try:
#         # Create table
#         client.execute("""
#             CREATE TABLE IF NOT EXISTS users (
#                 uid TEXT PRIMARY KEY,
#                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             )
#         """)
#
#         # Insert a user
#         result = client.execute(
#             "INSERT INTO users (uid) VALUES (?)",
#             ["01K1BH5PW17TWEE1RZV7H6WENF"]
#         )
#         print("Insert successful:", result)
#
#         # Batch operations
#         batch_result = client.batch([
#             {"sql": "INSERT INTO users (uid) VALUES (?)", "args": ["USER001"]},
#             {"sql": "INSERT INTO users (uid) VALUES (?)", "args": ["USER002"]}
#         ])
#         print("Batch execution successful:", batch_result)
#
#     except Exception as e:
#         print("Operation failed:", str(e))
#     finally:
#         client.close()

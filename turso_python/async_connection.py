# Asynchronous Turso/libsql HTTP client using aiohttp and anyio
# No usage of python-dotenv; pass credentials explicitly or via environment variables.

from __future__ import annotations

import os
import random
from typing import Any

import aiohttp

from .exceptions import TursoHTTPError, TursoRateLimitError


def _normalize_database_url(url: str) -> str:
    if url.startswith("libsql://"):
        return "https://" + url[len("libsql://"):]
    return url


class AsyncTursoConnection:
    def __init__(
        self,
        database_url: str | None = None,
        auth_token: str | None = None,
        *,
        timeout: int = 30,
        session: aiohttp.ClientSession | None = None,
        retries: int = 0,
        backoff_base: float = 0.2,
) -> None:
        env_url = os.getenv("TURSO_DATABASE_URL")
        env_token = os.getenv("TURSO_AUTH_TOKEN")
        if not (database_url or env_url):
            raise ValueError("database_url not provided and TURSO_DATABASE_URL is not set")
        if not (auth_token or env_token):
            raise ValueError("auth_token not provided and TURSO_AUTH_TOKEN is not set")

        self.database_url = _normalize_database_url(database_url or env_url)  # type: ignore[arg-type]
        self.auth_token = auth_token or env_token  # type: ignore[assignment]
        # More precise timeouts by phase
        self._timeout = aiohttp.ClientTimeout(total=None, connect=timeout, sock_connect=timeout, sock_read=timeout)
        self._external_session = session is not None
        self._session = session
        self._headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }
        self._retries = max(0, int(retries))
        self._backoff_base = float(backoff_base)

    async def __aenter__(self) -> AsyncTursoConnection:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session and not self._external_session:
            await self._session.close()
            self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            # Allow ad-hoc usage without context manager, but ensure session exists
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def execute_query(self, sql: str, args: list[Any] | None = None) -> dict[str, Any]:
        payload = {
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": sql,
                        "args": self._format_args(args or []),
                    },
                },
                {"type": "close"},
            ]
        }
        attempt = 0
        while True:
            try:
                async with self.session.post(
                    f"{self.database_url}/v2/pipeline", json=payload, headers=self._headers
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    retry_after = None
                    if resp.status == 429:
                        ra = resp.headers.get('Retry-After')
                        try:
                            retry_after = float(ra) if ra else None
                        except Exception:
                            retry_after = None
                    text = await resp.text()
                    if resp.status == 429:
                        raise TursoRateLimitError(resp.status, text, retry_after)
                    raise TursoHTTPError(resp.status, text)
            except (aiohttp.ClientError, TursoHTTPError):
                if attempt >= self._retries:
                    raise
            # backoff with jitter
            delay = self._backoff_base * (2 ** attempt) + random.uniform(0, self._backoff_base)
            import anyio
            await anyio.sleep(delay)
            attempt += 1

    async def execute_pipeline(self, queries: list[dict[str, Any]]) -> dict[str, Any]:
        payload = {"requests": queries + [{"type": "close"}]}
        attempt = 0
        while True:
            try:
                async with self.session.post(
                    f"{self.database_url}/v2/pipeline", json=payload, headers=self._headers
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    retry_after = None
                    if resp.status == 429:
                        ra = resp.headers.get('Retry-After')
                        try:
                            retry_after = float(ra) if ra else None
                        except Exception:
                            retry_after = None
                    text = await resp.text()
                    if resp.status == 429:
                        raise TursoRateLimitError(resp.status, text, retry_after)
                    raise TursoHTTPError(resp.status, text)
            except (aiohttp.ClientError, TursoHTTPError):
                if attempt >= self._retries:
                    raise
            import anyio
            delay = self._backoff_base * (2 ** attempt) + random.uniform(0, self._backoff_base)
            await anyio.sleep(delay)
            attempt += 1

    @staticmethod
    def _format_args(args: list[Any]) -> list[dict[str, str]]:
        formatted: list[dict[str, str]] = []
        for a in args:
            if a is None:
                formatted.append({"type": "null", "value": "null"})
            elif isinstance(a, bool):
                formatted.append({"type": "integer", "value": "1" if a else "0"})
            elif isinstance(a, int):
                formatted.append({"type": "integer", "value": str(a)})
            elif isinstance(a, float):
                formatted.append({"type": "float", "value": str(a)})
            else:
                formatted.append({"type": "text", "value": str(a)})
        return formatted


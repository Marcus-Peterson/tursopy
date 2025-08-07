# Asynchronous Turso/libsql HTTP client using aiohttp and anyio
# No usage of python-dotenv; pass credentials explicitly or via environment variables.

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional
import aiohttp


def _normalize_database_url(url: str) -> str:
    if url.startswith("libsql://"):
        return "https://" + url[len("libsql://"):]
    return url


class AsyncTursoConnection:
    def __init__(
        self,
        database_url: Optional[str] = None,
        auth_token: Optional[str] = None,
        *,
        timeout: int = 30,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        env_url = os.getenv("TURSO_DATABASE_URL")
        env_token = os.getenv("TURSO_AUTH_TOKEN")
        if not (database_url or env_url):
            raise ValueError("database_url not provided and TURSO_DATABASE_URL is not set")
        if not (auth_token or env_token):
            raise ValueError("auth_token not provided and TURSO_AUTH_TOKEN is not set")

        self.database_url = _normalize_database_url(database_url or env_url)  # type: ignore[arg-type]
        self.auth_token = auth_token or env_token  # type: ignore[assignment]
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._external_session = session is not None
        self._session = session
        self._headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }

    async def __aenter__(self) -> "AsyncTursoConnection":
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

    async def execute_query(self, sql: str, args: Optional[List[Any]] = None) -> Dict[str, Any]:
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
        async with self.session.post(
            f"{self.database_url}/v2/pipeline", json=payload, headers=self._headers
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            text = await resp.text()
            raise RuntimeError(f"Turso API Error {resp.status}: {text}")

    async def execute_pipeline(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload = {"requests": queries + [{"type": "close"}]}
        async with self.session.post(
            f"{self.database_url}/v2/pipeline", json=payload, headers=self._headers
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            text = await resp.text()
            raise RuntimeError(f"Turso API Error {resp.status}: {text}")

    @staticmethod
    def _format_args(args: List[Any]) -> List[Dict[str, str]]:
        formatted: List[Dict[str, str]] = []
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


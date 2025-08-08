from __future__ import annotations
from typing import Any, Optional
from contextlib import asynccontextmanager
from .async_connection import AsyncTursoConnection

class AsyncTursoCRUD:
    def __init__(self, connection: AsyncTursoConnection):
        self.connection = connection
        self._owns_connection = False

    @classmethod
    @asynccontextmanager
    async def create_with_connection(cls, **connection_kwargs):
        """Context manager that handles connection lifecycle"""
        conn = AsyncTursoConnection(**connection_kwargs)
        try:
            await conn.__aenter__()
            crud = cls(conn)
            crud._owns_connection = True
            yield crud
        finally:
            await conn.__aexit__(None, None, None)

    async def close(self):
        """Explicit close method for manual management"""
        if self._owns_connection and self.connection:
            await self.connection.__aexit__(None, None, None)
            self.connection = None

    async def create(self, table: str, data: dict[str, Any]) -> dict[str, Any]:
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return await self.connection.execute_query(sql, list(data.values()))

    async def read(self, table: str, 
                 where: Optional[str] = None, 
                 args: Optional[list[Any]] = None,
                 columns: str = "*") -> dict[str, Any]:
        sql = f"SELECT {columns} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return await self.connection.execute_query(sql, args or [])

    async def update(self, table: str, 
                   data: dict[str, Any], 
                   where: str, 
                   where_args: list[Any]) -> dict[str, Any]:
        set_clause = ", ".join([f"{k} = ?" for k in data])
        sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
        return await self.connection.execute_query(
            sql, 
            list(data.values()) + where_args
        )

    async def delete(self, table: str, where: str, args: list[Any]) -> dict[str, Any]:
        sql = f"DELETE FROM {table} WHERE {where}"
        return await self.connection.execute_query(sql, args)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
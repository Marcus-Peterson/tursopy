# Async CRUD helpers built on AsyncTursoConnection
from __future__ import annotations

from typing import Any, Dict, List, Optional
from .async_connection import AsyncTursoConnection


class AsyncTursoCRUD:
    def __init__(self, connection: AsyncTursoConnection):
        self.connection = connection

    async def create(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return await self.connection.execute_query(sql, list(data.values()))

    async def read(self, table: str, where: Optional[str] = None, args: Optional[List[Any]] = None) -> Dict[str, Any]:
        sql = f"SELECT * FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return await self.connection.execute_query(sql, args or [])

    async def update(self, table: str, data: Dict[str, Any], where: str, args: List[Any]) -> Dict[str, Any]:
        set_clause = ", ".join([f"{k} = ?" for k in data])
        sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
        return await self.connection.execute_query(sql, list(data.values()) + list(args))

    async def delete(self, table: str, where: str, args: List[Any]) -> Dict[str, Any]:
        sql = f"DELETE FROM {table} WHERE {where}"
        return await self.connection.execute_query(sql, args)


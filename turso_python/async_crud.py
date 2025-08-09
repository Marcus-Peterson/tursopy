from __future__ import annotations
from typing import Any, Optional, List
from contextlib import asynccontextmanager
from .async_connection import AsyncTursoConnection
from .response_parser import TursoResponseParser

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
        """Create a new record in the table"""
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        raw_result = await self.connection.execute_query(sql, list(data.values()))
        return TursoResponseParser.normalize_response(raw_result)
    
    async def read(self, table: str,
                  where: Optional[str] = None,
                  args: Optional[list[Any]] = None,
                  columns: str = "*",
                  joins: Optional[List[str]] = None) -> dict[str, Any]:
        """
        Read records from the table, with optional JOINs.
        
        Returns normalized format:
        {
            'rows': [['01K1B9YA0292ZYEVHQ1HGY6VWC'], ['01JT2S68ZRW0PRDT5TM67CWYJX']],
            'columns': ['uid'],
            'count': 2
        }
        """
        sql = f"SELECT {columns} FROM {table}"
        if joins:
            sql += " " + " ".join(joins)
        if where:
            sql += f" WHERE {where}"
        
        raw_result = await self.connection.execute_query(sql, args or [])
        return TursoResponseParser.normalize_response(raw_result)
    
    async def update(self, table: str,
                    data: dict[str, Any],
                    where: str,
                    where_args: list[Any]) -> dict[str, Any]:
        """Update records in the table"""
        set_clause = ", ".join([f"{k} = ?" for k in data])
        sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
        
        raw_result = await self.connection.execute_query(
            sql, 
            list(data.values()) + where_args
        )
        return TursoResponseParser.normalize_response(raw_result)
    
    async def delete(self, table: str, where: str, args: list[Any]) -> dict[str, Any]:
        """Delete records from the table"""
        sql = f"DELETE FROM {table} WHERE {where}"
        raw_result = await self.connection.execute_query(sql, args)
        return TursoResponseParser.normalize_response(raw_result)
    
    async def set_foreign_key_checks(self, enable: bool) -> None:
        """Enable or disable foreign key constraint checks for the current connection."""
        state = "ON" if enable else "OFF"
        sql = f"PRAGMA foreign_keys = {state};"
        await self.connection.execute_query(sql)

    async def get_foreign_key_checks_status(self) -> bool:
        """Check if foreign key constraint checks are enabled for the current connection."""
        sql = "PRAGMA foreign_keys;"
        raw_result = await self.connection.execute_query(sql)
        normalized = TursoResponseParser.normalize_response(raw_result)
        if normalized.get('rows') and normalized['rows'][0]:
            return bool(normalized['rows'][0][0])
        return False
    
    # Convenience methods for easier data access
    async def read_all_rows(self, table: str, **kwargs) -> List[List[Any]]:
        """Get just the rows data directly"""
        result = await self.read(table, **kwargs)
        return result['rows']
    
    async def read_first_row(self, table: str, **kwargs) -> Optional[List[Any]]:
        """Get the first row directly, or None if no results"""
        result = await self.read(table, **kwargs)
        rows = result['rows']
        return rows[0] if rows else None
    
    async def read_count(self, table: str, **kwargs) -> int:
        """Get the count of matching records"""
        result = await self.read(table, **kwargs)
        return result['count']
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

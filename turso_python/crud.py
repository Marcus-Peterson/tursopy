# Contains CRUD operations (synchronous helpers).
# New asynchronous equivalents are provided in async_connection.py and async_crud.py
# This module intentionally does not use python-dotenv.

import os
import requests
import json
from typing import Any, Dict, List, Optional


def _normalize_database_url(url: str) -> str:
    if url.startswith("libsql://"):
        return "https://" + url[len("libsql://"):]
    return url

class TursoClient:
    def __init__(self, database_url: Optional[str] = None, auth_token: Optional[str] = None, *, timeout: int = 30):
        env_url = os.getenv("TURSO_DATABASE_URL")
        env_token = os.getenv("TURSO_AUTH_TOKEN")
        if not (database_url or env_url):
            raise ValueError("database_url not provided and TURSO_DATABASE_URL is not set")
        if not (auth_token or env_token):
            raise ValueError("auth_token not provided and TURSO_AUTH_TOKEN is not set")
        self.database_url = _normalize_database_url(database_url or env_url)  # type: ignore[arg-type]
        self.auth_token = auth_token or env_token  # type: ignore[assignment]
        self.timeout = timeout
        self.headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json'
        }

    def create_database(self,org_name, db_name, group_name, api_token):
        url = f"https://api.turso.tech/v1/organizations/{org_name}/databases"
    
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
    
        data = {
            "name": db_name,
            "group": group_name
        }
    
        response = requests.post(url, headers=headers, data=json.dumps(data))
    
        if response.status_code == 201:
            print(f"Database '{db_name}' created successfully.")
            return response.json()
        else:
            print(f"Failed to create database. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return None

    def execute_query(self, sql, args=None):
        """
        Execute a single SQL query.
        :param sql: SQL string
        :param args: List of arguments for the query
        :return: Response JSON or error message
        """
        payload = {
            'requests': [
                {
                    'type': 'execute',
                    'stmt': {
                        'sql': sql,
                        'args': self._format_args(args)
                    }
                },
                {'type': 'close'}
            ]
        }

        response = requests.post(
            f"{self.database_url}/v2/pipeline", json=payload, headers=self.headers, timeout=self.timeout
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Query failed: {response.status_code}, {response.text}")

    def execute_batch(self, queries):
        """
        Execute multiple queries in a single transaction.
        :param queries: List of dictionaries with 'sql' and 'args'
        :return: Response JSON or error message
        """
        payload = {
            'requests': [
                {
                    'type': 'execute',
                    'stmt': {
                        'sql': q['sql'],
                        'args': self._format_args(q.get('args'))
                    }
                } for q in queries
            ] + [{'type': 'close'}]
        }

        response = requests.post(
            f"{self.database_url}/v2/pipeline", json=payload, headers=self.headers, timeout=self.timeout
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Batch execution failed: {response.status_code}, {response.text}")

    def _format_args(self, args):
        """
        Format arguments for SQL statements.
        :param args: List of argument values
        :return: Formatted argument list
        """
        if not args:
            return []
        formatted = []
        for arg in args:
            if arg is None:
                formatted.append({"type": "null", "value": "null"})
            elif isinstance(arg, bool):
                formatted.append({"type": "integer", "value": "1" if arg else "0"})
            elif isinstance(arg, int):
                formatted.append({"type": "integer", "value": str(arg)})
            elif isinstance(arg, float):
                formatted.append({"type": "float", "value": str(arg)})
            else:
                formatted.append({"type": "text", "value": str(arg)})
        return formatted

class TursoSchemaManager(TursoClient):
    def create_table(self, table_name, schema):
        """
        Create a table with the specified schema.
        :param table_name: Name of the table
        :param schema: Dictionary with column names and types
        """
        columns = ', '.join([f"{col} {dtype}" for col, dtype in schema.items()])
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        return self.execute_query(sql)

    def drop_table(self, table_name):
        """
        Drop a table.
        :param table_name: Name of the table
        """
        sql = f"DROP TABLE IF EXISTS {table_name}"
        return self.execute_query(sql)

class TursoDataManager(TursoClient):
    def insert(self, table_name, data):
        """
        Insert a row into a table.
        :param table_name: Name of the table
        :param data: Dictionary of column-value pairs
        """
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data.values()])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        return self.execute_query(sql, list(data.values()))

    def fetch_all(self, table_name, conditions=None):
        """
        Fetch all rows from a table with optional conditions.
        :param table_name: Name of the table
        :param conditions: SQL WHERE clause (string)
        """
        sql = f"SELECT * FROM {table_name}"
        if conditions:
            sql += f" WHERE {conditions}"
        return self.execute_query(sql)

    def fetch_one(self, table_name, conditions):
        """
        Fetch a single row from a table.
        :param table_name: Name of the table
        :param conditions: SQL WHERE clause (string)
        """
        sql = f"SELECT * FROM {table_name} WHERE {conditions} LIMIT 1"
        result = self.execute_query(sql)
        return result['results'][0]['response']['result']['rows'][0] if result else None

    def update(self, table_name, updates, conditions):
        """
        Update rows in a table.
        :param table_name: Name of the table
        :param updates: Dictionary of column-value pairs to update
        :param conditions: SQL WHERE clause (string)
        """
        set_clause = ', '.join([f"{col} = ?" for col in updates.keys()])
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {conditions}"
        return self.execute_query(sql, list(updates.values()))

    def delete(self, table_name, conditions):
        """
        Delete rows from a table.
        :param table_name: Name of the table
        :param conditions: SQL WHERE clause (string)
        """
        sql = f"DELETE FROM {table_name} WHERE {conditions}"
        return self.execute_query(sql)


class TursoCRUD:
    def __init__(self, connection):
        self.connection = connection

    def create(self, table, data):
        """Insert a record into a table."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        args = list(data.values())  # Pass plain values
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return self.connection.execute_query(sql, args)

    def read(self, table, where=None, args=None):
        """Retrieve records from a table with optional filters."""
        sql = f"SELECT * FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return self.connection.execute_query(sql, args)

    def update(self, table, data, where, args):
        """Update records in a table."""
        set_clause = ', '.join([f"{key} = ?" for key in data])
        update_args = list(data.values())  # Use plain values
        sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
        return self.connection.execute_query(sql, update_args + [arg['value'] for arg in args])  # Flatten args


    def delete(self, table, where, args):
        """Delete records from a table."""
        sql = f"DELETE FROM {table} WHERE {where}"
        return self.connection.execute_query(sql, args)

    @staticmethod
    def _infer_type(value):
        if isinstance(value, int):
            return "integer"
        elif isinstance(value, str):
            return "text"
        elif isinstance(value, float):
            return "float"
        return "text"

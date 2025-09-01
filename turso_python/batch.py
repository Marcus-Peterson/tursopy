#Handles batch operations.
from turso_python.crud import TursoCRUD


class TursoBatch:
    def __init__(self, connection):
        self.connection = connection

    def batch_insert(self, table, data_list):
        """Insert multiple rows into a table.
        Uses the connection.batch helper so argument typing/serialization is consistent
        with single-statement execute_query. This avoids float-as-string issues.
        """
        if not data_list:
            return

        # Ensure consistent column order across rows
        keys = list(data_list[0].keys())
        columns = ', '.join(keys)
        placeholders = ', '.join(['?' for _ in keys])

        queries = [
            {
                'sql': f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                'args': [row[k] for k in keys],
            }
            for row in data_list
        ]

        # Delegate to connection.batch which formats args appropriately
        return self.connection.batch(queries)
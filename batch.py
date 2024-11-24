#Handles batch operations.
from crud import TursoCRUD
class TursoBatch:
    def __init__(self, connection):
        self.connection = connection

    def batch_insert(self, table, data_list):
        """Insert multiple rows into a table."""
        if not data_list:
            return
        
        columns = ', '.join(data_list[0].keys())
        placeholders = ', '.join(['?' for _ in data_list[0]])
        args_list = [
            [{"type": TursoCRUD._infer_type(value), "value": str(value)} for value in row.values()]
            for row in data_list
        ]
        queries = [
            {'type': 'execute', 'stmt': {'sql': f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", 'args': args}}
            for args in args_list
        ]
        return self.connection.execute_pipeline(queries)
    
    

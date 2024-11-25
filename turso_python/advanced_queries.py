class TursoAdvancedQueries:
    def __init__(self, connection):
        self.connection = connection

    def join_query(self, base_table, join_details=None, select_columns='*', where=None):
        """Perform a join query."""
        # Default to joining with an empty list if no joins are provided
        join_details = join_details or []
        sql = f"SELECT {select_columns} FROM {base_table}"
        
        for join_table, join_condition in join_details:
            sql += f" JOIN {join_table} ON {join_condition}"
        
        if where:
            sql += f" WHERE {where}"
        
        return self.connection.execute_query(sql)

    def aggregate_query(self, table, aggregation, column, where=None):
        """Perform aggregation queries (e.g., COUNT, AVG, SUM, etc.)."""
        aggregation_functions = ['COUNT', 'AVG', 'SUM', 'MIN', 'MAX']
        
        if aggregation not in aggregation_functions:
            raise ValueError(f"Invalid aggregation function. Must be one of {aggregation_functions}")
        
        sql = f"SELECT {aggregation}({column}) FROM {table}"
        if where:
            sql += f" WHERE {where}"
        
        return self.connection.execute_query(sql)

    def subquery_query(self, base_table, subquery, select_columns='*', where=None):
        """Perform a query with a subquery."""
        sql = f"SELECT {select_columns} FROM {base_table} WHERE {subquery}"
        if where:
            sql += f" AND {where}"
        
        return self.connection.execute_query(sql)

    def order_by_query(self, table, select_columns='*', where=None, order_by=None, limit=None):
        """Perform a query with sorting and limit."""
        sql = f"SELECT {select_columns} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit:
            sql += f" LIMIT {limit}"
        
        return self.connection.execute_query(sql)

    def complex_where_query(self, table, select_columns='*', conditions=None):
        """Perform a query with complex WHERE clauses (AND, OR, parentheses)."""
        conditions = conditions or []
        
        sql = f"SELECT {select_columns} FROM {table}"
        
        if conditions:
            where_clause = ' AND '.join(conditions)
            sql += f" WHERE ({where_clause})"
        
        return self.connection.execute_query(sql)
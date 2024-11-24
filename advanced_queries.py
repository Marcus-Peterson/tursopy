#For complex queries.
class TursoAdvancedQueries:
    def __init__(self, connection):
        self.connection = connection

    def join_query(self, base_table, join_table, join_condition, select_columns='*', where=None):
        """Perform a join query."""
        sql = f"SELECT {select_columns} FROM {base_table} JOIN {join_table} ON {join_condition}"
        if where:
            sql += f" WHERE {where}"
        return self.connection.execute_query(sql)

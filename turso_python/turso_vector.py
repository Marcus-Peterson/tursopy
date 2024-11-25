from turso_python.connection import TursoConnection
import json
"""
-   Vector embeddings
-   This client is used for creating vector embeddings, vector search and more
"""
class TursoVector:
    def __init__(self, connection: TursoConnection, table_name: str):
        self.connection = connection
        self.table_name = table_name

    def create_table(self, vector_column: str, vector_type: str, dimensions: int):
        """Creates a table with a vector column."""
        query = f"""
        CREATE TABLE {self.table_name} (
            title TEXT,
            year INT,
            {vector_column} {vector_type}({dimensions})  -- vector column
        );
        """
        return self.connection.execute_query(query)

    def insert_embedding(self, title: str, year: int, vector_column: str, embedding: list):
        """Inserts embeddings into the table."""
        query = f"""
        INSERT INTO {self.table_name} (title, year, {vector_column})
        VALUES (?, ?, vector32(?));
        """
        # Convert list to a JSON string representation of the vector
        embedding_str = json.dumps(embedding)
        params = (title, year, embedding_str)
        return self.connection.execute_query(query, params)

    def create_index(self, vector_column: str):
        """Creates a vector index on the table."""
        query = f"""
        CREATE INDEX {self.table_name}_idx ON {self.table_name} (libsql_vector_idx({vector_column}));
        """
        return self.connection.execute_query(query)

    def vector_similarity_search(self, vector_column: str, query_vector: list, top_k: int = 5):
        """Performs a vector similarity search using cosine distance."""
        query = f"""
        SELECT title, year
        FROM vector_top_k('{self.table_name}_idx', vector32(?), ?)
        JOIN {self.table_name} ON {self.table_name}.rowid = id;
        """
        # Convert list to a JSON string representation of the query vector
        query_vector_str = json.dumps(query_vector)
        params = (query_vector_str, top_k)
        return self.connection.execute_query(query, params)

    def vector_distance_cos(self, vector_column: str, query_vector: list):
        """Calculates the cosine distance between vectors."""
        query = f"""
        SELECT title, vector_extract({vector_column}), vector_distance_cos({vector_column}, vector32(?))
        FROM {self.table_name}
        ORDER BY vector_distance_cos({vector_column}, vector32(?)) ASC;
        """
        query_vector_str = json.dumps(query_vector)
        params = (query_vector_str, query_vector_str)
        return self.connection.execute_query(query, params)

    def create_partial_index(self, vector_column: str, year_threshold: int):
        """Creates a partial index based on a condition (e.g., year >= threshold)."""
        query = f"""
        CREATE INDEX {self.table_name}_partial_idx ON {self.table_name} 
        (libsql_vector_idx({vector_column})) 
        WHERE year >= ?;
        """
        return self.connection.execute_query(query, (year_threshold,))

    def reindex(self):
        """Rebuilds the index."""
        query = f"REINDEX {self.table_name}_idx;"
        return self.connection.execute_query(query)

    def drop_index(self):
        """Drops the index."""
        query = f"DROP INDEX {self.table_name}_idx;"
        return self.connection.execute_query(query)

    def extract_vector(self, vector_column: str, row_id: int):
        """Extracts the vector from a specific row."""
        query = f"SELECT vector_extract({vector_column}) FROM {self.table_name} WHERE rowid = ?;"
        return self.connection.execute_query(query, (row_id,))

    def update_embedding(self, vector_column: str, row_id: int, new_embedding: list):
        """Updates the vector for a given row."""
        query = f"""
        UPDATE {self.table_name} SET {vector_column} = vector32(?)
        WHERE rowid = ?;
        """
        embedding_str = json.dumps(new_embedding)
        return self.connection.execute_query(query, (embedding_str, row_id))

    def delete_row(self, row_id: int):
        """Deletes a row from the table."""
        query = f"DELETE FROM {self.table_name} WHERE rowid = ?;"
        return self.connection.execute_query(query, (row_id,))
import unittest
from crud import TursoSchemaManager,TursoCRUD, TursoDataManager, TursoClient  # Adjust import as per your structure
import os
from dotenv import load_dotenv
import logging
load_dotenv()

def main_1():
    # Initialize TursoClient and managers
    client = TursoClient()
    schema_manager = TursoSchemaManager()
    data_manager = TursoDataManager()
    crud_manager = TursoCRUD(client)

    # Test 1: Create a table
    print("Testing create_table...")
    schema = {"id": "INTEGER PRIMARY KEY", "name": "TEXT", "age": "INTEGER"}
    result = schema_manager.create_table("users", schema)
    print("Create Table Result:", result)

    # Test 2: Insert data into the table
    print("Testing insert...")
    data = {"id": 1, "name": "John Doe", "age": 30}
    result = data_manager.insert("users", data)
    print("Insert Result:", result)

    # Test 3: Fetch all data
    print("Testing fetch_all...")
    result = data_manager.fetch_all("users")
    print("Fetch All Result:", result)

    # Test 4: Fetch one row
    print("Testing fetch_one...")
    result = data_manager.fetch_one("users", "id = 1")
    print("Fetch One Result:", result)

    # Test 5: Update a row
    print("Testing update...")
    updates = {"name": "John Smith", "age": 31}
    result = data_manager.update("users", updates, "id = 1")
    print("Update Result:", result)

    # Test 6: Fetch all data after update
    print("Testing fetch_all after update...")
    result = data_manager.fetch_all("users")
    print("Fetch All After Update Result:", result)

    # Test 7: Delete a row
    print("Testing delete...")
    result = data_manager.delete("users", "id = 1")
    print("Delete Result:", result)

    # Test 8: Fetch all data after deletion
    print("Testing fetch_all after deletion...")
    result = data_manager.fetch_all("users")
    print("Fetch All After Deletion Result:", result)

    # Test 9: Batch operations
    print("Testing batch operations...")
    batch_queries = [
        {"sql": "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", "args": [2, "Alice", 25]},
        {"sql": "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", "args": [3, "Bob", 28]}
    ]
    result = client.execute_batch(batch_queries)
    print("Batch Operation Result:", result)

    # Test 10: CRUD operations
    print("Testing CRUD operations via TursoCRUD...")
    crud_manager.create("users", {"id": 4, "name": "Charlie", "age": 35})
    print("CRUD Create: Fetch All:", data_manager.fetch_all("users"))
    crud_manager.update("users", {"name": "Charles"}, "id = ?", [{"type": "integer", "value": "4"}])
    print("CRUD Update: Fetch All:", data_manager.fetch_all("users"))
    crud_manager.delete("users", "id = ?", [4])
    print("CRUD Delete: Fetch All:", data_manager.fetch_all("users"))

    # Test 11: Drop table
    print("Testing drop_table...")
    result = schema_manager.drop_table("users")
    print("Drop Table Result:", result)


from batch import TursoBatch
from connection import TursoConnection
from logger import TursoLogger
from schema_validator import SchemaValidator
from advanced_queries import TursoAdvancedQueries
def main_2():
    # Create a connection to the database
    connection = TursoConnection()

    # Create instances of the classes
    turso_batch = TursoBatch(connection)
    turso_logger = TursoLogger()
    schema_validator = SchemaValidator()
    turso_advanced_queries = TursoAdvancedQueries(connection)

    # Test: Insert batch of data
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35}
    ]
    
    logging.info("Testing batch insert...")
    try:
        result = turso_batch.batch_insert("users", data)
        logging.info("Batch insert result: %s", result)
    except Exception as e:
        logging.error("Error during batch insert: %s", e)

    # Test: Execute a single query
    logging.info("Testing single query execution...")
    try:
        result = connection.execute_query("SELECT * FROM users")
        logging.info("Query result: %s", result)
    except Exception as e:
        logging.error("Error during query execution: %s", e)

    # Test: Join query
    logging.info("Testing join query...")
    try:
        result = turso_advanced_queries.join_query(
            "users", "orders", "users.id = orders.user_id", "users.name, orders.amount", "users.age > 30"
        )
        logging.info("Join query result: %s", result)
    except Exception as e:
        logging.error("Error during join query: %s", e)

    # Test: Schema validation
    logging.info("Testing schema validation...")
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "age": {"type": "integer"}
        },
        "required": ["id", "name", "age"]
    }

    valid_data = {"id": 4, "name": "David", "age": 40}
    invalid_data = {"id": 5, "name": "Eve"}

    try:
        schema_validator.validate_input(valid_data, schema)
        logging.info("Valid data passed validation.")
    except ValueError as e:
        logging.error("Valid data failed validation: %s", e)

    try:
        schema_validator.validate_input(invalid_data, schema)
        logging.info("Invalid data passed validation.")
    except ValueError as e:
        logging.error("Invalid data failed validation: %s", e)

if __name__ == "__main__":
    #main_1()           #Uncomment to test
    #main_2()           #Uncomment to test
    pass

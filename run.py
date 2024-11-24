"""connection.py – Handles Turso API communication.
crud.py – Contains CRUD operations.
advanced_queries.py – For complex queries.
batch.py – Handles batch operations.
logger.py – Manages logging.
schema_validator.py – Validates input schemas."""

files = ["connection.py","crud.py","advanced_queries.py","batch.py","logger.py","schema_validator.py"]

for i in files:
    with open(i,"a") as file:
        file.write("#")
        file.close()
# TursoPy

A lightweight, dependency-minimal Python client for Turso databases. Born out of frustration with dependency issues in existing solutions, TursoPy provides a straightforward way to interact with Turso databases without the complexity.

> **Note**: This project isn't meant to compete with libsql-experimental or suggest it's any better or worse. It's simply an alternative born from experiencing dependency issues with existing solutions. While TursoPy might not be as feature-rich as libsql-experimental, it gets the job done with minimal fuss. As a solo developer without corporate backing, I created this to solve my own frustrations and share it with others who might face similar challenges.

## Features

- Simple CRUD operations
- Batch processing support
- Advanced query capabilities including JOINs
- Minimal dependencies (just `requests` and `python-dotenv`)
- Straightforward error handling
- Database creation utilities

## Installation
```
pip install turso-python
```
If pip install doesn't work, clone the project or download the zip from the realease section

## Quick Start

1. Set up your environment variables:

```env
TURSO_DATABASE_URL="https://your_database_url"
TURSO_AUTH_TOKEN="your_auth_token"
```
When you use the turso dashboard to generate urls and auth token, the url will be libsql: change that to https:
> libsql://database-organisation.turso.io --> https://database-organisation.turso.io


2. Basic usage:

```python
from connection import TursoConnection
from crud import TursoCRUD

# Initialize connection
connection = TursoConnection()
crud = TursoCRUD(connection)

# Insert data
data = {"name": "John Doe", "age": 30}
crud.create("users", data)

# Read data
result = crud.read("users", "name = ?", [{"type": "text", "value": "John Doe"}])
```

## Detailed Usage Guide

### Connection Management

The `TursoConnection` class handles all communication with the Turso API:

```python
from connection import TursoConnection

# Using environment variables
connection = TursoConnection()

# Or explicit credentials
connection = TursoConnection(
    database_url="your_database_url",
    auth_token="your_auth_token"
)
```

### CRUD Operations

#### Create

```python
crud = TursoCRUD(connection)

# Single insert
user_data = {
    "name": "Jane Smith",
    "email": "jane@example.com",
    "age": 25
}
crud.create("users", user_data)
```

#### Read

```python
# Fetch all records
all_users = crud.read("users")

# Fetch with conditions
young_users = crud.read(
    table="users",
    where="age < ?",
    args=[{"type": "integer", "value": "30"}]
)
```

#### Update

```python
update_data = {"age": 31}
crud.update(
    table="users",
    data=update_data,
    where="name = ?",
    args=[{"type": "text", "value": "Jane Smith"}]
)
```

#### Delete

```python
crud.delete(
    table="users",
    where="name = ?",
    args=[{"type": "text", "value": "Jane Smith"}]
)
```

### Batch Operations

For bulk operations, use the `TursoBatch` class:

```python
from batch import TursoBatch

batch = TursoBatch(connection)

users = [
    {"name": "User 1", "age": 25},
    {"name": "User 2", "age": 30},
    {"name": "User 3", "age": 35}
]

batch.batch_insert("users", users)
```

### Advanced Queries

The `TursoAdvancedQueries` class handles complex operations:

```python
from advanced_queries import TursoAdvancedQueries

advanced = TursoAdvancedQueries(connection)

# Perform a JOIN operation
result = advanced.join_query(
    base_table="users",
    join_table="orders",
    join_condition="users.id = orders.user_id",
    select_columns="users.name, orders.order_date",
    where="orders.amount > 100"
)
```

### Schema Management

The `TursoSchemaManager` helps with table operations:

```python
from schema_validator import TursoSchemaManager

schema_manager = TursoSchemaManager(connection)

# Create a new table
schema = {
    "id": "INTEGER PRIMARY KEY",
    "name": "TEXT NOT NULL",
    "email": "TEXT UNIQUE",
    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
}
schema_manager.create_table("users", schema)

# Drop a table
schema_manager.drop_table("old_table")
```

### Database Creation

```python
from crud import TursoClient

client = TursoClient()

# Create a new database
client.create_database(
    org_name="your_org",
    db_name="new_database",
    group_name="default",
    api_token="your_api_token"
)
```

## Error Handling

TursoPy includes basic error handling for common scenarios:

```python
try:
    result = crud.read("non_existent_table")
except Exception as e:
    print(f"An error occurred: {e}")
```

## Best Practices

1. Always use environment variables for sensitive credentials
2. Use batch operations for bulk inserts/updates
3. Close connections when done (handled automatically in most cases)
4. Use proper type hints in query arguments
5. Handle errors appropriately in production code

## Contributing

Contributions are welcome! As a solo developer project, I appreciate any help in improving TursoPy. Please feel free to:

- Report bugs
- Suggest features
- Submit pull requests
- Improve documentation

## License

Apache 2.0 License

## Support

For issues, questions, or suggestions, please open an issue on GitHub. As a solo developer, I'll do my best to respond in a timely manner.

---

Remember: TursoPy is designed to be simple and straightforward. While it might not have all the bells and whistles of other clients, it focuses on reliability and ease of use. Sometimes, simpler is better!

**Note for 2024-11-24, I literally built this yesterday so I'm still tweaking some stuff and fixing some stuff**
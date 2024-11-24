#Validates input schemas
from jsonschema import validate, ValidationError

class SchemaValidator:
    @staticmethod
    def validate_input(data, schema):
        try:
            validate(instance=data, schema=schema)
            return True
        except ValidationError as e:
            raise ValueError(f"Schema validation error: {e.message}")

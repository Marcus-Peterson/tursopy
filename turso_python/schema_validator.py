import msgspec
from typing import Any


class SchemaValidator:
    @staticmethod
    def _py_type_for_json_type(t: str):
        t = (t or "").lower()
        if t in {"integer", "int"}:
            return int
        if t in {"number", "float", "real", "double"}:
            return (int, float)
        if t in {"string", "text"}:
            return str
        if t in {"boolean", "bool"}:
            return bool
        if t == "array":
            return list
        if t == "object":
            return dict
        # Unknown types: accept anything
        return object

    @staticmethod
    def _validate_against_schema_dict(data: Any, schema: dict[str, Any]) -> None:
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a dict when using JSON Schema-like validation")
        schema_type = schema.get("type")
        if schema_type and schema_type != "object":
            # Basic support only for object schemas here
            raise ValueError(f"Unsupported schema root type: {schema_type}")

        properties: dict[str, dict[str, Any]] = schema.get("properties", {}) or {}
        required = set(schema.get("required", []) or [])

        if not isinstance(data, dict):
            raise ValueError("Data must be an object/dict for the given schema")

        # Check required fields
        missing = [k for k in required if k not in data]
        if missing:
            raise ValueError(f"Missing required field(s): {', '.join(missing)}")

        # Check types for provided properties
        for key, prop in properties.items():
            if key not in data:
                continue  # optional
            expected_type = SchemaValidator._py_type_for_json_type(prop.get("type", ""))
            value = data[key]
            if expected_type is object:
                # Unknown type, skip strict checking
                continue
            if not isinstance(value, expected_type):
                raise ValueError(
                    f"Field '{key}' expected type {prop.get('type')}, got {type(value).__name__}"
                )

        # Additional properties allowed by default; extend here if needed

    @staticmethod
    def validate_input(data: Any, schema: Any) -> bool:
        """Validate data against either:
        - a JSON Schema-like dict with keys: type, properties, required; or
        - a Python type supported by msgspec (e.g., dataclass/Struct) for strict decoding.
        Returns True on success; raises ValueError on validation failure.
        """
        # Branch on schema shape
        if isinstance(schema, dict):
            SchemaValidator._validate_against_schema_dict(data, schema)
            return True
        # Fall back to msgspec when given a Python type
        try:
            msgspec.json.decode(msgspec.json.encode(data), type=schema)
            return True
        except msgspec.ValidationError as e:
            raise ValueError(f"Schema validation error: {e}")
        except TypeError as e:
            # Provided schema isn't a Python type and not a dict
            raise ValueError(f"Unsupported schema type for msgspec: {e}")

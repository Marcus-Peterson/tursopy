import msgspec


class SchemaValidator:
    @staticmethod
    def validate_input(data, schema):
        try:
            msgspec.json.decode(msgspec.json.encode(data), type=schema)
            return True
        except msgspec.ValidationError as e:
            raise ValueError(f"Schema validation error: {e}")
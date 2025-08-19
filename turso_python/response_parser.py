
from typing import Any


class TursoResponseParser:
    """Helper class to parse Turso database responses"""

    @staticmethod
    def _raise_if_error(response: dict[str, Any]) -> None:
        """Inspect the pipeline response and raise an exception if any step indicates error.
        We consider either a top-level result with type == 'error' or a response payload
        that contains an 'error' marker.
        """
        try:
            results = response.get('results', []) if isinstance(response, dict) else []
            for item in results:
                if not isinstance(item, dict):
                    continue
                if item.get('type') == 'error':
                    # Some payloads include an 'error' field directly.
                    err = item.get('error') or item.get('response') or 'Unknown error'
                    raise Exception(f"Turso execute error: {err}")
                resp = item.get('response')
                if isinstance(resp, dict) and resp.get('type') == 'error':
                    raise Exception(f"Turso execute error: {resp}")
        except Exception:
            # If parsing itself throws, propagate that exception
            raise

    @staticmethod
    def extract_rows(response: dict[str, Any]) -> list[list[Any]]:
        """
        Extract rows from Turso response format
        
        Turso format: response.results[0].response.result.rows
        Each row contains objects with 'type' and 'value' keys
        """
        try:
            if not response or not isinstance(response, dict):
                return []

            results = response.get('results', [])
            if not results:
                return []

            first_result = results[0]
            if not isinstance(first_result, dict) or first_result.get('type') != 'ok':
                return []

            response_data = first_result.get('response', {})
            if response_data.get('type') != 'execute':
                return []

            result = response_data.get('result', {})
            raw_rows = result.get('rows', [])

            parsed_rows = []
            for raw_row in raw_rows:
                parsed_row = []
                for cell in raw_row:
                    if isinstance(cell, dict) and 'value' in cell:
                        parsed_row.append(cell['value'])
                    else:
                        parsed_row.append(cell)
                parsed_rows.append(parsed_row)

            return parsed_rows

        except Exception:
            return []

    @staticmethod
    def extract_columns(response: dict[str, Any]) -> list[str]:
        """Extract column names from Turso response"""
        try:
            if not response or not isinstance(response, dict):
                return []

            results = response.get('results', [])
            if not results:
                return []

            first_result = results[0]
            if not isinstance(first_result, dict) or first_result.get('type') != 'ok':
                return []

            response_data = first_result.get('response', {})
            if response_data.get('type') != 'execute':
                return []

            result = response_data.get('result', {})
            cols = result.get('cols', [])

            return [col.get('name', '') for col in cols]

        except Exception:
            return []

    @staticmethod
    def normalize_response(response: dict[str, Any]) -> dict[str, Any]:
        """
        Convert Turso response to a normalized format that matches expectations
        Returns: {'rows': [[value1, value2], ...], 'columns': ['col1', 'col2'], 'count': int}
        """
        # Raise if any step indicates an error
        TursoResponseParser._raise_if_error(response)

        rows = TursoResponseParser.extract_rows(response)
        columns = TursoResponseParser.extract_columns(response)

        return {
            'rows': rows,
            'columns': columns,
            'count': len(rows)
        }

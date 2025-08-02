import os
from dotenv import load_dotenv
import requests
from urllib.parse import urlparse
from typing import Optional, Union, List, Dict, Any

load_dotenv()

class TursoClient:
    def __init__(self, database_url: Optional[str] = None, auth_token: Optional[str] = None):
        """
        Initialize connection to Turso database.
        
        Args:
            database_url: Either libsql:// or https:// URL (will be normalized)
            auth_token: Turso authentication token
        """
        self.database_url = self._normalize_url(database_url or os.getenv("TURSO_DATABASE_URL"))
        self.auth_token = auth_token or os.getenv("TURSO_AUTH_TOKEN")
        self.headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json'
        }
        self.session = requests.Session()

    def _normalize_url(self, url: str) -> str:
        """Convert libsql:// to https:// and validate URL format."""
        if url.startswith('libsql://'):
            url = 'https://' + url[len('libsql://'):]
        
        # Remove any existing /v2/pipeline suffix
        url = url.rstrip('/').replace('/v2/pipeline', '')
        
        # Validate URL structure
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid database URL: {url}")
            
        return url

    def _format_args(self, args: Union[List[Any], tuple, None]) -> List[Dict[str, Any]]:
        """Convert Python values to Turso API value format."""
        if not args:
            return []
        
        formatted = []
        for arg in args:
            if isinstance(arg, str):
                formatted.append({"type": "text", "value": arg})
            elif isinstance(arg, int):
                formatted.append({"type": "integer", "value": str(arg)})
            elif isinstance(arg, float):
                formatted.append({"type": "float", "value": str(arg)})
            elif arg is None:
                formatted.append({"type": "null"})
            elif isinstance(arg, bool):
                formatted.append({"type": "integer", "value": "1" if arg else "0"})
            else:
                raise ValueError(f"Unsupported argument type: {type(arg)}")
        return formatted

    def execute(self, sql: str, args: Optional[Union[List[Any], tuple]] = None) -> Dict[str, Any]:
        """
        Execute a single SQL statement.
        
        Args:
            sql: SQL query with ? placeholders
            args: Parameters for the query
            
        Returns:
            Response from Turso API
        """
        payload = {
            'requests': [
                {
                    'type': 'execute',
                    'stmt': {
                        'sql': sql,
                        'args': self._format_args(args)
                    }
                },
                {'type': 'close'}
            ]
        }
        
        try:
            response = self.session.post(
                f'{self.database_url}/v2/pipeline',
                json=payload,
                headers=self.headers,
                timeout=10
            )
            return self._handle_response(response)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Request failed: {str(e)}")

    def batch(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Execute multiple SQL statements in a single transaction.
        
        Args:
            queries: List of dicts with 'sql' and 'args' keys
            
        Returns:
            Response from Turso API
        """
        requests = []
        for query in queries:
            requests.append({
                'type': 'execute',
                'stmt': {
                    'sql': query['sql'],
                    'args': self._format_args(query.get('args'))
                }
            })
        requests.append({'type': 'close'})
        
        try:
            response = self.session.post(
                f'{self.database_url}/v2/pipeline',
                json={'requests': requests},
                headers=self.headers,
                timeout=15
            )
            return self._handle_response(response)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Batch request failed: {str(e)}")

    @staticmethod
    def _handle_response(response: requests.Response) -> Dict[str, Any]:
        """Process API response and handle errors."""
        if response.status_code == 200:
            return response.json()
        
        try:
            error_data = response.json()
            error_msg = error_data.get('error', response.text)
        except ValueError:
            error_msg = response.text
            
        raise RuntimeError(f"Turso API Error {response.status_code}: {error_msg}")

    def close(self):
        """Close the HTTP session."""
        self.session.close()

# # Example usage
# if __name__ == "__main__":
#     # Initialize client (reads from .env by default)
#     client = TursoClient()
    
#     try:
#         # Create table
#         client.execute("""
#             CREATE TABLE IF NOT EXISTS users (
#                 uid TEXT PRIMARY KEY,
#                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             )
#         """)
        
#         # Insert a user
#         result = client.execute(
#             "INSERT INTO users (uid) VALUES (?)",
#             ["01K1BH5PW17TWEE1RZV7H6WENF"]
#         )
#         print("Insert successful:", result)
        
#         # Batch operations
#         batch_result = client.batch([
#             {"sql": "INSERT INTO users (uid) VALUES (?)", "args": ["USER001"]},
#             {"sql": "INSERT INTO users (uid) VALUES (?)", "args": ["USER002"]}
#         ])
#         print("Batch execution successful:", batch_result)
        
#     except Exception as e:
#         print("Operation failed:", str(e))
#     finally:
#         client.close()
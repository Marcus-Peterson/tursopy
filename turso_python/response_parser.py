from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class TursoResponseParser:
    """Helper class to parse Turso database responses"""
    
    @staticmethod
    def extract_rows(response: Dict[str, Any]) -> List[List[Any]]:
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
            
            logger.debug(f"Parsed {len(parsed_rows)} rows from Turso response")
            return parsed_rows
            
        except Exception as e:
            logger.error(f"Error parsing Turso response: {e}")
            logger.debug(f"Raw response: {response}")
            return []
    
    @staticmethod
    def extract_columns(response: Dict[str, Any]) -> List[str]:
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
            
        except Exception as e:
            logger.error(f"Error extracting columns: {e}")
            return []
    
    @staticmethod
    def normalize_response(response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert Turso response to a normalized format that matches expectations
        Returns: {'rows': [[value1, value2], ...], 'columns': ['col1', 'col2'], 'count': int}
        """
        rows = TursoResponseParser.extract_rows(response)
        columns = TursoResponseParser.extract_columns(response)
        
        return {
            'rows': rows,
            'columns': columns,
            'count': len(rows),
            'raw_response': response  
        }
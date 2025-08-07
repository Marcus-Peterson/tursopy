import json
import types
import requests
import requests_mock
import pytest
from turso_python.connection import TursoConnection
from turso_python.exceptions import TursoHTTPError


def pipeline_response_ok(rows=None):
    if rows is None:
        rows = [[{"type": "integer", "value": "1"}]]
    return {
        "results": [
            {
                "type": "execute",
                "response": {
                    "result": {
                        "rows": rows
                    }
                }
            }
        ]
    }


def test_sync_execute_success():
    with requests_mock.Mocker() as m:
        m.post('https://example.test/v2/pipeline', json=pipeline_response_ok())
        c = TursoConnection(database_url='https://example.test', auth_token='t')
        resp = c.execute_query('SELECT 1')
        assert isinstance(resp, dict)
        assert 'results' in resp


def test_sync_retries_and_error():
    with requests_mock.Mocker() as m:
        m.post('https://example.test/v2/pipeline', status_code=500, json={"error": "boom"})
        c = TursoConnection(database_url='https://example.test', auth_token='t', retries=1, backoff_base=0)
        with pytest.raises(TursoHTTPError):
            c.execute_query('SELECT 1')


import pytest
from aioresponses import aioresponses

from turso_python.async_connection import AsyncTursoConnection
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


@pytest.mark.anyio
async def test_async_execute_success():
    with aioresponses() as m:
        m.post('https://example.test/v2/pipeline', status=200, payload=pipeline_response_ok())
        async with AsyncTursoConnection(database_url='https://example.test', auth_token='t') as c:
            resp = await c.execute_query('SELECT 1')
            assert isinstance(resp, dict)
            assert 'results' in resp


@pytest.mark.anyio
async def test_async_retries_and_error():
    with aioresponses() as m:
        m.post('https://example.test/v2/pipeline', status=500, payload={"error": "boom"}, repeat=True)
        async with AsyncTursoConnection(database_url='https://example.test', auth_token='t', retries=1, backoff_base=0) as c:
            with pytest.raises(TursoHTTPError):
                await c.execute_query('SELECT 1')


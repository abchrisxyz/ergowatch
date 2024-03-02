import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB


@pytest.fixture(scope="module")
def client():
    schema_paths = ["ew/src/core/store/schema.sql"]
    sql = f"""
        insert into core.headers (height, timestamp, header_id, parent_id, main_chain) values 
        (10, 1567123456789, 'header10', 'header09', True),
        (20, 1568123456789, 'header20', 'header19', True),
        (30, 1569123456789, 'header30', 'header29', True);
    """
    with MockDB(schema_paths, sql=sql) as _:
        with TestClient(app) as client:
            yield client


def test_status(client):
    url = "/sync_height"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == {
        "height": 30,
    }

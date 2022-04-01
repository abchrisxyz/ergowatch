import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB


@pytest.fixture(scope="module")
def client():
    sql = f"""
        insert into core.headers (height, id, parent_id, timestamp) values 
        (10, 'header10', 'header09', 1567123456789),
        (20, 'header20', 'header19', 1568123456789),
        (30, 'header30', 'header29', 1569123456789);
    """
    with MockDB(sql=sql) as _:
        with TestClient(app) as client:
            yield client


def test_status(client):
    url = "/sync_height"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == {
        "height": 30,
    }

import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB
from ..api.constants import GENESIS_TIMESTAMP


@pytest.fixture(scope="module")
def client():
    schema_paths = ["ew/src/workers/timestamps/store/schema.sql"]
    sql = f"""
        insert into timestamps.timestamps (height, timestamp) values 
        (10, 1567123456789),
        (20, 1568123456789),
        (30, 1569123456789);
    """
    with MockDB(schema_paths=schema_paths, sql=sql) as _:
        with TestClient(app) as client:
            yield client


class TestHeightToTimestamp:
    def test_normal(self, client):
        url = "/utils/height2timestamp/20"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 1568123456789

    def test_height_ge0(self, client):
        url = f"/utils/height2timestamp/-1"
        response = client.get(url)
        assert response.status_code == 422

    def test_future_height(self, client):
        url = f"/utils/height2timestamp/9999"
        response = client.get(url)
        assert response.status_code == 404
        assert response.json()["detail"] == "Not Found"


class TestTimestampToHeight:
    def test_spot_on(self, client):
        url = "/utils/timestamp2height/1568123456789"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 20

    def test_timestamp_between_two_blocks(self, client):
        url = f"/utils/timestamp2height/{1568123456789 + 100_000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 20

    def test_before_geneses(self, client):
        url = f"/utils/timestamp2height/{GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_future_timestamp_returns_last_height(self, client):
        url = "/utils/timestamp2height/999999999999999"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 30

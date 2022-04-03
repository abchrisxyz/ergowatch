import pytest

from fastapi.testclient import TestClient

from ...main import app
from ..db import MockDB

# Change history limit for easier testing
from ...api.routes.metrics import utxos

utxos.HISTORY_LIMIT = 3


@pytest.fixture(scope="module")
def client():
    sql = f"""
        insert into mtr.utxos (height, value) values 
        (0, 3),
        (1, 4),
        (2, 5),
        (3, 6),
        (4, 7),
        (5, 9);
    """
    with MockDB(sql=sql) as _:
        with TestClient(app) as client:
            yield client


class TestCount:
    def test_default(self, client):
        url = "/metrics/utxos/count"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 9


class TestCountAtHeight:
    def test_valid(self, client):
        url = "/metrics/utxos/count/at/height/3"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 6

    def test_negative(self, client):
        url = "/metrics/utxos/count/at/height/-1"
        response = client.get(url)
        assert response.status_code == 422

    def test_gt_sync_height(self, client):
        url = "/metrics/utxos/count/at/height/6"
        response = client.get(url)
        assert response.status_code == 404


class TestCountHistory:
    def test_default(self, client):
        url = "/metrics/utxos/count/history"
        response = client.get(url)
        assert response.status_code == 200
        # Limited to first x records by history limit
        assert response.json() == [3, 4, 5]

    def test_from_to(self, client):
        url = "/metrics/utxos/count/history?from_height=2&to_height=4"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [5, 6, 7]

    def test_from_only(self, client):
        url = "/metrics/utxos/count/history?from_height=2"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [5, 6, 7]

    def test_out_of_range(self, client):
        url = "/metrics/utxos/count/history?from_height=200"
        response = client.get(url)
        print(response.text)
        assert response.status_code == 200
        assert response.json() == []

    def test_to_only(self, client):
        url = "/metrics/utxos/count/history?to_height=4"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [5, 6, 7]

    def test_from_negative(self, client):
        url = "/metrics/utxos/count/history?from_height=-5"
        response = client.get(url)
        assert response.status_code == 422

    def test_to_negative(self, client):
        url = "/metrics/utxos/count/history?to_height=-5"
        response = client.get(url)
        assert response.status_code == 422

    def test_from_ge_to(self, client):
        url = "/metrics/utxos/count/history?to_height=-5"
        response = client.get(url)
        assert response.status_code == 422

    def test_from_ge_to(self, client):
        url = "/metrics/utxos/count/history?from_height=4&to_height=2"
        response = client.get(url)
        assert response.status_code == 422

    def test_to_gt_sync_height(self, client):
        url = "/metrics/utxos/count/history?from_height=4&to_height=6"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [7, 9]

    def test_above_history_limit(self, client):
        url = "/metrics/utxos/count/history?from_height=4&to_height=9999999999"
        response = client.get(url)
        assert response.status_code == 422
        response.json()[
            "detail"
        ] == f"Height interval is limited to {utxos.HISTORY_LIMIT}"

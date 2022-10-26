import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB


@pytest.fixture(scope="module")
def client():
    sql = f"""
        insert into cex.supply (height, cex_id, main, deposit) values
        (10, 1,    0,   5000),
        (20, 1, 3000,   1000),
        (20, 2,    0,   4000),
        (40, 1, 2000,   2500),
        (50, 2, 3500,    500);
        
        insert into core.headers (height, id, parent_id, timestamp, difficulty, vote1, vote2, vote3) values 
        ( 1, 'header01', 'header00', 1560023456789, 111222333, 0, 0, 0),
        (10, 'header10', 'header09', 1561023456789, 111222333, 0, 0, 0),
        (20, 'header20', 'header19', 1562023456789, 111222333, 0, 0, 0),
        (30, 'header30', 'header29', 1563023456789, 111222333, 0, 0, 0),
        (40, 'header40', 'header39', 1564023456789, 111222333, 0, 0, 0),
        (50, 'header50', 'header49', 1565023456789, 111222333, 0, 0, 0);
    """
    with MockDB(sql=sql) as _:
        with TestClient(app) as client:
            yield client


class TestList:
    def test_text_ids(self, client):
        url = "/exchanges"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [
            "coinex",
            "gate",
            "kucoin",
            "probit",
            "tradeogre",
        ]


class TestSupply:
    def test_default(self, client):
        url = "/exchanges/coinex/supply"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1564023456789],
            "main": [2000],
            "deposit": [2500],
        }

        url = "/exchanges/gate/supply"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1565023456789],
            "main": [3500],
            "deposit": [500],
        }

    def test_since_matching_timestamp(self, client):
        """Since timestamp of a datapoint"""
        url = "/exchanges/coinex/supply?since=1562023456789"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562023456789, 1564023456789],
            "main": [3000, 2000],
            "deposit": [1000, 2500],
        }

    def test_since_matching_height(self, client):
        """Since height of a datapoint"""
        url = "/exchanges/coinex/supply?since=20"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562023456789, 1564023456789],
            "main": [3000, 2000],
            "deposit": [1000, 2500],
        }

    def test_since_partial_timestamp(self, client):
        """Since timestamp between two datapoints"""
        url = "/exchanges/coinex/supply?since=1562523456789"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562023456789, 1564023456789],
            "main": [3000, 2000],
            "deposit": [1000, 2500],
        }

    def test_since_partial_height(self, client):
        """Since height between two datapoints"""
        url = "/exchanges/coinex/supply?since=25"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562023456789, 1564023456789],
            "main": [3000, 2000],
            "deposit": [1000, 2500],
        }

    def test_since_zero(self, client):
        """Since timestamp prior to genesis"""
        url = "/exchanges/coinex/supply?since=0"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561023456789, 1562023456789, 1564023456789],
            "main": [0, 3000, 2000],
            "deposit": [5000, 1000, 2500],
        }

    def test_since_matching_timestamp_limit(self, client):
        """Since timestamp of a datapoint with limit"""
        url = "/exchanges/coinex/supply?since=1562023456789&limit=1"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562023456789],
            "main": [3000],
            "deposit": [1000],
        }

    def test_since_matching_height_limit(self, client):
        """Since height of a datapoint with limit"""
        url = "/exchanges/coinex/supply?since=20&limit=1"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562023456789],
            "main": [3000],
            "deposit": [1000],
        }

    def test_since_zero_limit(self, client):
        """Since timestamp prior to genesis with limit"""
        url = "/exchanges/coinex/supply?since=0&limit=2"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561023456789, 1562023456789],
            "main": [0, 3000],
            "deposit": [5000, 1000],
        }

    def test_unknown_exchange(self, client):
        url = "/exchanges/oops/supply"
        response = client.get(url)
        print(response.json())
        assert response.status_code == 422
        assert (
            "value is not a valid enumeration member;"
            in response.json()["detail"][0]["msg"]
        )

    def test_since_gt0(self, client):
        url = "/exchanges/oops/supply?since=-1"
        response = client.get(url)
        assert response.status_code == 422

    def test_limit_gt0(self, client):
        url = "/exchanges/oops/supply?limit=-1"
        response = client.get(url)
        assert response.status_code == 422

    def test_limit_le10k(self, client):
        url = "/exchanges/oops/supply?limit=10001"
        response = client.get(url)
        assert response.status_code == 422

    def test_limit_le10k(self, client):
        ts_in_seconds = 1_562_023_456
        url = f"/exchanges/coinex/supply?since={ts_in_seconds}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == "`since` timestamp doesn't appear to be in milliseconds"
        )

import os
import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB


@pytest.fixture(scope="module")
def client():
    sql = """
        insert into bal.erg_diffs (address, height, tx_id, value) values
        ('addr1', 10, 'tx_1',   5000),
        ('addr1', 20, 'tx_2',  -2000),
        ('addr2', 20, 'tx_2',   2000),
        ('addr1', 30, 'tx_3',   1000);

        insert into bal.erg (address, value) values
        ('addr1', 4000),
        ('addr2', 2000);

        insert into core.headers (height, id, parent_id, timestamp) values 
        (10, 'header10', 'header09', 1567123456789),
        (20, 'header20', 'header19', 1568123456789),
        (30, 'header30', 'header29', 1569123456789);
    """
    with MockDB(sql=sql) as _:
        with TestClient(app) as client:
            yield client


def test_balance(client):
    response = client.get("/addresses/addr1/balance")
    assert response.status_code == 200
    assert response.json() == 4000


def test_balance_returns_null_for_unknown_address(client):
    response = client.get("/addresses/unknownaddress/balance")
    assert response.status_code == 200
    assert response.json() == None


class TestBalanceAtHeight:
    def test_height_before_first_tx(self, client):
        response = client.get("/addresses/addr1/balance/at/height/5")
        assert response.status_code == 200
        assert response.json() == 0

    def test_height_on_tx(self, client):
        response = client.get("/addresses/addr1/balance/at/height/20")
        assert response.status_code == 200
        assert response.json() == 3000

    def test_height_within_between_txs(self, client):
        response = client.get("/addresses/addr1/balance/at/height/25")
        assert response.status_code == 200
        assert response.json() == 3000

    def test_height_after_last_tx(self, client):
        response = client.get("/addresses/addr1/balance/at/height/100")
        assert response.status_code == 200
        assert response.json() == 4000

    def test_height_gt0(self, client):
        response = client.get("/addresses/addr1/balance/at/height/0")
        assert response.status_code == 422
        response = client.get("/addresses/addr1/balance/at/height/-1")
        assert response.status_code == 422


class TestBalanceAtTimestamp:
    def test_ts_before_first_tx(self, client):
        response = client.get("/addresses/addr1/balance/at/timestamp/1000123456789")
        assert response.status_code == 200
        assert response.json() == 0

    def test_ts_on_tx(self, client):
        response = client.get("/addresses/addr1/balance/at/timestamp/1568123456789")
        assert response.status_code == 200
        assert response.json() == 3000

    def test_ts_within_between_txs(self, client):
        response = client.get("/addresses/addr1/balance/at/timestamp/1568500000000")
        assert response.status_code == 200
        assert response.json() == 3000

    def test_ts_after_last_tx(self, client):
        response = client.get("/addresses/addr1/balance/at/timestamp/2000123456789")
        assert response.status_code == 200
        assert response.json() == 4000

    def test_ts_gt0(self, client):
        response = client.get("/addresses/addr1/balance/at/timestamp/0")
        assert response.status_code == 422
        response = client.get("/addresses/addr1/balance/at/timestamp/-1")
        assert response.status_code == 422


class TestBalanceHistory:
    def test_default(self, client):
        response = client.get("/addresses/addr1/balance/history")
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                30,
                20,
                10,
            ],
            "balances": [
                4000,
                3000,
                5000,
            ],
        }

    def test_timestamps(self, client):
        response = client.get("/addresses/addr1/balance/history?timestamps=true")
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                30,
                20,
                10,
            ],
            "timestamps": [
                1569123456789,
                1568123456789,
                1567123456789,
            ],
            "balances": [
                4000,
                3000,
                5000,
            ],
        }

    def test_asc(self, client):
        response = client.get("/addresses/addr1/balance/history?desc=false")
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                10,
                20,
                30,
            ],
            "balances": [
                5000,
                3000,
                4000,
            ],
        }

    def test_asc_timestamps(self, client):
        response = client.get(
            "/addresses/addr1/balance/history?desc=false&timestamps=true"
        )
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                10,
                20,
                30,
            ],
            "timestamps": [
                1567123456789,
                1568123456789,
                1569123456789,
            ],
            "balances": [
                5000,
                3000,
                4000,
            ],
        }

    def test_limit(self, client):
        response = client.get("/addresses/addr1/balance/history?limit=1")
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                30,
            ],
            "balances": [
                4000,
            ],
        }

    def test_offset_timestamps(self, client):
        response = client.get("/addresses/addr1/balance/history?offset=1&timestamps=1")
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                20,
                10,
            ],
            "timestamps": [
                1568123456789,
                1567123456789,
            ],
            "balances": [
                3000,
                5000,
            ],
        }

    def test_limit_and_offset(self, client):
        response = client.get("/addresses/addr1/balance/history?limit=1&offset=1")
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                20,
            ],
            "balances": [
                3000,
            ],
        }

    def test_nested(self, client):
        response = client.get("/addresses/addr1/balance/history?flat=false")
        assert response.status_code == 200
        assert response.json() == [
            {"height": 30, "balance": 4000},
            {"height": 20, "balance": 3000},
            {"height": 10, "balance": 5000},
        ]

    def test_nested_timestamps(self, client):
        response = client.get(
            "/addresses/addr1/balance/history?flat=false&timestamps=1"
        )
        assert response.status_code == 200
        assert response.json() == [
            {"height": 30, "timestamp": 1569123456789, "balance": 4000},
            {"height": 20, "timestamp": 1568123456789, "balance": 3000},
            {"height": 10, "timestamp": 1567123456789, "balance": 5000},
        ]

    def test_nested_asc_limit(self, client):
        response = client.get(
            "/addresses/addr1/balance/history?flat=false&desc=false&limit=2"
        )
        assert response.status_code == 200
        assert response.json() == [
            {"height": 10, "balance": 5000},
            {"height": 20, "balance": 3000},
        ]

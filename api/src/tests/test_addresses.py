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


def test_balance_history_default(client):
    response = client.get("/addresses/addr1/balance/history")
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


def test_balance_history_asc(client):
    response = client.get("/addresses/addr1/balance/history?desc=false")
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


def test_balance_history_limit(client):
    response = client.get("/addresses/addr1/balance/history?limit=1")
    assert response.status_code == 200
    assert response.json() == {
        "heights": [
            30,
        ],
        "timestamps": [
            1569123456789,
        ],
        "balances": [
            4000,
        ],
    }


def test_balance_history_offset(client):
    response = client.get("/addresses/addr1/balance/history?offset=1")
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


def test_balance_history_limit_and_offset(client):
    response = client.get("/addresses/addr1/balance/history?limit=1&offset=1")
    assert response.status_code == 200
    assert response.json() == {
        "heights": [
            20,
        ],
        "timestamps": [
            1568123456789,
        ],
        "balances": [
            3000,
        ],
    }


def test_balance_history_nested(client):
    response = client.get("/addresses/addr1/balance/history?flat=false")
    assert response.status_code == 200
    assert response.json() == [
        {"height": 30, "timestamp": 1569123456789, "balance": 4000},
        {"height": 20, "timestamp": 1568123456789, "balance": 3000},
        {"height": 10, "timestamp": 1567123456789, "balance": 5000},
    ]


def test_balance_history_nested_asc_limit(client):
    response = client.get(
        "/addresses/addr1/balance/history?flat=false&desc=false&limit=2"
    )
    assert response.status_code == 200
    assert response.json() == [
        {"height": 10, "timestamp": 1567123456789, "balance": 5000},
        {"height": 20, "timestamp": 1568123456789, "balance": 3000},
    ]

import os
import pytest
from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB

ADDR = {
    "9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 1,
    "9addr2axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 2,
    "9addr2bxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 3,
    "9addr4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 4,
    "9addr5axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 5,
    "9addr5bxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 6,
    "9addr7xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 7,
    "1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 8,
    "9contract2xshorterthan51chars": 9,
    "9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx": 10,
    "4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 11,
}

ADDR_SQL = (
    "insert into core.addresses (id, address, spot_height) values "
    + ",".join([f"({i+1}, '{addr}', 1)" for i, addr in enumerate(ADDR)])
    + ";"
)


@pytest.fixture(scope="module")
def client():
    sql = f"""
        {ADDR_SQL}

        insert into adr.erg (address_id, value) values        ({ADDR['9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       5000000),
        ({ADDR['9addr2axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       4000000),
        ({ADDR['9addr2bxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       4000000),
        ({ADDR['9addr4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       3000000),
        ({ADDR['9addr5axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       2000000),
        ({ADDR['9addr5bxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       2000000),
        ({ADDR['9addr7xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       1000000),
        ({ADDR['1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   10000000000),
        ({ADDR['9contract2xshorterthan51chars']},                         20000000000),
        ({ADDR['9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx']}, 30000000000),
        ({ADDR['4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},      4000000);
    """
    with MockDB(sql=sql) as db_name:
        with TestClient(app) as client:
            yield client


def test_top_rank(client):
    response = client.get(
        "/ranking/9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    )
    assert response.status_code == 200
    assert response.json() == {
        "above": None,
        "target": {
            "rank": 1,
            "address": "9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "balance": 5000000,
        },
        "under": {
            "rank": 2,
            "address": "9addr2axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "balance": 4000000,
        },
    }


def test_mid_rank(client):
    response = client.get(
        "/ranking/9addr4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    )
    assert response.status_code == 200
    assert response.json() == {
        "above": {
            "rank": 2,
            "address": "9addr2axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "balance": 4000000,
        },
        "target": {
            "rank": 4,
            "address": "9addr4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "balance": 3000000,
        },
        "under": {
            "rank": 5,
            "address": "9addr5axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "balance": 2000000,
        },
    }


def test_bottom_rank(client):
    response = client.get(
        "/ranking/9addr7xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    )
    assert response.status_code == 200
    assert response.json() == {
        "above": {
            "rank": 5,
            "address": "9addr5axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "balance": 2000000,
        },
        "target": {
            "rank": 7,
            "address": "9addr7xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "balance": 1000000,
        },
        "under": None,
    }


def test_non_p2pk_address_returns_422(client):
    # 51 chars but not starting with 9
    response = client.get(
        "/ranking/1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    )
    assert response.status_code == 422

    # Starting with 9 but less than 51 chars
    response = client.get("/ranking/9contract2xshorterthan51chars")
    assert response.status_code == 422

    # Starting with 9 but more than 51 chars
    response = client.get(
        "/ranking/9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx"
    )
    assert response.status_code == 422


def test_non_existing_p2pk_address_returns_404(client):
    response = client.get(
        "/ranking/9addrxunknownxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Address not found"

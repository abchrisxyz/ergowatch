import pytest
from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB

ADDR = {
    "9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 11,
    "9addr2axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 21,
    "9addr2bxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 31,
    "9addr4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 41,
    "9addr5axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 51,
    "9addr5bxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 61,
    "9addr7xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 71,
    "1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 83,
    "9contract2xshorterthan51chars": 93,
    "9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx": 103,
    "4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 113,
}

ADDR_SQL = (
    "insert into core.addresses (id, spot_height, address) values "
    + ",".join([f"({aid}, 1, '{addr}')" for (addr, aid) in ADDR.items()])
    + ";"
)


@pytest.fixture(scope="module")
def client():
    schema_paths = [
        "ew/src/core/store/schema.sql",
        "ew/src/workers/erg/store/schema.sql",
        "ew/src/workers/tokens/store/schema.sql",
    ]
    sql = f"""
        {ADDR_SQL}

        insert into erg.balances (address_id, nano, mean_age_timestamp) values
        ({ADDR['9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       5000000, 0),
        ({ADDR['9addr2axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       4000000, 0),
        ({ADDR['9addr2bxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       4000000, 0),
        ({ADDR['9addr4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       3000000, 0),
        ({ADDR['9addr5axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       2000000, 0),
        ({ADDR['9addr5bxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       2000000, 0),
        ({ADDR['9addr7xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},       1000000, 0),
        ({ADDR['1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   10000000000, 0),
        ({ADDR['9contract2xshorterthan51chars']},                         20000000000, 0),
        ({ADDR['9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx']}, 30000000000, 0),
        ({ADDR['4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},      4000000, 0);
    """
    with MockDB(schema_paths=schema_paths, sql=sql) as db_name:
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

import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB

TOKEN_A = "tokenaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_B = "tokenbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
TOKEN_X = "validxtokenxidxofxnonxexistingxtokenxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

ASSET_ID_A = 1000
ASSET_ID_B = 2000

ADDR = {
    "addr1": 11,
    "addr2": 22,
    "addr3": 33,
    "addr4": 41,
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

        insert into core.tokens (asset_id, spot_height, token_id) values
        ({ASSET_ID_A}, 1, '{TOKEN_A}'),
        ({ASSET_ID_B}, 1, '{TOKEN_B}');
        
        insert into erg.balances (address_id, nano, mean_age_timestamp) values
        ({ADDR['addr1']}, 4000, 0),
        ({ADDR['addr2']}, 2000, 0),
        ({ADDR['addr3']}, 1000, 0),
        ({ADDR['addr4']}, 5000, 0);

        insert into tokens.balances (address_id, asset_id, value) values
        ({ADDR['addr1']}, '{ASSET_ID_A}', 400),
        ({ADDR['addr1']}, '{ASSET_ID_B}', 800),
        ({ADDR['addr2']}, '{ASSET_ID_A}', 200),
        ({ADDR['addr3']}, '{ASSET_ID_A}', 100),
        ({ADDR['addr4']}, '{ASSET_ID_A}', 500);
    """
    with MockDB(schema_paths=schema_paths, sql=sql) as _:
        with TestClient(app) as client:
            yield client


class TestAddressesByBalance:
    def test_erg(self, client):
        url = "/lists/addresses/by/balance"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [
            {"address": "addr4", "balance": 5000},
            {"address": "addr1", "balance": 4000},
            {"address": "addr2", "balance": 2000},
            {"address": "addr3", "balance": 1000},
        ]

    def test_token(self, client):
        url = f"/lists/addresses/by/balance?token_id={TOKEN_A}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [
            {"address": "addr4", "balance": 500},
            {"address": "addr1", "balance": 400},
            {"address": "addr2", "balance": 200},
            {"address": "addr3", "balance": 100},
        ]

    def test_erg_limit(self, client):
        url = "/lists/addresses/by/balance?limit=3"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [
            {"address": "addr4", "balance": 5000},
            {"address": "addr1", "balance": 4000},
            {"address": "addr2", "balance": 2000},
        ]

    def test_token_limit(self, client):
        url = f"/lists/addresses/by/balance?token_id={TOKEN_A}&limit=3"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [
            {"address": "addr4", "balance": 500},
            {"address": "addr1", "balance": 400},
            {"address": "addr2", "balance": 200},
        ]

    def test_unknown_token(self, client):
        url = f"/lists/addresses/by/balance?token_id={TOKEN_X}"
        response = client.get(url)
        assert response.status_code == 404
        assert response.json()["detail"] == "Token not found"

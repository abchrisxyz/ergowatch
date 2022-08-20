import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB

TOKEN_A = "tokenaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_B = "tokenbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
TOKEN_X = "validxtokenxidxofxnonxexistingxtokenxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

ADDR = {
    "addr1": 1,
    "addr2": 2,
    "addr3": 3,
    "addr4": 4,
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
        
        insert into bal.erg (address_id, value) values
        ({ADDR['addr1']}, 4000),
        ({ADDR['addr2']}, 2000),
        ({ADDR['addr3']}, 1000),
        ({ADDR['addr4']}, 5000);

        insert into bal.tokens (address_id, token_id, value) values
        ({ADDR['addr1']}, '{TOKEN_A}', 400),
        ({ADDR['addr1']}, '{TOKEN_B}', 800),
        ({ADDR['addr2']}, '{TOKEN_A}', 200),
        ({ADDR['addr3']}, '{TOKEN_A}', 100),
        ({ADDR['addr4']}, '{TOKEN_A}', 500);
    """
    with MockDB(sql=sql) as _:
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

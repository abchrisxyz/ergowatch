import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB

TOKEN_A = "tokenaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_B = "tokenbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbeip4"
TOKEN_X = "validxtokenxidxofxnonxexistingxtokenxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

P2PK = "9" * 51
ADDR = {
    "addr1": 1,
    "addr2": 2,
    P2PK: 3,
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

        insert into core.headers (height, id, parent_id, timestamp, difficulty, vote1, vote2, vote3) values 
        (10, 'header10', 'header09', 1567123456789, 111222333, 0, 0, 0),
        (20, 'header20', 'header19', 1568123456789, 111222333, 0, 0, 0),
        (30, 'header30', 'header29', 1569123456789, 111222333, 0, 0, 0);

        insert into core.transactions (id, header_id, height, index) values 
        ('tx-1', 'header10', 10, 0),
        ('tx-2', 'header20', 20, 0);

        insert into core.outputs(box_id, tx_id, header_id, creation_height, address_id, index, value, size) values
        ('box-1', 'tx-1', 'header10', 10, {ADDR['addr1']}, 0, 10000000, 123),
        ('box-2', 'tx-2', 'header20', 20, {ADDR['addr1']}, 0, 10000000, 123);
        
        insert into core.tokens (id, box_id, emission_amount) values
        ('{TOKEN_A}', 'box-1', 900);
        insert into core.tokens (id, box_id, emission_amount, name, description, decimals, standard) values
        ('{TOKEN_B}', 'box-2', 800, 'token_b', 'description of token b', 2, 'EIP-4');

        insert into bal.tokens_diffs (address_id, token_id, height, tx_id, value) values
        ({ADDR['addr1']}, '{TOKEN_A}', 10, 'tx_1',   900),
        ({ADDR['addr1']}, '{TOKEN_B}', 10, 'tx_2',   800),
        ({ADDR['addr1']}, '{TOKEN_A}', 20, 'tx_2',  -200),
        ({ADDR['addr2']}, '{TOKEN_A}', 20, 'tx_2',   150),
        ({ADDR['addr1']}, '{TOKEN_A}', 30, 'tx_3',  -300),
        ({ADDR[P2PK]}, '{TOKEN_A}', 30, 'tx_3',   300);

        insert into bal.tokens (address_id, token_id, value) values
        ({ADDR['addr1']}, '{TOKEN_A}', 400),
        ({ADDR['addr1']}, '{TOKEN_B}', 800),
        ({ADDR['addr2']}, '{TOKEN_A}', 150),
        ({ADDR[P2PK]}, '{TOKEN_A}', 300);
    """
    with MockDB(sql=sql) as _:
        with TestClient(app) as client:
            yield client


class TestDetails:
    def test_dummy_token(self, client):
        url = f"/tokens/{TOKEN_A}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "token_id": TOKEN_A,
            "emission_amount": 900,
            "name": None,
            "description": None,
            "decimals": 0,
            "standard": None,
        }

    def test_eip4_token(self, client):
        url = f"/tokens/{TOKEN_B}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "token_id": TOKEN_B,
            "name": "token_b",
            "description": "description of token b",
            "emission_amount": 800,
            "decimals": 2,
            "standard": "EIP-4",
        }

    def test_unknown_token(self, client):
        url = f"/tokens/{TOKEN_X}"
        response = client.get(url)
        assert response.status_code == 404


class TestSupply:
    def test_supply(self, client):
        url = f"/tokens/{TOKEN_A}/supply"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "emitted": 900,
            "in_p2pks": 300,
            "in_contracts": 550,
            "burned": 50,
        }

    def test_unknown_token(self, client):
        url = f"/tokens/{TOKEN_X}/supply"
        response = client.get(url)
        assert response.status_code == 404

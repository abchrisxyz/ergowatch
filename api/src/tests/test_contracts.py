import os
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
    "9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 11,
    "9addr2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 21,
    "9addr3xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 31,
    "1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 43,
    "9contract2xshorterthan51chars": 53,
    "9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx": 63,
    "4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 73,
    "4biscontractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 83,
    "5contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 93,
}


@pytest.fixture(scope="module")
def client():
    schema_paths = [
        "ew/src/core/store/schema.sql",
        "ew/src/workers/erg/store/schema.sql",
        "ew/src/workers/tokens/store/schema.sql",
    ]
    sql = f"""
        insert into core.tokens (asset_id, spot_height, token_id) values
        ({ASSET_ID_A}, 1, '{TOKEN_A}'),
        ({ASSET_ID_B}, 1, '{TOKEN_B}');

        insert into erg.balances (address_id, nano, mean_age_timestamp) values
        ({ADDR['9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   1000000, 0),
        ({ADDR['9addr2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   2000000, 0),
        ({ADDR['9addr3xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   3000000, 0),
        ({ADDR['1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   1000000, 0),
        ({ADDR['9contract2xshorterthan51chars']},                         2000000, 0),
        ({ADDR['9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx']}, 3000000, 0),
        ({ADDR['4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},  4000000, 0),
        ({ADDR['5contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},  5000000, 0);

        insert into tokens.balances (address_id, asset_id, value) values
        ({ADDR['9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{ASSET_ID_A}',   1000000),
        ({ADDR['9addr2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{ASSET_ID_A}',   2000000),
        ({ADDR['9addr3xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{ASSET_ID_A}',   3000000),
        ({ADDR['1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{ASSET_ID_A}',   1000000),
        ({ADDR['9contract2xshorterthan51chars']}, '{ASSET_ID_A}',                         2000000),
        ({ADDR['9contract2xshorterthan51chars']}, '{ASSET_ID_B}',                         2000000),
        ({ADDR['9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{ASSET_ID_A}', 3000000),
        ({ADDR['9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{ASSET_ID_B}', 3000000),
        ({ADDR['4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{ASSET_ID_A}',  4000000),
        ({ADDR['4biscontractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{ASSET_ID_A}',  4000000),
        ({ADDR['5contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{ASSET_ID_A}',  5000000);
        
    """
    with MockDB(schema_paths=schema_paths, sql=sql) as db_name:
        with TestClient(app) as client:
            yield client


class TestCount:
    def test_count_total(self, client):
        url = "/contracts/count"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 5

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 6

    def test_count_query_ge(self, client):
        url = "/contracts/count?bal_ge=2000000"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 4

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 5

    def test_count_query_lt(self, client):
        url = "/contracts/count?bal_lt=4000000"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 3

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 3

    def test_count_query_ge_lt(self, client):
        url = "/contracts/count?bal_ge=2000000&bal_lt=4000000"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 2

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 2

    def test_count_zero(self, client):
        url = "/contracts/count?bal_ge=999999999999"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 0

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 0

    def test_count_query_ge_cannot_be_negative(self, client):
        url = "/contracts/count?bal_ge=-2000000"
        response = client.get(url)
        assert response.status_code == 422

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 422

    def test_count_query_lt_cannot_be_negative(self, client):
        url = "/contracts/count?bal_lt=-2000000"
        response = client.get(url)
        assert response.status_code == 422

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 422

    def test_unknown_token_id(self, client):
        url = "/contracts/count"
        response = client.get(url + f"?token_id={TOKEN_X}")
        assert response.status_code == 200
        assert response.json() == 0


class TestSupply:
    def test_default(self, client):
        url = "/contracts/supply"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 15_000_000

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 19_000_000

    def test_unknown_token_id(self, client):
        url = "/contracts/supply"
        response = client.get(url + f"?token_id={TOKEN_X}")
        assert response.status_code == 404

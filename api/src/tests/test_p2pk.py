import os
import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB


TOKEN_A = "tokenaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_B = "tokenbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
TOKEN_X = "validxtokenxidxofxnonxexistingxtokenxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

ADDR = {
    "9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 1,
    "9addr2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 2,
    "9addr3xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 3,
    "9addr3bisxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 4,
    "9addr4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 5,
    "9addr5xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 6,
    "1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 7,
    "9contract2xshorterthan51chars": 8,
    "9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx": 9,
    "4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx": 10,
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
        
        insert into adr.erg (address_id, value) values        ({ADDR['9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   1000000),
        ({ADDR['9addr2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   2000000),
        ({ADDR['9addr3xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   3000000),
        ({ADDR['9addr4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   4000000),
        ({ADDR['9addr5xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   5000000),
        ({ADDR['1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},   1000000),
        ({ADDR['9contract2xshorterthan51chars']},                         2000000),
        ({ADDR['9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx']}, 3000000),
        ({ADDR['4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']},  4000000);

        insert into adr.tokens (address_id, token_id, value) values        ({ADDR['9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{TOKEN_A}',   1000000),
        ({ADDR['9addr2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{TOKEN_A}',   2000000),
        ({ADDR['9addr3xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{TOKEN_A}',   3000000),
        ({ADDR['9addr3xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{TOKEN_B}',   3000000),
        ({ADDR['9addr3bisxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{TOKEN_A}',   3000000),
        ({ADDR['9addr4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{TOKEN_A}',   4000000),
        ({ADDR['9addr5xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{TOKEN_A}',   5000000),
        ({ADDR['1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{TOKEN_A}',   1000000),
        ({ADDR['9contract2xshorterthan51chars']}, '{TOKEN_A}',                         2000000),
        ({ADDR['9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{TOKEN_A}', 3000000),
        ({ADDR['4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx']}, '{TOKEN_A}',  4000000);
    """
    with MockDB(sql=sql) as db_name:
        with TestClient(app) as client:
            yield client


def test_count_total(client):
    url = "/p2pk/count"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == 5

    response = client.get(url + f"?token_id={TOKEN_A}")
    assert response.status_code == 200
    assert response.json() == 6


def test_count_query_ge(client):
    url = "/p2pk/count?bal_ge=2000000"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == 4

    response = client.get(url + f"&token_id={TOKEN_A}")
    assert response.status_code == 200
    assert response.json() == 5


def test_count_query_lt(client):
    url = "/p2pk/count?bal_lt=4000000"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == 3

    response = client.get(url + f"&token_id={TOKEN_A}")
    assert response.status_code == 200
    assert response.json() == 4


def test_count_query_ge_lt(client):
    url = "/p2pk/count?bal_ge=2000000&bal_lt=4000000"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == 2

    response = client.get(url + f"&token_id={TOKEN_A}")
    assert response.status_code == 200
    assert response.json() == 3


def test_count_zero(client):
    url = "/p2pk/count?bal_ge=999999999999"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == 0

    response = client.get(url + f"&token_id={TOKEN_A}")
    assert response.status_code == 200
    assert response.json() == 0


def test_count_query_ge_cannot_be_negative(client):
    url = "/p2pk/count?bal_ge=-2000000"
    response = client.get(url)
    assert response.status_code == 422

    response = client.get(url + f"&token_id={TOKEN_A}")
    assert response.status_code == 422


def test_count_query_lt_cannot_be_negative(client):
    url = "/p2pk/count?bal_lt=-2000000"
    response = client.get(url)
    assert response.status_code == 422

    response = client.get(url + f"&token_id={TOKEN_A}")
    assert response.status_code == 422


def test_unknown_token_id(client):
    url = "/p2pk/count"
    response = client.get(url + f"?token_id={TOKEN_X}")
    assert response.status_code == 200
    assert response.json() == 0

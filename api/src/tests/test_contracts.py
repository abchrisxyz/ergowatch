import os
import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB


@pytest.fixture(scope="module")
def client():
    sql = """
        insert into bal.erg (address, value) values
        ('9addr1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',   1000000),
        ('9addr2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',   2000000),
        ('9addr3xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',   3000000),
        ('1contract1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',   1000000),
        ('9contract2xshorterthan51chars',                         2000000),
        ('9contract3xlongerthan51charsxxxxxxxxxxxxxxxxxxxxxxxxx', 3000000),
        ('4contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',  4000000),
        ('5contractxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',  5000000);
    """
    with MockDB(sql=sql) as db_name:
        with TestClient(app) as client:
            yield client


def test_count_total(client):
    response = client.get("/contracts/count")
    assert response.status_code == 200
    assert response.json() == 5


def test_count_query_ge(client):
    response = client.get("/contracts/count?bal_ge=2000000")
    assert response.status_code == 200
    assert response.json() == 4


def test_count_query_lt(client):
    response = client.get("/contracts/count?bal_lt=4000000")
    assert response.status_code == 200
    assert response.json() == 3


def test_count_query_ge_lt(client):
    response = client.get("/contracts/count?bal_ge=2000000&bal_lt=4000000")
    assert response.status_code == 200
    assert response.json() == 2


def test_count_zero(client):
    response = client.get("/contracts/count?bal_ge=999999999999")
    assert response.status_code == 200
    assert response.json() == 0


def test_count_query_ge_cannot_be_negative(client):
    response = client.get("/contracts/count?bal_ge=-2000000")
    assert response.status_code == 422


def test_count_query_lt_cannot_be_negative(client):
    response = client.get("/contracts/count?bal_lt=-2000000")
    assert response.status_code == 422

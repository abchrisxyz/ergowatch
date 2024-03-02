import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB

ADDR_ID_1 = 1001
ADDR_ID_2 = 2002
ADDR_1 = "addr1"
ADDR_2 = "addr2"

TOKEN_A = "token1"
TOKEN_B = "token2"
TOKEN_X = "badtoken"
ASSET_ID_A = 1000
ASSET_ID_B = 2000


@pytest.fixture(scope="module")
def client():
    schema_paths = [
        "ew/src/core/store/schema.sql",
        "ew/src/workers/erg_diffs/store/schema.sql",
        "ew/src/workers/erg/store/schema.sql",
        "ew/src/workers/tokens/store/schema.sql",
        "ew/src/workers/timestamps/store/schema.sql",
    ]
    sql = f"""
        insert into core.addresses (id, spot_height, address) values
        ({ADDR_ID_1}, 10, '{ADDR_1}'),
        ({ADDR_ID_2}, 20, '{ADDR_2}');

        insert into core.tokens (asset_id, spot_height, token_id) values
        ({ASSET_ID_A}, 100, '{TOKEN_A}'),
        ({ASSET_ID_B}, 200, '{TOKEN_B}');

        insert into erg.balance_diffs (address_id, height, tx_idx, nano) values
        ({ADDR_ID_1}, 10, 0,   5000),
        ({ADDR_ID_1}, 20, 0,  -2000),
        ({ADDR_ID_2}, 20, 0,   2000),
        ({ADDR_ID_1}, 30, 0,   1000);

        insert into erg.balances (address_id, nano, mean_age_timestamp) values
        ({ADDR_ID_1}, 4000, 0),
        ({ADDR_ID_2}, 2000, 0);

        insert into tokens.balance_diffs (address_id, asset_id, height, tx_idx, value) values
        ({ADDR_ID_1}, '{ASSET_ID_A}', 10, 0,   500),
        ({ADDR_ID_1}, '{ASSET_ID_B}', 10, 0,   800),
        ({ADDR_ID_1}, '{ASSET_ID_A}', 20, 0,  -200),
        ({ADDR_ID_2}, '{ASSET_ID_A}', 20, 0,   200),
        ({ADDR_ID_1}, '{ASSET_ID_A}', 30, 0,   100);

        insert into tokens.balances (address_id, asset_id, value) values
        ({ADDR_ID_1}, '{ASSET_ID_A}', 400),
        ({ADDR_ID_1}, '{ASSET_ID_B}', 800),
        ({ADDR_ID_2}, '{ASSET_ID_A}', 200);

        insert into timestamps.timestamps (height, timestamp) values 
        (10, 1567123456789),
        (20, 1568123456789),
        (30, 1569123456789);
    """
    with MockDB(schema_paths=schema_paths, sql=sql) as _:
        with TestClient(app) as client:
            yield client


class TestBalance:
    def test_balance(self, client):
        url = "/addresses/addr1/balance"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 4000

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 400

    def test_unknown_address(self, client):
        response = client.get("/addresses/unknownaddress/balance")
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"

    def test_unknown_token(self, client):
        response = client.get(f"/addresses/addr1/balance?token_id={TOKEN_X}")
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"


class TestBalanceAtHeight:
    def test_height_before_first_tx(self, client):
        url = "/addresses/addr1/balance/at/height/5"
        response = client.get(url)
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"

    def test_height_on_tx(self, client):
        url = "/addresses/addr1/balance/at/height/20"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 3000

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 300

    def test_height_within_between_txs(self, client):
        url = "/addresses/addr1/balance/at/height/25"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 3000

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 300

    def test_height_after_last_tx(self, client):
        url = "/addresses/addr1/balance/at/height/100"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 4000

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 400

    def test_height_ge0(self, client):
        #  Height 0 is allowed, but 404 because no balance found
        url = "/addresses/addr1/balance/at/height/0"
        response = client.get(url)
        assert response.status_code == 404

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 404

        #  Negative height is not allowed, expect 422
        url = "/addresses/addr1/balance/at/height/-1"
        response = client.get(url)
        assert response.status_code == 422

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 422

    def test_unknown_address(self, client):
        response = client.get("/addresses/unknownaddress/balance/at/height/20")
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"

    def test_unknown_token(self, client):
        response = client.get(
            f"/addresses/addr1/balance/at/height/20?token_id={TOKEN_X}"
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"


class TestBalanceAtTimestamp:
    def test_ts_before_first_tx(self, client):
        url = "/addresses/addr1/balance/at/timestamp/1000123456789"
        response = client.get(url)
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"

    def test_ts_on_tx(self, client):
        url = "/addresses/addr1/balance/at/timestamp/1568123456789"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 3000

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 300

    def test_ts_within_between_txs(self, client):
        url = "/addresses/addr1/balance/at/timestamp/1568500000000"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 3000

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 300

    def test_ts_after_last_tx(self, client):
        url = "/addresses/addr1/balance/at/timestamp/2000123456789"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == 4000

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == 400

    def test_ts_gt0(self, client):
        url = "/addresses/addr1/balance/at/timestamp/0"
        response = client.get(url)
        assert response.status_code == 422

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 422

        url = "/addresses/addr1/balance/at/timestamp/-1"
        response = client.get(url)
        assert response.status_code == 422

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 422

    def test_unknown_address(self, client):
        response = client.get(
            "/addresses/unknwonaddress/balance/at/timestamp/1568123456789"
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"

    def test_unknown_token(self, client):
        response = client.get(
            f"/addresses/addr1/balance/at/timestamp/1568123456789?token_id={TOKEN_X}"
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"


class TestBalanceHistory:
    def test_default(self, client):
        url = "/addresses/addr1/balance/history"
        response = client.get(url)
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

        response = client.get(url + f"?token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                30,
                20,
                10,
            ],
            "balances": [
                400,
                300,
                500,
            ],
        }

    def test_timestamps(self, client):
        url = "/addresses/addr1/balance/history?timestamps=true"
        response = client.get(url)
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

        response = client.get(url + f"&token_id={TOKEN_A}")
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
                400,
                300,
                500,
            ],
        }

    def test_asc(self, client):
        url = "/addresses/addr1/balance/history?desc=false"
        response = client.get(url)
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

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                10,
                20,
                30,
            ],
            "balances": [
                500,
                300,
                400,
            ],
        }

    def test_asc_timestamps(self, client):
        url = "/addresses/addr1/balance/history?desc=false&timestamps=true"
        response = client.get(url)
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

        response = client.get(url + f"&token_id={TOKEN_A}")
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
                500,
                300,
                400,
            ],
        }

    def test_limit(self, client):
        url = "/addresses/addr1/balance/history?limit=1"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                30,
            ],
            "balances": [
                4000,
            ],
        }

        response = client.get(url + "&token_tkna")
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
        url = "/addresses/addr1/balance/history?offset=1&timestamps=1"
        response = client.get(url)
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

        response = client.get(url + f"&token_id={TOKEN_A}")
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
                300,
                500,
            ],
        }

    def test_limit_and_offset(self, client):
        url = "/addresses/addr1/balance/history?limit=1&offset=1"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                20,
            ],
            "balances": [
                3000,
            ],
        }

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == {
            "heights": [
                20,
            ],
            "balances": [
                300,
            ],
        }

    def test_nested(self, client):
        url = "/addresses/addr1/balance/history?flat=false"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [
            {"height": 30, "balance": 4000},
            {"height": 20, "balance": 3000},
            {"height": 10, "balance": 5000},
        ]

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == [
            {"height": 30, "balance": 400},
            {"height": 20, "balance": 300},
            {"height": 10, "balance": 500},
        ]

    def test_nested_timestamps(self, client):
        url = "/addresses/addr1/balance/history?flat=false&timestamps=1"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [
            {"height": 30, "timestamp": 1569123456789, "balance": 4000},
            {"height": 20, "timestamp": 1568123456789, "balance": 3000},
            {"height": 10, "timestamp": 1567123456789, "balance": 5000},
        ]

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == [
            {"height": 30, "timestamp": 1569123456789, "balance": 400},
            {"height": 20, "timestamp": 1568123456789, "balance": 300},
            {"height": 10, "timestamp": 1567123456789, "balance": 500},
        ]

    def test_nested_asc_limit(self, client):
        url = "/addresses/addr1/balance/history?flat=false&desc=false&limit=2"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == [
            {"height": 10, "balance": 5000},
            {"height": 20, "balance": 3000},
        ]

        response = client.get(url + f"&token_id={TOKEN_A}")
        assert response.status_code == 200
        assert response.json() == [
            {"height": 10, "balance": 500},
            {"height": 20, "balance": 300},
        ]

    def test_unknown_address(self, client):
        url = "/addresses/unknownaddress/balance/history"
        response = client.get(url)
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"

    def test_unknown_token_id(self, client):
        url = f"/addresses/addr1/balance/history?token_id={TOKEN_X}"
        response = client.get(url)
        assert response.status_code == 404
        assert response.json()["detail"] == "No balance found"

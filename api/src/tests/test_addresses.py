import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB

TOKEN_A = "tokenaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_B = "tokenbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
TOKEN_X = "validxtokenxidxofxnonxexistingxtokenxxxxxxxxxxxxxxxxxxxxxxxxxxxx"


@pytest.fixture(scope="module")
def client():
    coinex_main = "9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU"
    sql = f"""
        insert into core.addresses (id, address, spot_height, p2pk, miner) values
        (1, 'addr1', 10, True, False),
        (2, 'addr2', 20, True, False),
        (3, '{coinex_main}', 30, True, False);

        insert into adr.erg_diffs (address_id, height, tx_id, value) values
        (1, 10, 'tx_1',   5000),
        (1, 20, 'tx_2',  -2000),
        (2, 20, 'tx_2',   2000),
        (1, 30, 'tx_3',   1000);

        insert into adr.erg (address_id, value, mean_age_timestamp) values
        (1, 4000, 0),
        (2, 2000, 0);

        insert into adr.tokens_diffs (address_id, token_id, height, tx_id, value) values
        (1, '{TOKEN_A}', 10, 'tx_1',   500),
        (1, '{TOKEN_B}', 10, 'tx_1',   800),
        (1, '{TOKEN_A}', 20, 'tx_2',  -200),
        (2, '{TOKEN_A}', 20, 'tx_2',   200),
        (1, '{TOKEN_A}', 30, 'tx_3',   100);

        insert into adr.tokens (address_id, token_id, value) values
        (1, '{TOKEN_A}', 400),
        (1, '{TOKEN_B}', 800),
        (2, '{TOKEN_A}', 200);

        insert into core.headers (height, id, parent_id, timestamp, difficulty, vote1, vote2, vote3) values 
        (10, 'header10', 'header09', 1567123456789, 111222333, 0, 0, 0),
        (20, 'header20', 'header19', 1568123456789, 111122233, 0, 0, 0),
        (30, 'header30', 'header29', 1569123456789, 111222333, 0, 0, 0);

        insert into cex.main_addresses (address_id, cex_id) values
        (3, 1);
    """
    with MockDB(sql=sql) as _:
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
        response = client.get("/addresses/addr1/balance/history")
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


class TestTags:
    def test_predefined_tags(self, client):
        treasury = "4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy"
        url = f"/addresses/{treasury}/tags"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == ["ef-treasury"]

    def test_exchange_tags(self, client):
        coinex_main = "9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU"
        url = f"/addresses/{coinex_main}/tags"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == ["exchange", "exchange-main", "exchange-coinex"]

    def test_unknown_address(self, client):
        url = f"/addresses/unknown/tags"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == []

    def test_nonvallid_address(self, client):
        url = f"/addresses/not_good/tags"
        response = client.get(url)
        assert response.status_code == 422

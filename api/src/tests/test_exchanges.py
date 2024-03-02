import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB


@pytest.fixture(scope="module")
def client():
    schema_paths = [
        "ew/src/core/store/schema.sql",
        "ew/src/workers/erg/store/schema.sql",
        "ew/src/workers/exchanges/store/schema.sql",
    ]
    sql = f"""
        -- Remove actual exchange data so we can assert things
        truncate table exchanges.exchanges;
        truncate table exchanges.main_addresses;

        insert into core.addresses (id, spot_height, address) values
        (11, 1, 'address_a'),
        (21, 1, 'address_b'),
        (31, 1, 'address_c'),
        (41, 1, 'address_d'),
        (51, 1, 'address_e');

        insert into exchanges.exchanges (id, name, text_id) values
        (1, 'Coinex', 'coinex'),
        (2, 'Gate.io', 'gate'),
        (3, 'KuCoin', 'kucoin');

        insert into exchanges.main_addresses (cex_id, address_id, address) values
        (1, 11, 'address_a'),
        (1, 21, 'address_b'),
        (2, 31, 'address_c'),
        (1, 41, 'address_d'),
        (3, 51, 'address_e');
        
        insert into erg.balances (address_id, nano, mean_age_timestamp) values 
        (11, 1000000, 0),
        -- 21 is spent
        (31, 3000000, 0),
        (41, 4000000, 0),
        (51, 5000000, 0);
    """
    with MockDB(schema_paths=schema_paths, sql=sql) as _:
        with TestClient(app) as client:
            yield client


class TestTrackList:
    def test_response(self, client):
        url = "/exchanges/tracklist"
        response = client.get(url)
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 5
        assert {"cex": "coinex", "address": "address_d", "balance": 4000000} in data

import pytest

from fastapi.testclient import TestClient

from ..main import app
from .db import MockDB

# Contract deployment date, as defined in sigusd worker/schema.
# The worker (and it's schema) will be initialized at that height,
# so history will always start here.
LAUNCH_HEIGHT = 453064


@pytest.fixture(scope="module")
def client():
    schema_paths = [
        "ew/src/framework/ew.sql",
        "ew/src/core/store/schema.sql",
        "ew/src/workers/sigmausd/store/schema.sql",
    ]
    sql = f"""  
        insert into sigmausd.history (height, oracle, circ_sc, circ_rc, reserves, sc_nano_net, rc_nano_net)
        values
            ({LAUNCH_HEIGHT + 1}, 101, 102, 103, 104, 105, 106),
            ({LAUNCH_HEIGHT + 2}, 201, 202, 203, 204, 205, 206);
    """
    with MockDB(schema_paths=schema_paths, sql=sql) as _:
        with TestClient(app) as client:
            yield client


class TestState:
    def test_state(self, client):
        url = "/sigmausd/state"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "reserves": 204,
            "circ_sigusd": 2.02,  # two decimals
            "circ_sigrsv": 203,
            "peg_rate_nano": 201,
        }

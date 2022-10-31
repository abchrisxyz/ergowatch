import pytest
from typing import List

from fastapi.testclient import TestClient

from ...main import app
from ..db import MockDB

from ...api.routes import metrics

# Change time window limit for easier testing
LIMIT = 3
limits = metrics.generate_time_window_limits(LIMIT)
metrics.supply_distribution.TimeWindowLimits = limits
GENESIS_TIMESTAMP = metrics.GENESIS_TIMESTAMP

BLOCK_DT = 120_000
HOUR_DT = 3_600_000
DAY_DT = 86_400_000

LAST_TS = 1600000000000
BLOCK_TSS = [LAST_TS - i * BLOCK_DT for i in (5, 4, 3, 2, 1, 0)]
HOUR_TSS = [LAST_TS - i * HOUR_DT for i in (5, 4, 3, 2, 1, 0)]
DAY_TSS = [LAST_TS - i * DAY_DT for i in (5, 4, 3, 2, 1, 0)]

LAST_H = 6000
BLOCK_HS = [LAST_H - i * 1 for i in (5, 4, 3, 2, 1, 0)]
HOUR_HS = [LAST_H - i * 100 for i in (5, 4, 3, 2, 1, 0)]
DAY_HS = [LAST_H - i * 1000 for i in (5, 4, 3, 2, 1, 0)]

LAST_VAL = [600001, 600002, 600003, 600004]
NCOLS = len(LAST_VAL)
BLOCK_VALS = [[h * 100 + i for i in range(1, NCOLS + 1)] for h in BLOCK_HS]
HOUR_VALS = [[h * 100 + i for i in range(1, NCOLS + 1)] for h in HOUR_HS]
DAY_VALS = [[h * 100 + i for i in range(1, NCOLS + 1)] for h in DAY_HS]
assert LAST_VAL == BLOCK_VALS[-1] == HOUR_VALS[-1] == DAY_VALS[-1]

# Dummy circualting supply values = negative height
BLOCK_SCS = [-h for h in BLOCK_HS]
HOUR_SCS = [-h for h in HOUR_HS]
DAY_SCS = [-h for h in DAY_HS]
LAST_SC = BLOCK_SCS[-1]
assert LAST_SC == BLOCK_SCS[-1] == HOUR_SCS[-1] == DAY_SCS[-1]


def to_sql(vals: List) -> str:
    """Converts list of values to sql statement fragment"""
    return ",".join([str(x) for x in vals])


@pytest.fixture(scope="module", params=["p2pk", "contracts", "miners"])
def client(request):
    address_type = request.param
    # Difficulty and votes
    dvs = "111222333, 0, 0, 0"
    sql = f"""
        insert into core.headers (difficulty, vote1, vote2, vote3, height, id, parent_id, timestamp) values 
        -- block level data
        ({dvs}, {BLOCK_HS[0]}, 'header1', 'header0', {BLOCK_TSS[0]}),
        ({dvs}, {BLOCK_HS[1]}, 'header2', 'header1', {BLOCK_TSS[1]}),
        ({dvs}, {BLOCK_HS[2]}, 'header3', 'header2', {BLOCK_TSS[2]}),
        ({dvs}, {BLOCK_HS[3]}, 'header4', 'header3', {BLOCK_TSS[3]}),
        ({dvs}, {BLOCK_HS[4]}, 'header5', 'header4', {BLOCK_TSS[4]}),
        ({dvs}, {BLOCK_HS[5]}, 'header6', 'header5', {BLOCK_TSS[5]});

        insert into mtr.timestamps_hourly(height, timestamp) values
        ({HOUR_HS[0]}, {HOUR_TSS[0]}),
        ({HOUR_HS[1]}, {HOUR_TSS[1]}),
        ({HOUR_HS[2]}, {HOUR_TSS[2]}),
        ({HOUR_HS[3]}, {HOUR_TSS[3]}),
        ({HOUR_HS[4]}, {HOUR_TSS[4]}),
        ({HOUR_HS[5]}, {HOUR_TSS[5]});

        insert into mtr.timestamps_daily(height, timestamp) values
        ({DAY_HS[0]}, {DAY_TSS[0]}),
        ({DAY_HS[1]}, {DAY_TSS[1]}),
        ({DAY_HS[2]}, {DAY_TSS[2]}),
        ({DAY_HS[3]}, {DAY_TSS[3]}),
        ({DAY_HS[4]}, {DAY_TSS[4]}),
        ({DAY_HS[5]}, {DAY_TSS[5]});

        -- values = height * 10 + 1
        insert into mtr.supply_on_top_addresses_{address_type} (height, top_1_prc, top_1k, top_100, top_10) values 
        ({DAY_HS[0]}, {to_sql(DAY_VALS[0])}),
        ({DAY_HS[1]}, {to_sql(DAY_VALS[1])}),
        ({DAY_HS[2]}, {to_sql(DAY_VALS[2])}),
        ({DAY_HS[3]}, {to_sql(DAY_VALS[3])}),
        ({DAY_HS[4]}, {to_sql(DAY_VALS[4])}),
        ({HOUR_HS[0]}, {to_sql(HOUR_VALS[0])}),
        ({HOUR_HS[1]}, {to_sql(HOUR_VALS[1])}),
        ({HOUR_HS[2]}, {to_sql(HOUR_VALS[2])}),
        ({HOUR_HS[3]}, {to_sql(HOUR_VALS[3])}),
        ({HOUR_HS[4]}, {to_sql(HOUR_VALS[4])}),
        ({BLOCK_HS[0]}, {to_sql(BLOCK_VALS[0])}),
        ({BLOCK_HS[1]}, {to_sql(BLOCK_VALS[1])}),
        ({BLOCK_HS[2]}, {to_sql(BLOCK_VALS[2])}),
        ({BLOCK_HS[3]}, {to_sql(BLOCK_VALS[3])}),
        ({BLOCK_HS[4]}, {to_sql(BLOCK_VALS[4])}),
        ({BLOCK_HS[5]}, {to_sql(BLOCK_VALS[5])});

        insert into blk.stats (height, circulating_supply, emission, reward, tx_fees, tx_count, volume) values
        ({DAY_HS[0]}, {DAY_SCS[0]}, 0, 0, 0, 0, 0),
        ({DAY_HS[1]}, {DAY_SCS[1]}, 0, 0, 0, 0, 0),
        ({DAY_HS[2]}, {DAY_SCS[2]}, 0, 0, 0, 0, 0),
        ({DAY_HS[3]}, {DAY_SCS[3]}, 0, 0, 0, 0, 0),
        ({DAY_HS[4]}, {DAY_SCS[4]}, 0, 0, 0, 0, 0),
        ({HOUR_HS[0]}, {HOUR_SCS[0]}, 0, 0, 0, 0, 0),
        ({HOUR_HS[1]}, {HOUR_SCS[1]}, 0, 0, 0, 0, 0),
        ({HOUR_HS[2]}, {HOUR_SCS[2]}, 0, 0, 0, 0, 0),
        ({HOUR_HS[3]}, {HOUR_SCS[3]}, 0, 0, 0, 0, 0),
        ({HOUR_HS[4]}, {HOUR_SCS[4]}, 0, 0, 0, 0, 0),
        ({BLOCK_HS[0]}, {BLOCK_SCS[0]}, 0, 0, 0, 0, 0),
        ({BLOCK_HS[1]}, {BLOCK_SCS[1]}, 0, 0, 0, 0, 0),
        ({BLOCK_HS[2]}, {BLOCK_SCS[2]}, 0, 0, 0, 0, 0),
        ({BLOCK_HS[3]}, {BLOCK_SCS[3]}, 0, 0, 0, 0, 0),
        ({BLOCK_HS[4]}, {BLOCK_SCS[4]}, 0, 0, 0, 0, 0),
        ({BLOCK_HS[5]}, {BLOCK_SCS[5]}, 0, 0, 0, 0, 0);
    """
    with MockDB(sql=sql) as _:
        with TestClient(app) as client:
            client.address_type = address_type
            yield client


def base_url(address_type: str, r: str):
    return f"/metrics/supply/distribution/{address_type}?r={r}"


class TestCountBlock:
    r = "block"

    def test_default(self, client):
        url = base_url(client.address_type, self.r)
        response = client.get(url)
        assert response.status_code == 200
        # Return last block record
        assert response.json() == {
            "timestamps": [LAST_TS],
            "top_1prc": [LAST_VAL[0]],
            "top_1k": [LAST_VAL[1]],
            "top_100": [LAST_VAL[2]],
            "top_10": [LAST_VAL[3]],
            "circ_supply": [LAST_SC],
        }

    def test_from_to(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={BLOCK_TSS[1]}&to={BLOCK_TSS[3]}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": BLOCK_TSS[1:4],
            "top_1prc": [row[0] for row in BLOCK_VALS[1:4]],
            "top_1k": [row[1] for row in BLOCK_VALS[1:4]],
            "top_100": [row[2] for row in BLOCK_VALS[1:4]],
            "top_10": [row[3] for row in BLOCK_VALS[1:4]],
            "circ_supply": BLOCK_SCS[1:4],
        }

    def test_from_only(self, client):
        # from height 1
        url = base_url(client.address_type, self.r)
        url += f"&fr={BLOCK_TSS[1]}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": BLOCK_TSS[1:5],
            "top_1prc": [row[0] for row in BLOCK_VALS[1:5]],
            "top_1k": [row[1] for row in BLOCK_VALS[1:5]],
            "top_100": [row[2] for row in BLOCK_VALS[1:5]],
            "top_10": [row[3] for row in BLOCK_VALS[1:5]],
            "circ_supply": BLOCK_SCS[1:5],
        }

    def test_to_only(self, client):
        # to right after block 5999
        url = base_url(client.address_type, self.r)
        url += f"&to={BLOCK_TSS[4] + 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": BLOCK_TSS[2:5],
            "top_1prc": [row[0] for row in BLOCK_VALS[2:5]],
            "top_1k": [row[1] for row in BLOCK_VALS[2:5]],
            "top_100": [row[2] for row in BLOCK_VALS[2:5]],
            "top_10": [row[3] for row in BLOCK_VALS[2:5]],
            "circ_supply": BLOCK_SCS[2:5],
        }

    def test_from_prior_to_genesis(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_window_size(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={BLOCK_TSS[0]}&to={BLOCK_TSS[5]}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {120000 * LIMIT} for block resolution"
        )


class TestCountHourly:
    r = "1h"

    def test_default(self, client):
        url = base_url(client.address_type, self.r)
        response = client.get(url)
        assert response.status_code == 200
        # Return latest record
        assert response.json() == {
            "timestamps": [LAST_TS],
            "top_1prc": [LAST_VAL[0]],
            "top_1k": [LAST_VAL[1]],
            "top_100": [LAST_VAL[2]],
            "top_10": [LAST_VAL[3]],
            "circ_supply": [LAST_SC],
        }

    def test_from_to(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={HOUR_TSS[1] - 1}&to={HOUR_TSS[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[1:4],
            "top_1prc": [row[0] for row in HOUR_VALS[1:4]],
            "top_1k": [row[1] for row in HOUR_VALS[1:4]],
            "top_100": [row[2] for row in HOUR_VALS[1:4]],
            "top_10": [row[3] for row in HOUR_VALS[1:4]],
            "circ_supply": HOUR_SCS[1:4],
        }

    def test_from_to_window_limit(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={HOUR_TSS[1]}&to={HOUR_TSS[5]}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {LIMIT * HOUR_DT} for 1h resolution"
        )

    def test_from_just_before_new(self, client):
        # from timestamp between h1 and h2, just before h2
        # expect value at h2, h3 and h4
        url = base_url(client.address_type, self.r)
        url += f"&fr={HOUR_TSS[1] - 100}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[1:4],
            "top_1prc": [row[0] for row in HOUR_VALS[1:4]],
            "top_1k": [row[1] for row in HOUR_VALS[1:4]],
            "top_100": [row[2] for row in HOUR_VALS[1:4]],
            "top_10": [row[3] for row in HOUR_VALS[1:4]],
            "circ_supply": HOUR_SCS[1:4],
        }

    def test_from_spot(self, client):
        # from timestamp at h2
        # expect value at h2, h3, h4 and h5
        url = base_url(client.address_type, self.r)
        url += f"&fr={HOUR_TSS[1]}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[1:5],
            "top_1prc": [row[0] for row in HOUR_VALS[1:5]],
            "top_1k": [row[1] for row in HOUR_VALS[1:5]],
            "top_100": [row[2] for row in HOUR_VALS[1:5]],
            "top_10": [row[3] for row in HOUR_VALS[1:5]],
            "circ_supply": HOUR_SCS[1:5],
        }

    def test_from_just_after_spot(self, client):
        # from timestamp just after h4
        # expect value at h5 and h6
        url = base_url(client.address_type, self.r)
        url += f"&fr={HOUR_TSS[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[4:6],
            "top_1prc": [row[0] for row in HOUR_VALS[4:6]],
            "top_1k": [row[1] for row in HOUR_VALS[4:6]],
            "top_100": [row[2] for row in HOUR_VALS[4:6]],
            "top_10": [row[3] for row in HOUR_VALS[4:6]],
            "circ_supply": HOUR_SCS[4:6],
        }

    def test_out_of_range(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={HOUR_TSS[-1] + 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [],
            "top_1prc": [],
            "top_1k": [],
            "top_100": [],
            "top_10": [],
            "circ_supply": [],
        }

    def test_to_only(self, client):
        # to timestamp of block h4
        # expect values at h2, h3 and h4
        url = base_url(client.address_type, self.r)
        url += f"&to={HOUR_TSS[3] + 100000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[1:4],
            "top_1prc": [row[0] for row in HOUR_VALS[1:4]],
            "top_1k": [row[1] for row in HOUR_VALS[1:4]],
            "top_100": [row[2] for row in HOUR_VALS[1:4]],
            "top_10": [row[3] for row in HOUR_VALS[1:4]],
            "circ_supply": HOUR_SCS[1:4],
        }

    def test_from_prior_to_genesis(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_from_ge_to(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={HOUR_TSS[3]}&to={HOUR_TSS[3] - 1}"
        response = client.get(url)
        assert response.status_code == 422
        assert response.json()["detail"] == "Parameter `fr` cannot be higher than `to`"

    def test_to_gt_sync_height(self, client):
        # to 2 hours after last one
        # expect value at h5 and h6
        url = base_url(client.address_type, self.r)
        url += f"&to={HOUR_TSS[-1] + HOUR_DT * 2}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[4:6],
            "top_1prc": [row[0] for row in HOUR_VALS[4:6]],
            "top_1k": [row[1] for row in HOUR_VALS[4:6]],
            "top_100": [row[2] for row in HOUR_VALS[4:6]],
            "top_10": [row[3] for row in HOUR_VALS[4:6]],
            "circ_supply": HOUR_SCS[4:6],
        }


class TestCountDaily:
    r = "24h"

    def test_default(self, client):
        url = base_url(client.address_type, self.r)
        response = client.get(url)
        assert response.status_code == 200
        # Return latest record
        assert response.json() == {
            "timestamps": [LAST_TS],
            "top_1prc": [LAST_VAL[0]],
            "top_1k": [LAST_VAL[1]],
            "top_100": [LAST_VAL[2]],
            "top_10": [LAST_VAL[3]],
            "circ_supply": [LAST_SC],
        }

    def test_from_to(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={DAY_TSS[1] - 1}&to={DAY_TSS[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[1:4],
            "top_1prc": [row[0] for row in DAY_VALS[1:4]],
            "top_1k": [row[1] for row in DAY_VALS[1:4]],
            "top_100": [row[2] for row in DAY_VALS[1:4]],
            "top_10": [row[3] for row in DAY_VALS[1:4]],
            "circ_supply": DAY_SCS[1:4],
        }

    def test_from_to_window_limit(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={DAY_TSS[1]}&to={DAY_TSS[5]}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {LIMIT * DAY_DT} for 24h resolution"
        )

    def test_from_just_before_new(self, client):
        # from timestamp between d1 and d2, just before d2
        # expect value at d2, d3 and d4
        url = base_url(client.address_type, self.r)
        url += f"&fr={1562112000000 - 100}"
        # from timestamp between d1 and d2, just before d2
        # expect value at d2, d3 and d4
        url = base_url(client.address_type, self.r)
        url += f"&fr={DAY_TSS[1] - 100}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[1:4],
            "top_1prc": [row[0] for row in DAY_VALS[1:4]],
            "top_1k": [row[1] for row in DAY_VALS[1:4]],
            "top_100": [row[2] for row in DAY_VALS[1:4]],
            "top_10": [row[3] for row in DAY_VALS[1:4]],
            "circ_supply": DAY_SCS[1:4],
        }

    def test_from_spot(self, client):
        # from timestamp at d2
        # expect value at d2, d3, d3 and d5
        url = base_url(client.address_type, self.r)
        url += f"&fr={DAY_TSS[1]}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[1:5],
            "top_1prc": [row[0] for row in DAY_VALS[1:5]],
            "top_1k": [row[1] for row in DAY_VALS[1:5]],
            "top_100": [row[2] for row in DAY_VALS[1:5]],
            "top_10": [row[3] for row in DAY_VALS[1:5]],
            "circ_supply": DAY_SCS[1:5],
        }

    def test_from_just_after_spot(self, client):
        # from timestamp just after d4
        # expect value at d5 and d6
        url = base_url(client.address_type, self.r)
        url += f"&fr={DAY_TSS[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[4:6],
            "top_1prc": [row[0] for row in DAY_VALS[4:6]],
            "top_1k": [row[1] for row in DAY_VALS[4:6]],
            "top_100": [row[2] for row in DAY_VALS[4:6]],
            "top_10": [row[3] for row in DAY_VALS[4:6]],
            "circ_supply": DAY_SCS[4:6],
        }

    def test_out_of_range(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={DAY_TSS[-1] + 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [],
            "top_1prc": [],
            "top_1k": [],
            "top_100": [],
            "top_10": [],
            "circ_supply": [],
        }

    def test_to_only(self, client):
        # to timestamp of block 26
        # expect values at d2, d3 and d4
        url = base_url(client.address_type, self.r)
        url += f"&to={DAY_TSS[3] + 100000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[1:4],
            "top_1prc": [row[0] for row in DAY_VALS[1:4]],
            "top_1k": [row[1] for row in DAY_VALS[1:4]],
            "top_100": [row[2] for row in DAY_VALS[1:4]],
            "top_10": [row[3] for row in DAY_VALS[1:4]],
            "circ_supply": DAY_SCS[1:4],
        }

    def test_from_prior_to_genesis(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_from_ge_to(self, client):
        url = base_url(client.address_type, self.r)
        url += f"&fr={DAY_TSS[3]}&to={DAY_TSS[3] - 1}"
        response = client.get(url)
        assert response.status_code == 422
        assert response.json()["detail"] == "Parameter `fr` cannot be higher than `to`"

    def test_to_gt_sync_height(self, client):
        # to 2 days after last one
        # expect value at d5 and d6
        url = base_url(client.address_type, self.r)
        url += f"&to={DAY_TSS[-1] + DAY_DT * 2}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[4:6],
            "top_1prc": [row[0] for row in DAY_VALS[4:6]],
            "top_1k": [row[1] for row in DAY_VALS[4:6]],
            "top_100": [row[2] for row in DAY_VALS[4:6]],
            "top_10": [row[3] for row in DAY_VALS[4:6]],
            "circ_supply": DAY_SCS[4:6],
        }


def test_default_r_is_block(client):
    url = f"/metrics/supply/distribution/{client.address_type}"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == {
        "timestamps": [LAST_TS],
        "top_1prc": [LAST_VAL[0]],
        "top_1k": [LAST_VAL[1]],
        "top_100": [LAST_VAL[2]],
        "top_10": [LAST_VAL[3]],
        "circ_supply": [LAST_SC],
    }

import pytest

from fastapi.testclient import TestClient

from ...main import app
from ..db import MockDB


# Change time window limit for easier testing
from ...api.routes import metrics

LIMIT = 3
metrics.utxos.TimeWindowLimits = metrics.generate_time_window_limits(LIMIT)
GENESIS_TIMESTAMP = metrics.GENESIS_TIMESTAMP

LAST_TS = 1600000000000
BLOCK_DT = 120_000
HOUR_DT = 3_600_000
DAY_DT = 86_400_000
BLOCK_TSS = [LAST_TS - i * BLOCK_DT for i in (5, 4, 3, 2, 1, 0)]
HOUR_TSS = [LAST_TS - i * HOUR_DT for i in (5, 4, 3, 2, 1, 0)]
DAY_TSS = [LAST_TS - i * DAY_DT for i in (5, 4, 3, 2, 1, 0)]
LAST_H = 6000
BLOCK_HS = [LAST_H - i * 1 for i in (5, 4, 3, 2, 1, 0)]
HOUR_HS = [LAST_H - i * 100 for i in (5, 4, 3, 2, 1, 0)]
DAY_HS = [LAST_H - i * 1000 for i in (5, 4, 3, 2, 1, 0)]
LAST_VAL = 60001
BLOCK_VALS = [h * 10 + 1 for h in BLOCK_HS]
HOUR_VALS = [h * 10 + 1 for h in HOUR_HS]
DAY_VALS = [h * 10 + 1 for h in DAY_HS]
assert LAST_VAL == BLOCK_VALS[-1] == HOUR_VALS[-1] == DAY_VALS[-1]


@pytest.fixture(scope="module")
def client():
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
        insert into mtr.utxos (height, value) values 
        ({DAY_HS[0]}, {DAY_VALS[0]}),
        ({DAY_HS[1]}, {DAY_VALS[1]}),
        ({DAY_HS[2]}, {DAY_VALS[2]}),
        ({DAY_HS[3]}, {DAY_VALS[3]}),
        ({DAY_HS[4]}, {DAY_VALS[4]}),
        ({HOUR_HS[0]}, {HOUR_VALS[0]}),
        ({HOUR_HS[1]}, {HOUR_VALS[1]}),
        ({HOUR_HS[2]}, {HOUR_VALS[2]}),
        ({HOUR_HS[3]}, {HOUR_VALS[3]}),
        ({HOUR_HS[4]}, {HOUR_VALS[4]}),
        ({BLOCK_HS[0]}, {BLOCK_VALS[0]}),
        ({BLOCK_HS[1]}, {BLOCK_VALS[1]}),
        ({BLOCK_HS[2]}, {BLOCK_VALS[2]}),
        ({BLOCK_HS[3]}, {BLOCK_VALS[3]}),
        ({BLOCK_HS[4]}, {BLOCK_VALS[4]}),
        ({BLOCK_HS[5]}, {BLOCK_VALS[5]});

        insert into mtr.utxos_summary(
            label, current, diff_1d, diff_1w, diff_4w, diff_6m, diff_1y
        ) values
            ('label_1', 1, 2, 3, 4, 5, 6),
            ('label_2', 10, 20, 30, 40, 50, 60);
    """
    with MockDB(sql=sql) as _:
        with TestClient(app) as client:
            yield client


class TestCountBlock:
    base_url = "/metrics/utxos?r=block"

    def test_default(self, client):
        url = self.base_url
        response = client.get(url)
        assert response.status_code == 200
        # Return last block record
        assert response.json() == {
            "timestamps": [LAST_TS],
            "values": [LAST_VAL],
        }

    def test_from_to(self, client):
        url = self.base_url + f"&fr={BLOCK_TSS[1]}&to={BLOCK_TSS[3]}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": BLOCK_TSS[1:4],
            "values": BLOCK_VALS[1:4],
        }

    def test_from_only(self, client):
        # from height 1
        url = self.base_url + f"&fr={BLOCK_TSS[1]}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": BLOCK_TSS[1:5],
            "values": BLOCK_VALS[1:5],
        }

    def test_to_only(self, client):
        # to right after block 5999
        url = self.base_url + f"&to={BLOCK_TSS[4] + 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": BLOCK_TSS[2:5],
            "values": BLOCK_VALS[2:5],
        }

    def test_from_prior_to_genesis(self, client):
        url = self.base_url + f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_window_size(self, client):
        url = self.base_url + f"&fr={BLOCK_TSS[0]}&to={BLOCK_TSS[5]}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {120000 * LIMIT} for block resolution"
        )


class TestCountHourly:
    base_url = "/metrics/utxos?r=1h"

    def test_default(self, client):
        url = self.base_url
        response = client.get(url)
        assert response.status_code == 200
        # Return last hourly record
        assert response.json() == {
            "timestamps": [LAST_TS],
            "values": [LAST_VAL],
        }

    def test_from_to(self, client):
        url = self.base_url + f"&fr={HOUR_TSS[1] - 1}&to={HOUR_TSS[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[1:4],
            "values": HOUR_VALS[1:4],
        }

    def test_from_to_window_limit(self, client):
        url = self.base_url + f"&fr={HOUR_TSS[1]}&to={HOUR_TSS[5]}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {LIMIT * HOUR_DT} for 1h resolution"
        )

    def test_from_just_before_new(self, client):
        # from timestamp between h1 and h2, just before h2
        # expect value at h2, h3 and h4
        url = self.base_url + f"&fr={HOUR_TSS[1] - 100}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[1:4],
            "values": HOUR_VALS[1:4],
        }

    def test_from_spot(self, client):
        # from timestamp at h4
        # expect value at h4, h5 and h6
        url = self.base_url + f"&fr={HOUR_TSS[3]}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[3:6],
            "values": HOUR_VALS[3:6],
        }

    def test_from_just_after_spot(self, client):
        # from timestamp just after h4
        # expect value at h5 and h6
        url = self.base_url + f"&fr={HOUR_TSS[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[4:6],
            "values": HOUR_VALS[4:6],
        }

    def test_out_of_range(self, client):
        url = self.base_url + f"&fr={HOUR_TSS[-1] + 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [],
            "values": [],
        }

    def test_to_only(self, client):
        # to timestamp of block h4
        # expect values at h2, h3 and h4
        url = self.base_url + f"&to={HOUR_TSS[3] + 100000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[1:4],
            "values": HOUR_VALS[1:4],
        }

    def test_from_prior_to_genesis(self, client):
        url = self.base_url + f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_from_ge_to(self, client):
        url = self.base_url + f"&fr={HOUR_TSS[3]}&to={HOUR_TSS[3] - 1}"
        response = client.get(url)
        assert response.status_code == 422
        assert response.json()["detail"] == "Parameter `fr` cannot be higher than `to`"

    def test_to_gt_sync_height(self, client):
        # to 2 hours after last one
        # expect value at h5 and h6
        url = self.base_url + f"&to={HOUR_TSS[-1] + HOUR_DT * 2}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": HOUR_TSS[4:6],
            "values": HOUR_VALS[4:6],
        }


class TestCountDaily:
    base_url = "/metrics/utxos?r=24h"

    def test_default(self, client):
        url = self.base_url
        response = client.get(url)
        assert response.status_code == 200
        # Return last daily record
        assert response.json() == {
            "timestamps": [LAST_TS],
            "values": [LAST_VAL],
        }

    def test_from_to(self, client):
        url = self.base_url + f"&fr={DAY_TSS[1] - 1}&to={DAY_TSS[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[1:4],
            "values": DAY_VALS[1:4],
        }

    def test_from_to_window_limit(self, client):
        url = self.base_url + f"&fr={DAY_TSS[1]}&to={DAY_TSS[5]}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {LIMIT * DAY_DT} for 24h resolution"
        )

    def test_from_just_before_new(self, client):
        # from timestamp between d1 and d2, just before d2
        # expect value at d2, d3 and d4
        url = self.base_url + f"&fr={1562112000000 - 100}"
        # from timestamp between d1 and d2, just before d2
        # expect value at d2, d3 and d4
        url = self.base_url + f"&fr={DAY_TSS[1] - 100}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[1:4],
            "values": DAY_VALS[1:4],
        }

    def test_from_spot(self, client):
        # from timestamp at d4
        # expect value at d4, d5 and d6
        url = self.base_url + f"&fr={DAY_TSS[3]}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[3:6],
            "values": DAY_VALS[3:6],
        }

    def test_from_just_after_spot(self, client):
        # from timestamp just after d4
        # expect value at d5 and d6
        url = self.base_url + f"&fr={DAY_TSS[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[4:6],
            "values": DAY_VALS[4:6],
        }

    def test_out_of_range(self, client):
        url = self.base_url + f"&fr={DAY_TSS[-1] + 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [],
            "values": [],
        }

    def test_to_only(self, client):
        # to timestamp of block 26
        # expect values at d2, d3 and d4
        url = self.base_url + f"&to={DAY_TSS[3] + 100000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[1:4],
            "values": DAY_VALS[1:4],
        }

    def test_from_prior_to_genesis(self, client):
        url = self.base_url + f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_from_ge_to(self, client):
        url = self.base_url + f"&fr={DAY_TSS[3]}&to={DAY_TSS[3] - 1}"
        response = client.get(url)
        assert response.status_code == 422
        assert response.json()["detail"] == "Parameter `fr` cannot be higher than `to`"

    def test_to_gt_sync_height(self, client):
        # to 2 days after last one
        # expect value at d5 and d6
        url = self.base_url + f"&to={DAY_TSS[-1] + DAY_DT * 2}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": DAY_TSS[4:6],
            "values": DAY_VALS[4:6],
        }


def test_default_r_is_block(client):
    url = "/metrics/utxos"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == {
        "timestamps": [LAST_TS],
        "values": [LAST_VAL],
    }


def test_summary(client):
    url = f"/metrics/summary/utxos"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == [
        {
            "label": "label_1",
            "current": 1,
            "diff_1d": 2,
            "diff_1w": 3,
            "diff_4w": 4,
            "diff_6m": 5,
            "diff_1y": 6,
        },
        {
            "label": "label_2",
            "current": 10,
            "diff_1d": 20,
            "diff_1w": 30,
            "diff_4w": 40,
            "diff_6m": 50,
            "diff_1y": 60,
        },
    ]

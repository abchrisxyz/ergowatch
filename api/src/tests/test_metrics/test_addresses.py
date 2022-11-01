import pytest
from typing import List

from fastapi.testclient import TestClient

from ...main import app
from ..db import MockDB

from ...api.routes import metrics

# Change time window limit for easier testing
LIMIT = 3
limits = metrics.generate_time_window_limits(LIMIT)
metrics.addresses.TimeWindowLimits = limits
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

LAST_VAL = [
    600001,
    600002,
    600003,
    600004,
    600005,
    600006,
    600007,
    600008,
    600009,
    600010,
    600011,
]
NCOLS = len(LAST_VAL)
BLOCK_VALS = [[h * 100 + i for i in range(1, NCOLS + 1)] for h in BLOCK_HS]
HOUR_VALS = [[h * 100 + i for i in range(1, NCOLS + 1)] for h in HOUR_HS]
DAY_VALS = [[h * 100 + i for i in range(1, NCOLS + 1)] for h in DAY_HS]
assert LAST_VAL == BLOCK_VALS[-1] == HOUR_VALS[-1] == DAY_VALS[-1]


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
        insert into mtr.address_counts_by_balance_{address_type} (
            height,
            total,
            ge_0p001,
            ge_0p01,
            ge_0p1,
            ge_1,
            ge_10,
            ge_100,
            ge_1k,
            ge_10k,
            ge_100k,
            ge_1m
        ) values 
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

        insert into mtr.address_counts_by_balance_{address_type}_summary(
            label, current, diff_1d, diff_1w, diff_4w, diff_6m, diff_1y
        ) values
            ('label_1', 1, 2, 3, 4, 5, 6),
            ('label_2', 10, 20, 30, 40, 50, 60);
    """
    with MockDB(sql=sql) as _:
        with TestClient(app) as client:
            client.address_type = address_type
            yield client


def base_url(address_type: str, r: str):
    return f"/metrics/addresses/{address_type}?r={r}"


@pytest.mark.parametrize(
    "r,dt,tss,vals",
    [
        ("block", BLOCK_DT, BLOCK_TSS, BLOCK_VALS),
        ("1h", HOUR_DT, HOUR_TSS, HOUR_VALS),
        ("24h", DAY_DT, DAY_TSS, DAY_VALS),
    ],
)
class TestSeriesApi:
    def test_default(self, client, r, dt, tss, vals):
        url = base_url(client.address_type, r)
        response = client.get(url)
        assert response.status_code == 200
        # Return latest record
        assert response.json() == {
            "timestamps": [LAST_TS],
            "gt_0": [LAST_VAL[0]],
            "ge_0p001": [LAST_VAL[1]],
            "ge_0p01": [LAST_VAL[2]],
            "ge_0p1": [LAST_VAL[3]],
            "ge_1": [LAST_VAL[4]],
            "ge_10": [LAST_VAL[5]],
            "ge_100": [LAST_VAL[6]],
            "ge_1k": [LAST_VAL[7]],
            "ge_10k": [LAST_VAL[8]],
            "ge_100k": [LAST_VAL[9]],
            "ge_1m": [LAST_VAL[10]],
        }

    def test_from_to(self, client, r, dt, tss, vals):
        url = base_url(client.address_type, r)
        url += f"&fr={tss[1] - 1}&to={tss[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[1:4],
            "gt_0": [row[0] for row in vals[1:4]],
            "ge_0p001": [row[1] for row in vals[1:4]],
            "ge_0p01": [row[2] for row in vals[1:4]],
            "ge_0p1": [row[3] for row in vals[1:4]],
            "ge_1": [row[4] for row in vals[1:4]],
            "ge_10": [row[5] for row in vals[1:4]],
            "ge_100": [row[6] for row in vals[1:4]],
            "ge_1k": [row[7] for row in vals[1:4]],
            "ge_10k": [row[8] for row in vals[1:4]],
            "ge_100k": [row[9] for row in vals[1:4]],
            "ge_1m": [row[10] for row in vals[1:4]],
        }

    def test_from_to_window_limit(self, client, r, dt, tss, vals):
        url = base_url(client.address_type, r)
        url += f"&fr={tss[1]}&to={tss[5]}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {LIMIT * dt} for {r} resolution"
        )

    def test_from_just_before_new(self, client, r, dt, tss, vals):
        # from timestamp between h1 and h2, just before h2
        # expect value at h2, h3 and h4
        url = base_url(client.address_type, r)
        url += f"&fr={tss[1] - 100}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[1:4],
            "gt_0": [row[0] for row in vals[1:4]],
            "ge_0p001": [row[1] for row in vals[1:4]],
            "ge_0p01": [row[2] for row in vals[1:4]],
            "ge_0p1": [row[3] for row in vals[1:4]],
            "ge_1": [row[4] for row in vals[1:4]],
            "ge_10": [row[5] for row in vals[1:4]],
            "ge_100": [row[6] for row in vals[1:4]],
            "ge_1k": [row[7] for row in vals[1:4]],
            "ge_10k": [row[8] for row in vals[1:4]],
            "ge_100k": [row[9] for row in vals[1:4]],
            "ge_1m": [row[10] for row in vals[1:4]],
        }

    def test_from_spot(self, client, r, dt, tss, vals):
        # from timestamp at h2
        # expect value at h2, h3, h4 and h5
        url = base_url(client.address_type, r)
        url += f"&fr={tss[1]}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[1:5],
            "gt_0": [row[0] for row in vals[1:5]],
            "ge_0p001": [row[1] for row in vals[1:5]],
            "ge_0p01": [row[2] for row in vals[1:5]],
            "ge_0p1": [row[3] for row in vals[1:5]],
            "ge_1": [row[4] for row in vals[1:5]],
            "ge_10": [row[5] for row in vals[1:5]],
            "ge_100": [row[6] for row in vals[1:5]],
            "ge_1k": [row[7] for row in vals[1:5]],
            "ge_10k": [row[8] for row in vals[1:5]],
            "ge_100k": [row[9] for row in vals[1:5]],
            "ge_1m": [row[10] for row in vals[1:5]],
        }

    def test_from_just_after_spot(self, client, r, dt, tss, vals):
        # from timestamp just after h4
        # expect value at h5 and h6
        url = base_url(client.address_type, r)
        url += f"&fr={tss[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[4:6],
            "gt_0": [row[0] for row in vals[4:6]],
            "ge_0p001": [row[1] for row in vals[4:6]],
            "ge_0p01": [row[2] for row in vals[4:6]],
            "ge_0p1": [row[3] for row in vals[4:6]],
            "ge_1": [row[4] for row in vals[4:6]],
            "ge_10": [row[5] for row in vals[4:6]],
            "ge_100": [row[6] for row in vals[4:6]],
            "ge_1k": [row[7] for row in vals[4:6]],
            "ge_10k": [row[8] for row in vals[4:6]],
            "ge_100k": [row[9] for row in vals[4:6]],
            "ge_1m": [row[10] for row in vals[4:6]],
        }

    def test_out_of_range(self, client, r, dt, tss, vals):
        url = base_url(client.address_type, r)
        url += f"&fr={tss[-1] + 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [],
            "gt_0": [],
            "ge_0p001": [],
            "ge_0p01": [],
            "ge_0p1": [],
            "ge_1": [],
            "ge_10": [],
            "ge_100": [],
            "ge_1k": [],
            "ge_10k": [],
            "ge_100k": [],
            "ge_1m": [],
        }

    def test_to_only(self, client, r, dt, tss, vals):
        # to timestamp of block h4
        # expect values at h2, h3 and h4
        url = base_url(client.address_type, r)
        url += f"&to={tss[3] + 100000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[1:4],
            "gt_0": [row[0] for row in vals[1:4]],
            "ge_0p001": [row[1] for row in vals[1:4]],
            "ge_0p01": [row[2] for row in vals[1:4]],
            "ge_0p1": [row[3] for row in vals[1:4]],
            "ge_1": [row[4] for row in vals[1:4]],
            "ge_10": [row[5] for row in vals[1:4]],
            "ge_100": [row[6] for row in vals[1:4]],
            "ge_1k": [row[7] for row in vals[1:4]],
            "ge_10k": [row[8] for row in vals[1:4]],
            "ge_100k": [row[9] for row in vals[1:4]],
            "ge_1m": [row[10] for row in vals[1:4]],
        }

    def test_from_prior_to_genesis(self, client, r, dt, tss, vals):
        url = base_url(client.address_type, r)
        url += f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_from_ge_to(self, client, r, dt, tss, vals):
        url = base_url(client.address_type, r)
        url += f"&fr={tss[3]}&to={tss[3] - 1}"
        response = client.get(url)
        assert response.status_code == 422
        assert response.json()["detail"] == "Parameter `fr` cannot be higher than `to`"

    def test_to_gt_sync_height(self, client, r, dt, tss, vals):
        # to 2 hours after last one
        # expect value at h5 and h6
        url = base_url(client.address_type, r)
        url += f"&to={tss[-1] + dt * 2}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[4:6],
            "gt_0": [row[0] for row in vals[4:6]],
            "ge_0p001": [row[1] for row in vals[4:6]],
            "ge_0p01": [row[2] for row in vals[4:6]],
            "ge_0p1": [row[3] for row in vals[4:6]],
            "ge_1": [row[4] for row in vals[4:6]],
            "ge_10": [row[5] for row in vals[4:6]],
            "ge_100": [row[6] for row in vals[4:6]],
            "ge_1k": [row[7] for row in vals[4:6]],
            "ge_10k": [row[8] for row in vals[4:6]],
            "ge_100k": [row[9] for row in vals[4:6]],
            "ge_1m": [row[10] for row in vals[4:6]],
        }


def test_default_r_is_block(client):
    url = f"/metrics/addresses/{client.address_type}"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == {
        "timestamps": [LAST_TS],
        "gt_0": [LAST_VAL[0]],
        "ge_0p001": [LAST_VAL[1]],
        "ge_0p01": [LAST_VAL[2]],
        "ge_0p1": [LAST_VAL[3]],
        "ge_1": [LAST_VAL[4]],
        "ge_10": [LAST_VAL[5]],
        "ge_100": [LAST_VAL[6]],
        "ge_1k": [LAST_VAL[7]],
        "ge_10k": [LAST_VAL[8]],
        "ge_100k": [LAST_VAL[9]],
        "ge_1m": [LAST_VAL[10]],
    }


def test_summary(client):
    url = f"/metrics/summary/addresses/{client.address_type}"
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

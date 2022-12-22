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

        -- ergusd
        insert into mtr.ergusd (height, value) values
        ({DAY_HS[0]}, {0 + 0.1}),
        ({DAY_HS[1]}, {1 + 0.1}),
        ({DAY_HS[2]}, {2 + 0.1}),
        ({DAY_HS[3]}, {3 + 0.1}),
        ({DAY_HS[4]}, {4 + 0.1}),
        ({HOUR_HS[0]}, {0 + 0.1}),
        ({HOUR_HS[1]}, {1 + 0.1}),
        ({HOUR_HS[2]}, {2 + 0.1}),
        ({HOUR_HS[3]}, {3 + 0.1}),
        ({HOUR_HS[4]}, {4 + 0.1}),
        ({BLOCK_HS[0]}, {0 + 0.1}),
        ({BLOCK_HS[1]}, {1 + 0.1}),
        ({BLOCK_HS[2]}, {2 + 0.1}),
        ({BLOCK_HS[3]}, {3 + 0.1}),
        ({BLOCK_HS[4]}, {4 + 0.1}),
        ({BLOCK_HS[5]}, {5 + 0.1});

        insert into mtr.supply_on_top_addresses_{address_type}_summary(
            label, current, diff_1d, diff_1w, diff_4w, diff_6m, diff_1y
        ) values
            ('label_1', 1, 2, 3, 4, 5, 6),
            ('label_2', 60, 10, 30, 10, 10, 10);
        insert into mtr.supply_composition_summary(
            label, current, diff_1d, diff_1w, diff_4w, diff_6m, diff_1y
        ) values
            ('dummy', 0, 0, 0, 0, 0, 0),
            ('total', 240, 40, 0, 40, 40, 40);
    """
    with MockDB(sql=sql) as _:
        with TestClient(app) as client:
            client.address_type = address_type
            yield client


def base_url(address_type: str, r: str):
    return f"/metrics/supply/distribution/{address_type}?r={r}"


@pytest.mark.parametrize(
    "r,dt,tss,vals,scs",
    [
        ("block", BLOCK_DT, BLOCK_TSS, BLOCK_VALS, BLOCK_SCS),
        ("1h", HOUR_DT, HOUR_TSS, HOUR_VALS, HOUR_SCS),
        ("24h", DAY_DT, DAY_TSS, DAY_VALS, DAY_SCS),
    ],
)
class TestSeriesApi:
    def test_default(self, client, r, dt, tss, vals, scs):
        url = base_url(client.address_type, r)
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

    def test_from_to(self, client, r, dt, tss, vals, scs):
        url = base_url(client.address_type, r)
        url += f"&fr={tss[1] - 1}&to={tss[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[1:4],
            "top_1prc": [row[0] for row in vals[1:4]],
            "top_1k": [row[1] for row in vals[1:4]],
            "top_100": [row[2] for row in vals[1:4]],
            "top_10": [row[3] for row in vals[1:4]],
            "circ_supply": scs[1:4],
        }

    def test_from_to_window_limit(self, client, r, dt, tss, vals, scs):
        url = base_url(client.address_type, r)
        url += f"&fr={tss[1]}&to={tss[5]}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {LIMIT * dt} for {r} resolution"
        )

    def test_from_just_before_new(self, client, r, dt, tss, vals, scs):
        # from timestamp between h1 and h2, just before h2
        # expect value at h2, h3 and h4
        url = base_url(client.address_type, r)
        url += f"&fr={tss[1] - 100}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[1:4],
            "top_1prc": [row[0] for row in vals[1:4]],
            "top_1k": [row[1] for row in vals[1:4]],
            "top_100": [row[2] for row in vals[1:4]],
            "top_10": [row[3] for row in vals[1:4]],
            "circ_supply": scs[1:4],
        }

    def test_from_spot(self, client, r, dt, tss, vals, scs):
        # from timestamp at h2
        # expect value at h2, h3, h4 and h5
        url = base_url(client.address_type, r)
        url += f"&fr={tss[1]}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[1:5],
            "top_1prc": [row[0] for row in vals[1:5]],
            "top_1k": [row[1] for row in vals[1:5]],
            "top_100": [row[2] for row in vals[1:5]],
            "top_10": [row[3] for row in vals[1:5]],
            "circ_supply": scs[1:5],
        }

    def test_from_just_after_spot(self, client, r, dt, tss, vals, scs):
        # from timestamp just after h4
        # expect value at h5 and h6
        url = base_url(client.address_type, r)
        url += f"&fr={tss[3] + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[4:6],
            "top_1prc": [row[0] for row in vals[4:6]],
            "top_1k": [row[1] for row in vals[4:6]],
            "top_100": [row[2] for row in vals[4:6]],
            "top_10": [row[3] for row in vals[4:6]],
            "circ_supply": scs[4:6],
        }

    def test_out_of_range(self, client, r, dt, tss, vals, scs):
        url = base_url(client.address_type, r)
        url += f"&fr={tss[-1] + 10}"
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

    def test_to_only(self, client, r, dt, tss, vals, scs):
        # to timestamp of block h4
        # expect values at h2, h3 and h4
        url = base_url(client.address_type, r)
        url += f"&to={tss[3] + 100000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[1:4],
            "top_1prc": [row[0] for row in vals[1:4]],
            "top_1k": [row[1] for row in vals[1:4]],
            "top_100": [row[2] for row in vals[1:4]],
            "top_10": [row[3] for row in vals[1:4]],
            "circ_supply": scs[1:4],
        }

    def test_from_prior_to_genesis(self, client, r, dt, tss, vals, scs):
        url = base_url(client.address_type, r)
        url += f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_from_ge_to(self, client, r, dt, tss, vals, scs):
        url = base_url(client.address_type, r)
        url += f"&fr={tss[3]}&to={tss[3] - 1}"
        response = client.get(url)
        assert response.status_code == 422
        assert response.json()["detail"] == "Parameter `fr` cannot be higher than `to`"

    def test_to_gt_sync_height(self, client, r, dt, tss, vals, scs):
        # to 2 hours after last one
        # expect value at h5 and h6
        url = base_url(client.address_type, r)
        url += f"&to={tss[-1] + dt * 2}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[4:6],
            "top_1prc": [row[0] for row in vals[4:6]],
            "top_1k": [row[1] for row in vals[4:6]],
            "top_100": [row[2] for row in vals[4:6]],
            "top_10": [row[3] for row in vals[4:6]],
            "circ_supply": scs[4:6],
        }

    def test_default_with_ergusd(self, client, r, dt, tss, vals, scs):
        url = base_url(client.address_type, r)
        url += "&ergusd=1"
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
            "ergusd": [len(BLOCK_HS) - 1 + 0.1],
        }

    def test_from_to_with_ergusd(self, client, r, dt, tss, vals, scs):
        url = base_url(client.address_type, r)
        url += f"&fr={tss[1] - 1}&to={tss[3] + 1}&ergusd=1"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": tss[1:4],
            "top_1prc": [row[0] for row in vals[1:4]],
            "top_1k": [row[1] for row in vals[1:4]],
            "top_100": [row[2] for row in vals[1:4]],
            "top_10": [row[3] for row in vals[1:4]],
            "circ_supply": scs[1:4],
            "ergusd": [i + 0.1 for i in range(1, 4)],
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


def test_summary(client):
    url = f"/metrics/summary/supply/distribution/{client.address_type}"
    response = client.get(url)
    assert response.status_code == 200
    res = response.json()
    assert len(res) == 2
    absolute = res["absolute"]
    relative = res["relative"]
    assert absolute == [
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
            "current": 60,
            "diff_1d": 10,
            "diff_1w": 30,
            "diff_4w": 10,
            "diff_6m": 10,
            "diff_1y": 10,
        },
    ]
    assert relative[1]["label"] == "label_2"
    assert relative[1]["current"] == 0.25
    assert relative[1]["diff_1d"] == 0.0
    assert relative[1]["diff_1w"] == 0.125

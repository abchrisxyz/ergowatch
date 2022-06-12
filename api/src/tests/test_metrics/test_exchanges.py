import pytest

from fastapi.testclient import TestClient

from ...main import app
from ..db import MockDB


# Change time window limit for easier testing
from ...api.routes import metrics

LIMIT = 3
metrics.exchanges.TimeWindowLimits = metrics.generate_time_window_limits(LIMIT)
GENESIS_TIMESTAMP = metrics.GENESIS_TIMESTAMP


@pytest.fixture(scope="module")
def client():
    sql = f"""
        insert into core.headers (height, id, parent_id, timestamp) values 
        -- block level data
        (0, 'header0', 'header_', 1561978800000), -- genesis
        (1, 'header1', 'header0', 1561978900000), -- + 100k (1 block)
        (2, 'header2', 'header1', 1561979000000), -- + 100k (1 block)
        (3, 'header3', 'header2', 1561979100000), -- + 100k (1 block)
        (4, 'header4', 'header3', 1561979200000), -- + 100k (1 block)
        (5, 'header5', 'header4', 1561979300000), -- + 100k (1 block)
        
        -- round hours +/- a delta
        -- need some blocks on round hours, others in between.
        -- Should span several hours and more than hourly time window limit
        ( 6, 'header06', 'header05', 1561982400000         ), -- h1 spot
        ( 7, 'header07', 'header06', 1561986000000 - 100000), -- h1
        ( 8, 'header08', 'header07', 1561986000000 + 100000), -- h2
        ( 9, 'header09', 'header08', 1561986000000 + 200000), -- h2
        (10, 'header10', 'header09', 1561989600000 - 100000), -- h2
        (11, 'header11', 'header10', 1561989600000 + 100000), -- h3
        (12, 'header12', 'header11', 1561993200000 - 100000), -- h3
        (13, 'header13', 'header12', 1561993200000         ), -- h4 spot
        (14, 'header14', 'header13', 1561993200000 + 100000), -- h4
        (15, 'header15', 'header14', 1561996800000 + 100000), -- h5
        (16, 'header16', 'header15', 1561996800000 + 200000), -- h5
        (17, 'header17', 'header16', 1562000400000 + 100000), -- h6

        -- round days +/- a delta
        -- need some blocks on round days, others in between.
        -- Should span several days and more than daily time window limit
        (18, 'header18', 'header17', 1562025600000),          -- d1 spot
        (19, 'header19', 'header18', 1562112000000 - 100000), -- d1
        (20, 'header20', 'header19', 1562112000000 + 100000), -- d2
        (21, 'header21', 'header20', 1562112000000 + 200000), -- d2
        (22, 'header22', 'header21', 1562198400000 - 100000), -- d2 
        (23, 'header23', 'header22', 1562198400000 + 100000), -- d3 
        (24, 'header24', 'header23', 1562284800000 - 100000), -- d3
        (25, 'header25', 'header24', 1562284800000         ), -- d4 spot
        (26, 'header26', 'header25', 1562284800000 + 100000), -- d4
        (27, 'header27', 'header26', 1562371200000 + 100000), -- d5 
        (28, 'header28', 'header27', 1562371200000 + 200000), -- d5 
        (29, 'header29', 'header28', 1562457600000 + 100000); -- d6 
      
        insert into mtr.cex_supply (height, total, deposit) values 
        (0, 3, 2),
        (1, 4, 3),
        (2, 5, 4),
        (3, 6, 5),
        (4, 7, 6),
        (5, 9, 8),

        -- hourly data
        ( 6,  60,  6), -- h1 spot
        ( 7,  70,  7), -- h1 
        ( 8,  80,  8), -- h2
        ( 9,  90,  9), -- h2
        (10, 100, 10), -- h2
        (11, 110, 11), -- h3
        (12, 120, 12), -- h3
        (13, 130, 13), -- h4 spot
        (14, 140, 14), -- h4
        (15, 150, 15), -- h5
        (16, 160, 16), -- h5
        (17, 170, 17), -- h6

        -- daily data
        (18, 180, 18), -- d1 spot
        (19, 190, 19), -- d1 
        (20, 200, 20), -- d2
        (21, 210, 21), -- d2
        (22, 220, 22), -- d2
        (23, 230, 23), -- d3
        (24, 240, 24), -- d3
        (25, 250, 25), -- d4 spot
        (26, 260, 26), -- d4
        (27, 270, 27), -- d5
        (28, 280, 28), -- d5
        (29, 290, 29); -- d6
    """
    with MockDB(sql=sql) as _:
        with TestClient(app) as client:
            yield client


class TestCountBlock:
    base_url = "/metrics/exchanges/supply?r=block"

    def test_default(self, client):
        url = self.base_url
        response = client.get(url)
        assert response.status_code == 200
        # Return last block record
        assert response.json() == {
            "timestamps": [1562457600000 + 100000],
            "total": [290],
            "deposit": [29],
        }

    def test_from_to_block(self, client):
        url = self.base_url + f"&fr={1561979000000}&to={1561979200000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561979000000, 1561979100000, 1561979200000],
            "total": [5, 6, 7],
            "deposit": [4, 5, 6],
        }

    def test_from_only(self, client):
        # from height 1
        url = self.base_url + f"&fr={1561978900000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561978900000, 1561979000000, 1561979100000, 1561979200000],
            "total": [4, 5, 6, 7],
            "deposit": [3, 4, 5, 6],
        }

    def test_to_only(self, client):
        # to right after block 4
        # expect blocks 1, 2, 3 and 4
        url = self.base_url + f"&to={1561979200000 + 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561978900000, 1561979000000, 1561979100000, 1561979200000],
            "total": [4, 5, 6, 7],
            "deposit": [3, 4, 5, 6],
        }

    def test_from_prior_to_genesis(self, client):
        url = self.base_url + f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_window_size(self, client):
        url = self.base_url + f"&fr={1561979000000}&to={1562000400000}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {120000 * LIMIT} for block resolution"
        )


class TestCountHourly:
    base_url = "/metrics/exchanges/supply?r=1h"

    def test_default(self, client):
        url = self.base_url
        response = client.get(url)
        assert response.status_code == 200
        # Return last hourly record
        assert response.json() == {
            "timestamps": [1562457600000],
            "total": [280],
            "deposit": [28],
        }

    def test_from_to(self, client):
        url = (
            self.base_url + f"&fr={1561986000000 - 100000}&to={1561993200000 + 100000}"
        )
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561986000000, 1561989600000, 1561993200000],
            "total": [70, 100, 130],
            "deposit": [7, 10, 13],
        }

    def test_from_to_window_limit(self, client):
        url = self.base_url + f"&fr={1561982400000}&to={1562000400000 + 200000}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {LIMIT * 3_600_000} for 1h resolution"
        )

    def test_from_just_before_new(self, client):
        # from timestamp between h1 and h2, just before h2
        # expect value at h2, h3 and h4
        url = self.base_url + f"&fr={1561986000000 - 100}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561986000000, 1561989600000, 1561993200000],
            "total": [70, 100, 130],
            "deposit": [7, 10, 13],
        }

    def test_from_spot(self, client):
        # from timestamp at h4
        # expect value at h4, h5 and h6
        url = self.base_url + f"&fr={1561993200000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561993200000, 1561996800000, 1562000400000],
            "total": [130, 140, 160],
            "deposit": [13, 14, 16],
        }

    def test_from_just_after_spot(self, client):
        # from timestamp just after h4
        # expect value at h5 and h6
        url = self.base_url + f"&fr={1561993200000 + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561996800000, 1562000400000],
            "total": [140, 160],
            "deposit": [14, 16],
        }

    def test_out_of_range(self, client):
        url = self.base_url + f"&fr={1561986000000 * 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [],
            "total": [],
            "deposit": [],
        }

    def test_to_only(self, client):
        # to timestamp of block 14
        # expect values at h2, h3 and h4
        url = self.base_url + f"&to={1561993200000 + 100000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561986000000, 1561989600000, 1561993200000],
            "total": [70, 100, 130],
            "deposit": [7, 10, 13],
        }

    def test_from_prior_to_genesis(self, client):
        url = self.base_url + f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_from_ge_to(self, client):
        url = self.base_url + f"&fr={1561986000000}&to={1561986000000 - 1000}"
        response = client.get(url)
        assert response.status_code == 422
        assert response.json()["detail"] == "Parameter `fr` cannot be higher than `to`"

    def test_to_gt_sync_height(self, client):
        # to 2 hours after last one
        # expect value at h5 and h6
        url = self.base_url + f"&to={1562000400000 + 3_600_000 * 2}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561996800000, 1562000400000],
            "total": [140, 160],
            "deposit": [14, 16],
        }


class TestCountDaily:
    base_url = "/metrics/exchanges/supply?r=24h"

    def test_default(self, client):
        url = self.base_url
        response = client.get(url)
        assert response.status_code == 200
        # Return last daily record
        assert response.json() == {
            "timestamps": [1562457600000],
            "total": [280],
            "deposit": [28],
        }

    def test_from_to(self, client):
        url = (
            self.base_url + f"&fr={1562112000000 - 100000}&to={1562284800000 + 100000}"
        )
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562112000000, 1562198400000, 1562284800000],
            "total": [190, 220, 250],
            "deposit": [19, 22, 25],
        }

    def test_from_to_window_limit(self, client):
        url = self.base_url + f"&fr={1561982400000}&to={1562457600000}"
        response = client.get(url)
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Time window is limited to {LIMIT * 86_400_000} for 24h resolution"
        )

    def test_from_just_before_new(self, client):
        # from timestamp between d1 and d2, just before d2
        # expect value at d2, d3 and d4
        url = self.base_url + f"&fr={1562112000000 - 100}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562112000000, 1562198400000, 1562284800000],
            "total": [190, 220, 250],
            "deposit": [19, 22, 25],
        }

    def test_from_spot(self, client):
        # from timestamp at d4
        # expect value at d4, d5 and d6
        url = self.base_url + f"&fr={1562284800000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562284800000, 1562371200000, 1562457600000],
            "total": [250, 260, 280],
            "deposit": [25, 26, 28],
        }

    def test_from_just_after_spot(self, client):
        # from timestamp just after d4
        # expect value at d5 and d6
        url = self.base_url + f"&fr={1562284800000 + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562371200000, 1562457600000],
            "total": [260, 280],
            "deposit": [26, 28],
        }

    def test_out_of_range(self, client):
        url = self.base_url + f"&fr={1561986000000 * 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [],
            "total": [],
            "deposit": [],
        }

    def test_to_only(self, client):
        # to timestamp of block 26
        # expect values at d2, d3 and d4
        url = self.base_url + f"&to={1562284800000 + 100000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562112000000, 1562198400000, 1562284800000],
            "total": [190, 220, 250],
            "deposit": [19, 22, 25],
        }

    def test_from_prior_to_genesis(self, client):
        url = self.base_url + f"&fr={GENESIS_TIMESTAMP - 1}"
        response = client.get(url)
        assert response.status_code == 422

    def test_from_ge_to(self, client):
        url = self.base_url + f"&fr={1561986000000}&to={1561986000000 - 1000}"
        response = client.get(url)
        assert response.status_code == 422
        assert response.json()["detail"] == "Parameter `fr` cannot be higher than `to`"

    def test_to_gt_sync_height(self, client):
        # to 2 days after last one
        # expect value at d5 and d6
        url = self.base_url + f"&to={1562457600000 + 86_400_000 * 2}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562371200000, 1562457600000],
            "total": [260, 280],
            "deposit": [26, 28],
        }


def test_default_r_is_block(client):
    url = "/metrics/exchanges/supply"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == {
        "timestamps": [1562457600000 + 100000],
        "total": [290],
        "deposit": [29],
    }

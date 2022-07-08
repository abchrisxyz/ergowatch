import pytest

from fastapi.testclient import TestClient

from ...main import app
from ..db import MockDB


# Change time window limit for easier testing
from ...api.routes import metrics

LIMIT = 3
metrics.utxos.TimeWindowLimits = metrics.generate_time_window_limits(LIMIT)
GENESIS_TIMESTAMP = metrics.GENESIS_TIMESTAMP


@pytest.fixture(scope="module")
def client():
    # Difficulty and votes
    dvs = "111222333, 0, 0, 0"
    sql = f"""
        insert into core.headers (difficulty, vote1, vote2, vote3, height, id, parent_id, timestamp) values 
        -- block level data
        ({dvs}, 0, 'header0', 'header_', 1561978800000), -- genesis
        ({dvs}, 1, 'header1', 'header0', 1561978900000), -- + 100k (1 block)
        ({dvs}, 2, 'header2', 'header1', 1561979000000), -- + 100k (1 block)
        ({dvs}, 3, 'header3', 'header2', 1561979100000), -- + 100k (1 block)
        ({dvs}, 4, 'header4', 'header3', 1561979200000), -- + 100k (1 block)
        ({dvs}, 5, 'header5', 'header4', 1561979300000), -- + 100k (1 block)
        
        -- round hours +/- a delta
        -- need some blocks on round hours, others in between.
        -- Should span several hours and more than hourly time window limit
        ({dvs},  6, 'header06', 'header05', 1561982400000         ), -- h1 spot
        ({dvs},  7, 'header07', 'header06', 1561986000000 - 100000), -- h1
        ({dvs},  8, 'header08', 'header07', 1561986000000 + 100000), -- h2
        ({dvs},  9, 'header09', 'header08', 1561986000000 + 200000), -- h2
        ({dvs}, 10, 'header10', 'header09', 1561989600000 - 100000), -- h2
        ({dvs}, 11, 'header11', 'header10', 1561989600000 + 100000), -- h3
        ({dvs}, 12, 'header12', 'header11', 1561993200000 - 100000), -- h3
        ({dvs}, 13, 'header13', 'header12', 1561993200000         ), -- h4 spot
        ({dvs}, 14, 'header14', 'header13', 1561993200000 + 100000), -- h4
        ({dvs}, 15, 'header15', 'header14', 1561996800000 + 100000), -- h5
        ({dvs}, 16, 'header16', 'header15', 1561996800000 + 200000), -- h5
        ({dvs}, 17, 'header17', 'header16', 1562000400000 + 100000), -- h6

        -- round days +/- a delta
        -- need some blocks on round days, others in between.
        -- Should span several days and more than daily time window limit
        ({dvs}, 18, 'header18', 'header17', 1562025600000),          -- d1 spot
        ({dvs}, 19, 'header19', 'header18', 1562112000000 - 100000), -- d1
        ({dvs}, 20, 'header20', 'header19', 1562112000000 + 100000), -- d2
        ({dvs}, 21, 'header21', 'header20', 1562112000000 + 200000), -- d2
        ({dvs}, 22, 'header22', 'header21', 1562198400000 - 100000), -- d2 
        ({dvs}, 23, 'header23', 'header22', 1562198400000 + 100000), -- d3 
        ({dvs}, 24, 'header24', 'header23', 1562284800000 - 100000), -- d3
        ({dvs}, 25, 'header25', 'header24', 1562284800000         ), -- d4 spot
        ({dvs}, 26, 'header26', 'header25', 1562284800000 + 100000), -- d4
        ({dvs}, 27, 'header27', 'header26', 1562371200000 + 100000), -- d5 
        ({dvs}, 28, 'header28', 'header27', 1562371200000 + 200000), -- d5 
        ({dvs}, 29, 'header29', 'header28', 1562457600000 + 100000); -- d6 
      
        insert into mtr.utxos (height, value) values 
        (0, 3),
        (1, 4),
        (2, 5),
        (3, 6),
        (4, 7),
        (5, 9),

        -- hourly data
        ( 6,  60), -- h1 spot
        ( 7,  70), -- h1 
        ( 8,  80), -- h2
        ( 9,  90), -- h2
        (10, 100), -- h2
        (11, 110), -- h3
        (12, 120), -- h3
        (13, 130), -- h4 spot
        (14, 140), -- h4
        (15, 150), -- h5
        (16, 160), -- h5
        (17, 170), -- h6

        -- daily data
        (18, 180), -- d1 spot
        (19, 190), -- d1 
        (20, 200), -- d2
        (21, 210), -- d2
        (22, 220), -- d2
        (23, 230), -- d3
        (24, 240), -- d3
        (25, 250), -- d4 spot
        (26, 260), -- d4
        (27, 270), -- d5
        (28, 280), -- d5
        (29, 290); -- d6
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
            "timestamps": [1562457600000 + 100000],
            "values": [290],
        }

    def test_from_to_block(self, client):
        url = self.base_url + f"&fr={1561979000000}&to={1561979200000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561979000000, 1561979100000, 1561979200000],
            "values": [5, 6, 7],
        }

    def test_from_only(self, client):
        # from height 1
        url = self.base_url + f"&fr={1561978900000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561978900000, 1561979000000, 1561979100000, 1561979200000],
            "values": [4, 5, 6, 7],
        }

    def test_to_only(self, client):
        # to right after block 4
        # expect blocks 1, 2, 3 and 4
        url = self.base_url + f"&to={1561979200000 + 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561978900000, 1561979000000, 1561979100000, 1561979200000],
            "values": [4, 5, 6, 7],
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
    base_url = "/metrics/utxos?r=1h"

    def test_default(self, client):
        url = self.base_url
        response = client.get(url)
        assert response.status_code == 200
        # Return last hourly record
        assert response.json() == {
            "timestamps": [1562457600000],
            "values": [280],
        }

    def test_from_to(self, client):
        url = (
            self.base_url + f"&fr={1561986000000 - 100000}&to={1561993200000 + 100000}"
        )
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561986000000, 1561989600000, 1561993200000],
            "values": [70, 100, 130],
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
            "values": [70, 100, 130],
        }

    def test_from_spot(self, client):
        # from timestamp at h4
        # expect value at h4, h5 and h6
        url = self.base_url + f"&fr={1561993200000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561993200000, 1561996800000, 1562000400000],
            "values": [130, 140, 160],
        }

    def test_from_just_after_spot(self, client):
        # from timestamp just after h4
        # expect value at h5 and h6
        url = self.base_url + f"&fr={1561993200000 + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561996800000, 1562000400000],
            "values": [140, 160],
        }

    def test_out_of_range(self, client):
        url = self.base_url + f"&fr={1561986000000 * 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [],
            "values": [],
        }

    def test_to_only(self, client):
        # to timestamp of block 14
        # expect values at h2, h3 and h4
        url = self.base_url + f"&to={1561993200000 + 100000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1561986000000, 1561989600000, 1561993200000],
            "values": [70, 100, 130],
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
            "values": [140, 160],
        }


class TestCountDaily:
    base_url = "/metrics/utxos?r=24h"

    def test_default(self, client):
        url = self.base_url
        response = client.get(url)
        assert response.status_code == 200
        # Return last daily record
        assert response.json() == {
            "timestamps": [1562457600000],
            "values": [280],
        }

    def test_from_to(self, client):
        url = (
            self.base_url + f"&fr={1562112000000 - 100000}&to={1562284800000 + 100000}"
        )
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562112000000, 1562198400000, 1562284800000],
            "values": [190, 220, 250],
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
            "values": [190, 220, 250],
        }

    def test_from_spot(self, client):
        # from timestamp at d4
        # expect value at d4, d5 and d6
        url = self.base_url + f"&fr={1562284800000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562284800000, 1562371200000, 1562457600000],
            "values": [250, 260, 280],
        }

    def test_from_just_after_spot(self, client):
        # from timestamp just after d4
        # expect value at d5 and d6
        url = self.base_url + f"&fr={1562284800000 + 1}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562371200000, 1562457600000],
            "values": [260, 280],
        }

    def test_out_of_range(self, client):
        url = self.base_url + f"&fr={1561986000000 * 10}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [],
            "values": [],
        }

    def test_to_only(self, client):
        # to timestamp of block 26
        # expect values at d2, d3 and d4
        url = self.base_url + f"&to={1562284800000 + 100000}"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {
            "timestamps": [1562112000000, 1562198400000, 1562284800000],
            "values": [190, 220, 250],
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
            "values": [260, 280],
        }


def test_default_r_is_block(client):
    url = "/metrics/utxos"
    response = client.get(url)
    assert response.status_code == 200
    assert response.json() == {
        "timestamps": [1562457600000 + 100000],
        "values": [290],
    }

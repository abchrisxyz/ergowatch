"""
Making sure the mock api works as intended.
"""
import requests
import pytest

from fixtures import genesis_env
from fixtures import bootstrapped_env


@pytest.mark.order(1)
class TestGenesisApi:
    def test_env_starts_api(self, genesis_env):
        """
        Test fixture starts mock api server
        """
        r = requests.get(f"http://localhost:9053/check")
        assert r.status_code == 200
        assert r.text == "working"

    def test_info_has_height_1(self, genesis_env):
        """
        Test node height is set to 1
        """
        r = requests.get(f"http://localhost:9053/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 1


@pytest.mark.order(1)
class TestBoostrappedApi:
    def test_env_starts_api(self, bootstrapped_env):
        """
        Test fixture starts mock api server
        """
        r = requests.get(f"http://localhost:9053/check")
        assert r.status_code == 200
        assert r.text == "working"

    def test_info_has_height_600k(self, bootstrapped_env):
        """
        Test node height is set to 600k
        """
        r = requests.get(f"http://localhost:9053/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 600_000

    def test_blocks_at(self, bootstrapped_env):
        """
        Test api returns right block
        """
        url = "http://localhost:9053/blocks/at/{}"

        h = 599_999
        r = requests.get(url.format(h))
        assert r.status_code == 200
        res = r.json()
        assert len(res) == 1
        assert (
            res[0] == "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1"
        )

        h = 600_000
        r = requests.get(url.format(h))
        assert r.status_code == 200
        res = r.json()
        assert len(res) == 1
        assert (
            res[0] == "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        )

    def test_blocks(self, bootstrapped_env):
        """
        Test api returns right block
        """
        url = "http://localhost:9053/blocks/{}"

        h = "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1"
        r = requests.get(url.format(h))
        assert r.status_code == 200
        res = r.json()
        assert res["header"]["height"] == 599_999
        assert res["header"]["id"] == h

        h = "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        r = requests.get(url.format(h))
        assert r.status_code == 200
        res = r.json()
        assert res["header"]["height"] == 600_000
        assert res["header"]["id"] == h

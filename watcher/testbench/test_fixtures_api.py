"""
Making sure the mock api works as intended.
"""
import requests
import pytest

from fixtures import genesis_env
from fixtures import block_600k_env
from fixtures import token_minting_env
from fixtures import core_rollback_env


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
class Test600kApi:
    def test_env_starts_api(self, block_600k_env):
        """
        Test fixture starts mock api server
        """
        r = requests.get(f"http://localhost:9053/check")
        assert r.status_code == 200
        assert r.text == "working"

    def test_info_has_height_600k(self, block_600k_env):
        """
        Test node height is set to 600k
        """
        r = requests.get(f"http://localhost:9053/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 600_000

    def test_blocks_at(self, block_600k_env):
        """
        Test api returns right block
        """
        url = "http://localhost:9053/blocks/at/{}"

        h = 600_000
        r = requests.get(url.format(h))
        assert r.status_code == 200
        res = r.json()
        assert len(res) == 1
        assert (
            res[0] == "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        )

    def test_blocks(self, block_600k_env):
        """
        Test api returns right block
        """
        url = "http://localhost:9053/blocks/{}"

        h = "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        r = requests.get(url.format(h))
        assert r.status_code == 200
        res = r.json()
        assert res["header"]["height"] == 600_000
        assert res["header"]["id"] == h


@pytest.mark.order(1)
class TestTokenMintingApi:
    def test_env_starts_api(self, token_minting_env):
        """
        Test fixture starts mock api server
        """
        r = requests.get(f"http://localhost:9053/check")
        assert r.status_code == 200
        assert r.text == "working"

    def test_info_has_height_600k(self, token_minting_env):
        """
        Test node height is set to 600k
        """
        r = requests.get(f"http://localhost:9053/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 600_001

    def test_blocks_at(self, token_minting_env):
        """
        Test api returns right block
        """
        url = "http://localhost:9053/blocks/at/{}"

        h = 600_001
        r = requests.get(url.format(h))
        assert r.status_code == 200
        res = r.json()
        assert len(res) == 1
        assert (
            res[0] == "c17fea7f193b020fbe212037a6543702a9580b71313e91ec0cb9575875ba9c2e"
        )

    def test_blocks(self, token_minting_env):
        """
        Test api returns right block
        """
        url = "http://localhost:9053/blocks/{}"

        h = "c17fea7f193b020fbe212037a6543702a9580b71313e91ec0cb9575875ba9c2e"
        r = requests.get(url.format(h))
        assert r.status_code == 200
        res = r.json()
        assert res["header"]["height"] == 600_001
        assert res["header"]["id"] == h


# skipping for now, need to confirm how node behaves with multiple blocks at same height
@pytest.mark.skip()
@pytest.mark.order(1)
class TestCoreRollbackApi:
    def test_env_starts_api(self, core_rollback_env):
        """
        Test fixture starts mock api server
        """
        r = requests.get(f"http://localhost:9053/check")
        assert r.status_code == 200
        assert r.text == "working"

    def test_info_has_height_600k(self, block_600k_env):
        """
        Test node height is set to 600k
        """
        r = requests.get(f"http://localhost:9053/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 600_000

    def test_blocks_at(self, block_600k_env):
        """
        Test api returns right block
        """
        url = "http://localhost:9053/blocks/at/{}"

        h = 600_000
        r = requests.get(url.format(h))
        assert r.status_code == 200
        res = r.json()
        assert len(res) == 1
        assert (
            res[0] == "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        )

    def test_blocks(self, block_600k_env):
        """
        Test api returns right block
        """
        url = "http://localhost:9053/blocks/{}"

        h = "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        r = requests.get(url.format(h))
        assert r.status_code == 200
        res = r.json()
        assert res["header"]["height"] == 600_000
        assert res["header"]["id"] == h

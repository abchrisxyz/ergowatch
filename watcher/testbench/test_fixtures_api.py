"""
Making sure the mock api works as intended.
"""
import requests
import pytest

from fixtures import genesis_env
from fixtures import block_600k_env
from fixtures import token_minting_env
from fixtures import fork_env


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


@pytest.mark.order(0)
class TestForkApi:
    def test_stepping_is_implemented(self, fork_env):
        """
        Stepping can be activated
        """
        r = requests.get(f"http://localhost:9053/enable_stepping")
        assert r.status_code == 200

        # Starting at 672_220
        r = requests.get(f"http://localhost:9053/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 672_220

        # At first, there's just 1 block at 672_220
        r = requests.get("http://localhost:9053/blocks/at/672220")
        assert r.status_code == 200
        res = r.json()
        assert len(res) == 1

        # Step to reveal next block
        r = requests.get(f"http://localhost:9053/step")
        assert r.status_code == 200

        # Still at height 672_220
        r = requests.get(f"http://localhost:9053/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 672_220

        # But now there's two blocks at 672_220
        r = requests.get("http://localhost:9053/blocks/at/672220")
        assert r.status_code == 200
        res = r.json()
        assert len(res) == 2

        # Step to reveal next block
        r = requests.get(f"http://localhost:9053/step")
        assert r.status_code == 200

        # Now at height 672_221
        r = requests.get(f"http://localhost:9053/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 672_221

        # There's one block at 672_221
        r = requests.get("http://localhost:9053/blocks/at/672221")
        assert r.status_code == 200
        res = r.json()
        assert len(res) == 1

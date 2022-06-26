import pytest
import requests
import copy

from fixtures.api import mock_api
from fixtures.blocks import block_600k
from fixtures.db.sql import DEFAULT_BOX_VALUE
from fixtures.scenario.genesis import GENESIS_BOX


@pytest.mark.order(1)
class TestGenesisApi:
    """
    Test empty api (no blocks)
    """

    @pytest.fixture
    def api(self, mock_api):
        return mock_api

    def test_info_height(self, api):
        r = requests.get(f"{api.url}/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 0

    def test_blocks_at(self, api):
        r = requests.get(f"{api.url}/blocks/at/1")
        assert r.status_code == 200
        assert r.json() == []

    def test_blocks(self, api):
        header_id = block_600k["header"]["id"]
        r = requests.get(f"{api.url}/blocks/{header_id}")
        assert r.status_code == 404

    def test_genesis_boxes(self, api):
        r = requests.get(f"{api.url}/utxo/genesis")
        assert r.status_code == 200
        boxes = r.json()
        assert len(boxes) == 1
        assert boxes[0]["boxId"] == GENESIS_BOX["boxId"]
        assert boxes[0]["value"] == DEFAULT_BOX_VALUE
        assert boxes[0]["transactionId"] == "0" * 64
        assert boxes[0]["creationHeight"] == 0

    def test_add_next_block(self, api):
        next_block = copy.deepcopy(block_600k)
        next_block["header"]["id"] = "dummy-header-for-block-1"
        next_block["header"]["height"] = 1
        api.add_block(next_block)
        # Check it got included
        r = requests.get(f"{api.url}/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 1
        r = requests.get(f"{api.url}/blocks/at/1")
        assert r.status_code == 200
        assert r.json() == ["dummy-header-for-block-1"]


@pytest.mark.order(1)
class TestPopulatedApi:
    """
    Test non-empty api
    """

    @pytest.fixture
    def api(self, mock_api):
        mock_api.set_blocks([block_600k])
        return mock_api

    def test_info_height(self, api):
        r = requests.get(f"{api.url}/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 600_000

    def test_blocks_at(self, api):
        r = requests.get(f"{api.url}/blocks/at/600000")
        assert r.status_code == 200
        assert r.json() == [block_600k["header"]["id"]]

    def test_blocks(self, api):
        header_id = block_600k["header"]["id"]
        r = requests.get(f"{api.url}/blocks/{header_id}")
        assert r.status_code == 200
        block = r.json()
        assert block["header"]["height"] == 600_000
        assert block["header"]["id"] == header_id

    def test_genesis_boxes(self, api):
        r = requests.get(f"{api.url}/utxo/genesis")
        assert r.status_code == 200
        boxes = r.json()
        assert len(boxes) == 1
        assert len(boxes) == 1
        assert boxes[0]["boxId"] == GENESIS_BOX["boxId"]
        assert boxes[0]["value"] == DEFAULT_BOX_VALUE
        assert boxes[0]["transactionId"] == "0" * 64
        assert boxes[0]["creationHeight"] == 0

    def test_add_next_block(self, api):
        next_block = copy.deepcopy(block_600k)
        next_block["header"]["id"] = "dummy-header-for-block-600001"
        next_block["header"]["height"] = 600_001
        api.add_block(next_block)
        # Check it got included
        r = requests.get(f"{api.url}/info")
        assert r.status_code == 200
        assert r.json()["fullHeight"] == 600_001
        r = requests.get(f"{api.url}/blocks/at/600001")
        assert r.status_code == 200
        assert r.json() == ["dummy-header-for-block-600001"]

    def test_add_non_contiguous_block(self, api):
        next_block = copy.deepcopy(block_600k)
        next_block["header"]["id"] = "dummy-header-for-block-600002"
        next_block["header"]["height"] = 600_002
        with pytest.raises(AssertionError):
            api.add_block(next_block)

    def test_add_past_block(self, api):
        next_block = copy.deepcopy(block_600k)
        next_block["header"]["id"] = "dummy-header-for-block-599999"
        next_block["header"]["height"] = 599_999
        with pytest.raises(AssertionError):
            api.add_block(next_block)

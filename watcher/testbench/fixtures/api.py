# """
# Node API mockup.
# """
import shlex
import subprocess
import sys
import os
from pathlib import Path
import json
from typing import List

import pytest
import bottle
import requests


MOCK_NODE_HOST = "localhost:9053"

# Genesis header and tx id.
# This value is hard coded in Watcher, so has to match.
GENESIS_ID = "0" * 64


class API(bottle.Bottle):
    """
    Configurable mock node API.

    Pass collections of blocks to be returned by block request.
    """

    def __init__(self) -> None:
        super().__init__()
        self._blocks = []

        # Backdoor routes
        self.add_route(bottle.Route(self, "/check", "GET", self._check))
        self.add_route(bottle.Route(self, "/set_blocks", "POST", self._set_blocks))
        self.add_route(bottle.Route(self, "/add_block", "POST", self._add_block))

        # Actual node routes
        self.add_route(bottle.Route(self, "/info", "GET", self.get_info))
        self.add_route(
            bottle.Route(self, "/blocks/at/:height", "GET", self.get_blocks_at)
        )
        self.add_route(bottle.Route(self, "/blocks/:header", "GET", self.get_blocks))
        self.add_route(
            bottle.Route(self, "/utxo/genesis", "GET", self.get_genesis_boxes)
        )

    @property
    def height(self):
        return self._blocks[-1]["header"]["height"] if self._blocks else 0

    def _check(self):
        return "working"

    def _enable_stepping(self):
        # Make sure this doesn't get called while stepping half way through blocks.
        # Index should be set to last block when this is called.
        assert self.index == len(self._blocks) - 1
        self.index = 0

    def _set_blocks(self):
        """
        Set the mock node's block list.
        Used to configure a mock api before usage.
        """
        blocks = bottle.request.json
        self._blocks = blocks
        self._validate_blocks()

    def _add_block(self):
        """
        Add a block to mock node's block list.
        Used to configure a mock api during usage.
        """
        block = bottle.request.json
        self._blocks.append(block)
        self._validate_blocks()

    def get_info(self):
        """
        Returns a dummy info response with height set to last block.
        """
        res = {
            "currentTime": 1643328102235,
            "network": "mainnet",
            "name": "ergo-mainnet-4.0.20.1",
            "stateType": "utxo",
            "difficulty": 1859476026032128,
            "bestFullHeaderId": "0bb72b432d30c015d09a4b2c84ecef9132da577b357cf0752234ed540c210049",
            "bestHeaderId": "0bb72b432d30c015d09a4b2c84ecef9132da577b357cf0752234ed540c210049",
            "peersCount": 30,
            "unconfirmedCount": 16,
            "appVersion": "4.0.20.1-0-ae2d7ab6-20220110-1156-SNAPSHOT",
            "stateRoot": "13889f9ed5cc4e701eb6804821aa4b8554cc58cb31b32223beebd5e8968431d318",
            "genesisBlockId": "b0244dfc267baca974a4caee06120321562784303a8a688976ae56170e4d175b",
            "previousFullHeaderId": "90fcf4fabed4a941a5a2af62fb42dfef320dd3b31f8b19b61f3bf3e9c152d31c",
            "fullHeight": self.height,
            "headersHeight": self.height,
            "stateVersion": "0bb72b432d30c015d09a4b2c84ecef9132da577b357cf0752234ed540c210049",
            "fullBlocksScore": 761195617357416900000,
            "launchTime": 1642631699780,
            "lastSeenMessageTime": 1643328071826,
            "headersScore": 761195617357416900000,
            "parameters": {
                "outputCost": 100,
                "tokenAccessCost": 100,
                "maxBlockCost": 7030268,
                "height": 672768,
                "maxBlockSize": 1271009,
                "dataInputCost": 100,
                "blockVersion": 2,
                "inputCost": 2000,
                "storageFeeFactor": 1250000,
                "minValuePerByte": 360,
            },
            "isMining": False,
        }
        bottle.response.content_type = "application/json"
        return json.dumps(res)

    def get_blocks_at(self, height):
        """
        Returns headers of blocks at given height.
        """
        res = [
            b["header"]["id"]
            for b in self._blocks
            if b["header"]["height"] == int(height)
        ]
        bottle.response.content_type = "application/json"
        return json.dumps(res)

    def get_blocks(self, header):
        """
        Returns block with given header.
        """
        bottle.response.content_type = "application/json"
        blocks = [b for b in self._blocks if b["header"]["id"] == header]
        assert len(blocks) <= 1
        if len(blocks) == 0:
            return bottle.HTTPError(status=404, body="not found")
        return json.dumps(blocks[0])

    def get_genesis_boxes(self):
        """
        Actual node return 3 genesis boxes.

        Here return only 1 with modified box id, tx id and value.
        Box values changed to 1000 for easier case building and assertions.
        """
        boxes = [
            {
                "boxId": "base-box1",
                "value": 1000,
                "ergoTree": "101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f",
                "assets": [],
                "creationHeight": 0,
                "additionalRegisters": {},
                "transactionId": "0" * 64,
                "index": 0,
            }
        ]
        return json.dumps(boxes)

    def _validate_blocks(self):
        # Ensure heights are continuous (allowing for duplicates)
        heights = [b["header"]["height"] for b in self._blocks]
        if heights:
            assert heights[-1] - heights[0] == len(set(heights)) - 1
        # Check ordering
        sorted_heights = [h for h in heights]
        sorted_heights.sort()
        assert sorted_heights == heights


class ApiUtil:
    def __init__(self):
        self.url = "http://localhost:9053"

    def check(self):
        res = requests.get(f"{self.url}/check")
        assert res.status_code == 200

    def set_blocks(self, blocks: List):
        r = requests.post(f"{self.url}/set_blocks", json=blocks)
        assert r.status_code == 200

    def add_block(self, block):
        r = requests.post(f"{self.url}/add_block", json=block)
        assert r.status_code == 200

    def reset(self):
        r = requests.get(f"{self.url}/reset")
        assert r.status_code == 200


@pytest.fixture
def mock_api():
    with MockApi() as api:
        yield ApiUtil()


_api = API()


class MockApi:
    """
    Utility class turning API's into context managers.
    """

    def __init__(self):
        self._api: str = "_api"
        self._p: subprocess.Popen = None

    def __enter__(self):
        os.chdir(Path(__file__).parent.absolute())
        args = [sys.executable]
        args.extend(shlex.split(f"-m bottle -b {MOCK_NODE_HOST} api:{self._api}"))
        print(f"Subprocess args: {args}")
        self._p = subprocess.Popen(args)
        # Give it some time to start up before allowing tests to query the api
        try:
            self._p.wait(0.3)
        except subprocess.TimeoutExpired:
            pass
        # If another api is still running, this one won't be able to bind and will fail.
        # Here we check it has indeed started.
        # If this fails, an orphaned api is likely still running
        assert self._p.returncode is None

    def __exit__(self, exception_type, exception_value, traceback):
        self._p.kill()

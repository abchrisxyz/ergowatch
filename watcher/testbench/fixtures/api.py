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
from typing import Dict

import pytest
import bottle
import requests

from fixtures.blocks import genesis_block
from fixtures.blocks import block_600k
from fixtures.blocks import token_minting_block
from fixtures.blocks.fork import block_672220
from fixtures.blocks.fork import block_672220_fork
from fixtures.blocks.fork import block_672221
from fixtures.blocks.bootstrap import block_672220 as bootstrap_672220
from fixtures.blocks.bootstrap import block_672221 as bootstrap_672221
from fixtures.blocks.balances import block_619221 as bal_619221
from fixtures.blocks.balances import block_619222 as bal_619222
from fixtures.blocks.balances_fork import block_698626_fork as bal_698626_fork
from fixtures.blocks.balances_fork import block_698626 as bal_698626_main
from fixtures.blocks.balances_fork import block_698627 as bal_698627

MOCK_NODE_HOST = "localhost:9053"

# Genesis header and tx id.
# This value is hard coded in Watcher, so has to match.
GENESIS_ID = "0" * 64


class API(bottle.Bottle):
    """
    Configurable mock node API.

    Pass collections of blocks to be returned by block request.
    """

    def __init__(self, blocks=[]) -> None:
        super().__init__()

        # Full series of blocks.
        self._blocks = blocks

        # The index of the last visible block.
        # Used to implement stepping. Incremented through step() calls.
        self.index = len(blocks) - 1

        # Ensure heights are continuous (allowing for duplicates)
        heights = [b["header"]["height"] for b in blocks]
        if heights:
            assert heights[-1] - heights[0] == len(set(heights)) - 1

        # Backdoor routes
        self.add_route(bottle.Route(self, "/check", "GET", self._check))
        self.add_route(
            bottle.Route(self, "/enable_stepping", "GET", self._enable_stepping)
        )
        self.add_route(bottle.Route(self, "/step", "GET", self._step))
        self.add_route(bottle.Route(self, "/set_blocks", "POST", self._set_blocks))

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
    def heights(self):
        blocks = self._blocks[0 : self.index + 1]
        return [b["header"]["height"] for b in blocks]

    @property
    def blocks(self):
        blocks = self._blocks[0 : self.index + 1]
        return {b["header"]["id"]: b for b in blocks}

    def _check(self):
        return "working"

    def _enable_stepping(self):
        # Make sure this doesn't get called while stepping half way through blocks.
        # Index should be set to last block when this is called.
        assert self.index == len(self._blocks) - 1
        self.index = 0

    def _step(self):
        self.index = min(len(self._blocks), self.index + 1)

    def _set_blocks(self):
        """
        Add a block to mock node's block list.
        Used to prepare a mock api before/during usage.
        """
        blocks = bottle.request.json
        self._blocks = blocks

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
            "fullHeight": self.heights[-1],
            "headersHeight": self.heights[-1],
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
            h for (h, b) in self.blocks.items() if b["header"]["height"] == int(height)
        ]
        bottle.response.content_type = "application/json"
        return json.dumps(res)

    def get_blocks(self, header):
        """
        Returns block with given header.
        """
        bottle.response.content_type = "application/json"
        if header not in self.blocks:
            return bottle.HTTPError(code=404, output="not found")
        return json.dumps(self.blocks[header])

    def get_genesis_boxes(self):
        """
        Return mainnet genesis blocks
        """
        boxes = [
            {
                "boxId": "b69575e11c5c43400bfead5976ee0d6245a1168396b2e2a4f384691f275d501c",
                "value": 93409132500000000,
                "ergoTree": "101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f",
                "assets": [],
                "creationHeight": 0,
                "additionalRegisters": {},
                "transactionId": "0000000000000000000000000000000000000000000000000000000000000000",
                "index": 0,
            },
            {
                "boxId": "b8ce8cfe331e5eadfb0783bdc375c94413433f65e1e45857d71550d42e4d83bd",
                "value": 1000000000,
                "ergoTree": "10010100d17300",
                "assets": [],
                "creationHeight": 0,
                "additionalRegisters": {
                    "R4": "0e4030303030303030303030303030303030303031346332653265376533336435316165376536366636636362363934326333343337313237623336633333373437",
                    "R5": "0e42307864303761393732393334363864393133326335613261646162326535326132333030396536373938363038653437623064323632336337653365393233343633",
                    "R6": "0e464272657869743a20626f746820546f727920736964657320706c617920646f776e207269736b206f66206e6f2d6465616c20616674657220627573696e65737320616c61726d",
                    "R7": "0e54e8bfb0e8af84efbc9ae5b9b3e8a1a1e38081e68c81e7bbade38081e58c85e5aeb9e28094e28094e696b0e697b6e4bba3e5ba94e5afb9e585a8e79083e58c96e68c91e68898e79a84e4b8ade59bbde4b98be98193",
                    "R8": "0e45d094d0b8d0b2d0b8d0b4d0b5d0bdd0b4d18b20d0a7d0a2d09fd09720d0b2d18bd180d0b0d181d182d183d18220d0bdd0b02033332520d0bdd0b020d0b0d0bad186d0b8d18e",
                },
                "transactionId": "0000000000000000000000000000000000000000000000000000000000000000",
                "index": 0,
            },
            {
                "boxId": "5527430474b673e4aafb08e0079c639de23e6a17e87edd00f78662b43c88aeda",
                "value": 4330791500000000,
                "ergoTree": "100e040004c094400580809cde91e7b0010580acc7f03704be944004808948058080c7b7e4992c0580b4c4c32104fe884804c0fd4f0580bcc1960b04befd4f05000400ea03d192c1b2a5730000958fa373019a73029c73037e997304a305958fa373059a73069c73077e997308a305958fa373099c730a7e99730ba305730cd193c2a7c2b2a5730d00d5040800",
                "assets": [],
                "creationHeight": 0,
                "additionalRegisters": {
                    "R4": "0e6f98040483030808cd039bb5fe52359a64c99a60fd944fc5e388cbdc4d37ff091cc841c3ee79060b864708cd031fb52cf6e805f80d97cde289f4f757d49accf0c83fb864b27d2cf982c37f9a8b08cd0352ac2a471339b0d23b3d2c5ce0db0e81c969f77891b9edf0bda7fd39a78184e7"
                },
                "transactionId": "0000000000000000000000000000000000000000000000000000000000000000",
                "index": 0,
            },
        ]
        return json.dumps(boxes)


class API2(bottle.Bottle):
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


BLOCK_COLLECTIONS = {
    "genesis": [genesis_block],
    "600k": [block_600k],
    "token_minting": [token_minting_block],
    "fork": [block_672220_fork, block_672220, block_672221],
    "bootstrap": [bootstrap_672220, bootstrap_672221],
    "balances": [bal_619221, bal_619222],
    "balances_fork": [bal_698626_fork, bal_698626_main, bal_698627],
}

# API variants
_api = API2()
api_genesis = API([genesis_block])
api_600k = API(BLOCK_COLLECTIONS["600k"])
api_token_minting = API(BLOCK_COLLECTIONS["token_minting"])
api_fork = API(BLOCK_COLLECTIONS["fork"])
api_bootstrap = API(BLOCK_COLLECTIONS["bootstrap"])
api_balances = API(BLOCK_COLLECTIONS["balances"])
api_balances_fork = API(BLOCK_COLLECTIONS["balances_fork"])


def get_api_blocks(api_variant: str) -> List[Dict]:
    return BLOCK_COLLECTIONS[api_variant]


class MockApi:
    """
    Utility class turning API's into context managers.
    """

    def __init__(self, variant: str = None):
        self._api: str = f"api_{variant}" if variant else "_api"
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

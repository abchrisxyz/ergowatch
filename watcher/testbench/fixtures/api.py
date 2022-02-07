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
import copy

import bottle

from fixtures.blocks import genesis_block
from fixtures.blocks import block_600k
from fixtures.blocks import token_minting_block
from fixtures.blocks import core_block
from fixtures.blocks.fork import block_672220
from fixtures.blocks.fork import block_672220_fork
from fixtures.blocks.fork import block_672221

MOCK_NODE_HOST = "localhost:9053"


class API(bottle.Bottle):
    """
    Configurable mock node API.

    Pass collections of blocks to be returned by block request.
    """

    def __init__(self, blocks) -> None:
        super().__init__()

        # Full series of blocks.
        self._blocks = blocks

        # The index of the last visible block.
        # Used to implement stepping. Incremented through step() calls.
        self.index = len(blocks) - 1

        # Ensure heights are continuous (allowing for duplicates)
        heights = [b["header"]["height"] for b in blocks]
        assert heights[-1] - heights[0] == len(set(heights)) - 1

        self.add_route(bottle.Route(self, "/check", "GET", self.check))
        self.add_route(
            bottle.Route(self, "/enable_stepping", "GET", self.enable_stepping)
        )
        self.add_route(bottle.Route(self, "/step", "GET", self.step))
        self.add_route(bottle.Route(self, "/info", "GET", self.get_info))
        self.add_route(
            bottle.Route(self, "/blocks/at/:height", "GET", self.get_blocks_at)
        )
        self.add_route(bottle.Route(self, "/blocks/:header", "GET", self.get_blocks))

    @property
    def heights(self):
        blocks = self._blocks[0 : self.index + 1]
        return [b["header"]["height"] for b in blocks]

    @property
    def blocks(self):
        blocks = self._blocks[0 : self.index + 1]
        return {b["header"]["id"]: b for b in blocks}

    def check(self):
        return "working"

    def enable_stepping(self):
        # Make sure this doesn't get called while stepping half way through blocks.
        # Index should be set to last block when this is called.
        assert self.index == len(self._blocks) - 1
        self.index = 0

    def step(self):
        self.index = min(len(self._blocks), self.index + 1)

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


# Dummy block used to indicate block 600k is main chain
rollback_600_001 = copy.deepcopy(block_600k)
rollback_600_001["header"]["parent_id"] = block_600k["header"]["id"]
rollback_600_001["header"]["height"] = 600_001

BLOCK_COLLECTIONS = {
    "genesis": [genesis_block],
    "600k": [block_600k],
    "token_minting": [token_minting_block],
    "core_rollback": [core_block, block_600k, rollback_600_001],
    "fork": [block_672220_fork, block_672220, block_672221],
}

# API variants
api_genesis = API([genesis_block])
api_600k = API(BLOCK_COLLECTIONS["600k"])
api_token_minting = API(BLOCK_COLLECTIONS["token_minting"])
api_fork = API(BLOCK_COLLECTIONS["fork"])


def get_api_blocks(api_variant: str) -> List[Dict]:
    return BLOCK_COLLECTIONS[api_variant]


class MockApi:
    """
    Utility class turning API's into context managers.
    """

    def __init__(self, variant: str):
        self._api: str = f"api_{variant}"
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

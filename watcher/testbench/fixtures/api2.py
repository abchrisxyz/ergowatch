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

import bottle

from fixtures.blocks import genesis_block
from fixtures.blocks import block_600k
from fixtures.blocks import token_minting_block
from fixtures.blocks import core_block

MOCK_NODE_HOST = "localhost:9053"


class API(bottle.Bottle):
    """
    Configurable mock node API.

    Pass collections of blocks to be returned by block request.

    Set buffered to true to have the node height increase, after each
    block request, untill last block. Default is false, meaning node
    will report height from last block.
    """

    def __init__(self, blocks, buffered=False) -> None:
        super().__init__()
        self.counter = 0
        self.blocks = {b["header"]["id"]: b for b in blocks}
        self.heights = [b["header"]["height"] for b in blocks]

        # Ensure heights are continuous (allowing for duplicates)
        assert self.heights[-1] - self.heights[0] == len(set(self.heights)) - 1

        self.add_route(bottle.Route(self, "/check", "GET", self.check))
        self.add_route(bottle.Route(self, "/info", "GET", self.get_info))
        self.add_route(
            bottle.Route(self, "/blocks/at/:height", "GET", self.get_blocks_at)
        )
        self.add_route(bottle.Route(self, "/blocks/:header", "GET", self.get_blocks))

    def check(self):
        return "working"

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


BLOCK_COLLECTIONS = {
    "genesis": [genesis_block],
    "600k": [block_600k],
    "token_minting": [token_minting_block],
    "core_rollback": [core_block, block_600k],
}

# API variants
api_genesis = API([genesis_block])
api_600k = API(BLOCK_COLLECTIONS["600k"])
api_token_minting = API(BLOCK_COLLECTIONS["token_minting"])
api_core_rollback = API(BLOCK_COLLECTIONS["core_rollback"])


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

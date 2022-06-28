from . import syntax
from .addresses import AddressCatalogue as AC
from .addresses import Box
from fixtures.scenario.genesis import GENESIS_BOX

# Actual genesis timestamp
GENSIS_TIMESTAMP = 1561978800000

# Time interval between two scenario blocks
TIMESTAMP_INTERVAL = 100_000  # 100 seconds


class Scenario:
    """
    A test scenario described by a simple syntax.
    """

    DT = TIMESTAMP_INTERVAL

    def __init__(
        self,
        desc: str,
        parent_height: int,
        first_ts: int,
        main_only: bool = False,
    ):
        """
        `desc`: scenario description string
        `parent_height`: height of last block before first scenario block
        `first_ts`: timestamp of first scenario block
        `main_only`: ignore blocks not part of main chain
        """
        self._desc = desc
        self._parent_height = parent_height
        self._first_ts = first_ts
        self._blocks, self._shortid2longid = syntax.parse(
            self._desc,
            parent_height + 1,
            first_ts,
        )
        if main_only:
            self._drop_fork_blocks()
        # Add genesis box to id map
        self._shortid2longid["base-box1"] = GENESIS_BOX["boxId"]
        # Generate reverse id map
        self._longid2shortid = {
            long: short for short, long in self._shortid2longid.items()
        }
        # If set, masks blocks starting at given index
        self._mask: int = None

    @property
    def parent_height(self):
        return self._parent_height

    @property
    def start_height(self):
        return self._parent_height + 1

    @property
    def first_ts(self):
        return self._first_ts

    @property
    def parent_ts(self):
        """Timestamp of scenario parent, if not genesis block"""
        return self._first_ts - TIMESTAMP_INTERVAL

    @property
    def genesis_ts(self):
        """Actual genesis timestamp"""
        return GENSIS_TIMESTAMP

    @property
    def dt(self):
        """Timestamp difference between two scenario blocks"""
        return TIMESTAMP_INTERVAL

    @property
    def blocks(self):
        """Block data for mock API"""
        if self._mask is None:
            return self._blocks
        return self._blocks[0 : self._mask]

    def mask(self, index: int):
        """Mask blocks starting from `index`"""
        self._mask = index

    def unmask(self):
        """Undo any previous masking"""
        self._mask = None

    def id(self, short_id: str) -> str:
        """Returns actual id from short id"""
        return self._shortid2longid[short_id]

    def short_id(self, long_id: str) -> str:
        """Returns short id from actual id"""
        return self._longid2shortid[long_id]

    def box(self, box_id) -> Box:
        """Return Box from `box_id`, long or short"""
        if len(box_id) == 64:
            return AC.box_from_short_id(self._longid2shortid[box_id])
        return AC.box_from_short_id(box_id)

    def address(self, box_id) -> str:
        """Return address from `box_id`, long or short"""
        return self.box(box_id).address

    def ergo_tree(self, box_id) -> str:
        """Return ergo_tree from `box_id`, long or short"""
        return self.box(box_id).ergo_tree

    def _drop_fork_blocks(self):
        """Drop blocks not part of main chain"""
        blocks = self._blocks
        main_chain = [blocks.pop()]
        blocks.reverse()
        for block in blocks:
            if block["header"]["id"] == main_chain[-1]["header"]["parentId"]:
                main_chain.append(block)
        main_chain.reverse()
        self._blocks = main_chain

"""
Syntax describing fictive blocks to be used in test cases.

Comments start with `//`.
Blocks start with `block-<id>`.
Default parent of a block if previous block.
Block parent can be set explicitly: block-<id>-<parent_id>
Tx id derived from index and block id.
Txs separated by `--`.
Tx inputs and ouputs separated by `>`.
One input or output per line.
Data-inputs enclosed in {curly brackets}.

Example:
    // comments
    block-a
    
    pub1-box1  100 (tokenx: 20, tokeny: 3)
    con1-box1  100
    -->
    cex1-box1   10
    pub1-box2  190 (token: 20)

    pub1-box2 190
    {cex1-box1} // data-input
    -->
    pub2-box1 189
    fees-box1   1

    // This block is not part of main chain
    block-x
    ...

    block-b-a
    ...

    block-c
"""
import re
from typing import List, Dict, Tuple
from collections import namedtuple

from fixtures.addresses import AddressCatalogue as AC
from fixtures.api import GENESIS_ID


BlockAttributes = namedtuple(
    "BlockAttributes", ["id", "header_id", "timestamp", "height", "parent_id"]
)


def parse(desc: str, start_height: int):
    blocks = split_blocks(desc)
    attrs = derive_block_attributes(blocks, start_height)
    return list(map(parse_block, blocks, attrs))


def split_blocks(desc: str) -> List[str]:
    """Split description string into block strings"""
    # Remove comments
    p = re.compile(r"//.*", re.MULTILINE)
    desc = p.sub("", desc)
    # Split
    m = re.findall(r"(?=block-).+?(?=block-|\Z)", desc, re.DOTALL | re.MULTILINE)
    # Cleanup whitespace
    blocks = [re.sub(r"\s+", " ", b) for b in m]
    return blocks


def parse_block_id(s: str) -> str:
    return re.match(r"^block-([\w]+)-?([\w]+)?", s).groups()


def derive_block_attributes(
    blocks: List[str], start_height: int
) -> List[BlockAttributes]:
    """
    Derive block attributes based on start height and parent-child relationships.
    """

    def label2id(label):

        # if "-" in label:

        return f"block-{label}"

    lbl, parent_lbl = parse_block_id(blocks[0])
    assert parent_lbl is None
    id2h = {label2id(lbl): start_height}
    t0 = 1234560000000
    dt = 100000
    attrs = [BlockAttributes(lbl, label2id(lbl), t0, start_height, GENESIS_ID)]
    for block in blocks[1:]:
        lbl, parent_lbl = parse_block_id(block)
        if parent_lbl is None:
            parent_lbl = attrs[-1].id
        block_id, parent_id = [label2id(s) for s in (lbl, parent_lbl)]
        h = id2h[parent_id] + 1
        t = t0 + dt * (h - start_height)
        id2h[block_id] = h
        attrs.append(BlockAttributes(lbl, block_id, t, h, parent_id))
    return attrs


def parse_block(s: str, attrs: BlockAttributes) -> Dict:
    return {
        "header": {
            "votes": "000000",
            "timestamp": attrs.timestamp,
            "size": 123,
            "height": attrs.height,
            "id": attrs.header_id,
            "parentId": attrs.parent_id,
        },
        "blockTransactions": {
            "headerId": attrs.header_id,
            "transactions": parse_txs(s, attrs),
            "blockVersion": 2,
            "size": 234,
        },
        "size": 1234,
    }


def parse_txs(s: str, block_attrs: BlockAttributes) -> List:
    txs = re.split(r"--", s)
    indices = range(len(txs))

    def f(tx, index):
        return parse_tx(tx, block_attrs, index)

    return list(map(f, txs, indices))


def parse_tx(s: str, block_attrs: BlockAttributes, index: int) -> Dict:
    # Split inputs/outputs
    inputs, outputs = re.split(r">+", s)

    # Parse boxes
    input_boxes = re.findall(r"(\w{4}-\w{4}) (\d+) (\(.*?\))?", inputs)
    data_input_boxes = re.findall(r"[{] *(\w{4}-\w{4}) *[}]", inputs)
    output_boxes = re.findall(r"(\w{4}-\w{4}) (\d+) (\(.*?\))?", outputs)

    tx_id = f"tx-{block_attrs.id}{index+1}"
    return {
        "id": tx_id,
        "inputs": [{"boxId": bx[0] for bx in input_boxes}] if input_boxes else [],
        "dataInputs": [{"boxId": bx for bx in data_input_boxes}]
        if data_input_boxes
        else [],
        "outputs": [
            {
                "boxId": bx[0],
                "value": int(bx[1]),
                "ergoTree": AC.boxid2box(bx[0]).ergo_tree,
                "assets": parse_assets(bx[2]),
                "creationHeight": block_attrs.height,
                "additionalRegisters": {},
                "transactionId": tx_id,
                "index": index,
            }
            for index, bx in enumerate(output_boxes)
        ],
        "size": 344,
    }


def parse_assets(s: str) -> List[Dict]:
    m = re.findall(r"([\w-]+):\s*(\d+)", s)
    assets = [{"tokenId": t, "amount": int(a)} for t, a in m]
    return assets


if __name__ == "__main__":
    desc = """
        // comments
        block-a
        pub1-box1  100 (tokenx: 20, tokeny: 3)
        con1-box1  100 // inline comment
        >
        cex1-box1   10
        pub1-box2  190 (tokenx: 20, tokeny: 1)

        -- // Multiple txs are separated by 2 dashes
        pub1-box2 190
        >
        pub2-box1 189
        fees-box1   1

        // This block is not part of main chain
        block-x
        pub2-box1 189
        >
        pub3-box1 189

        // Child of block-a
        block-b-a
        pub2-box1 189
        >
        pub4-box1 189

        block-c
        pub4-box1 189
        {pub3-box1}
        >
        pub5-box1 189
    """
    bs = parse(desc, 10)

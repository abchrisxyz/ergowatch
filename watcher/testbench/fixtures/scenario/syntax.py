"""
Syntax describing fictive blocks to be used in test cases.

Comments start with `//`.
Blocks start with `block-<id>`.
Default parent of a block if previous block.
Block parent can be set explicitly: block-<id>-<parent_id>
Tx id derived from index and block id.
Txs separated by `--`.
Tx inputs and ouputs separated by `>`.
Tokens between (), registers between [].
Data-inputs enclosed in {curly brackets}.

Example:
    // comments
    block-a
    
    pub1-box1  100 (tokenx: 20, tokeny: 3)
    con1-box1  100
    >
    cex1-box1   10
    pub1-box2  190 (token: 20)
    --
    pub1-box2 190
    {cex1-box1} // data-input
    >
    pub2-box1 189
    fees-box1   1

    // This block is not part of main chain
    block-x
    ...

    block-b-a
    ...

    block-c
"""
import copy
import re
from typing import List, Dict, Tuple
from collections import namedtuple
from black import out

import sigpy
from fixtures.scenario.addresses import AddressCatalogue as AC
from fixtures.scenario.genesis import GENESIS_BOX, GENESIS_ID


BlockAttributes = namedtuple(
    "BlockAttributes", ["id", "header_id", "timestamp", "height", "parent_id"]
)

# Default timestamp increment (100 seconds)
DT = 100_000


class Digest32Generator:
    """
    Generates 255 unique Digest32 strings, in order.
    """

    def __init__(self, template: str):
        """
        Create a new generator.

        `template` should be 60 chars long, first 2 and last 2 will get added by generator.
        """
        self._counter = 0
        self._template = "dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4"
        assert len(template) == 60
        self._template = template

    def new(self):
        self._counter += 1
        assert self._counter <= 255
        return f"{self._counter:0{2}x}{self._template}{self._counter:0{2}x}"


def parse(desc: str, start_height: int, start_ts: int):
    """
    Converts `desc` into block data and lookup structure.

    The block data can be passed to the mock api.
    The lookup converts dummy id's to actual Ergo id.
    """
    block_descs = split_blocks(desc)
    attrs = derive_block_attributes(block_descs, start_height, start_ts)
    blocks = list(map(parse_block, block_descs, attrs))
    blocks, tx_map = generate_tx_ids(blocks)
    blocks, box_map = generate_box_ids(blocks)

    return blocks, {**tx_map, **box_map}


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
    """
    Extract block label and optional parent label.

    "block-a"   --> "a", None
    "block-b-a" --> "a", "b"
    """
    return re.match(r"^block-([\w]+)-?([\w]+)?", s).groups()


def derive_block_attributes(
    blocks: List[str],
    start_height: int,
    start_ts: int,
) -> List[BlockAttributes]:
    """
    Derive block attributes based on start height and parent-child relationships.
    """

    def label2id(label):
        return f"block-{label}"

    lbl, parent_lbl = parse_block_id(blocks[0])
    assert parent_lbl is None
    id2h = {label2id(lbl): start_height}
    t0 = start_ts
    dt = DT
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
    output_boxes = re.findall(r"(\w{4}-\w{4}) (\d+)(\W+\(.*?\))?(\W+\[.*?\])?", outputs)
    tx_id = f"tx-{block_attrs.id}{index+1}"
    return {
        "id": tx_id,
        "inputs": [{"boxId": bx[0]} for bx in input_boxes] if input_boxes else [],
        "dataInputs": [{"boxId": bx} for bx in data_input_boxes]
        if data_input_boxes
        else [],
        "outputs": [
            {
                "boxId": bx[0],
                "value": int(bx[1]),
                "ergoTree": AC.boxid2box(bx[0]).ergo_tree,
                "assets": parse_assets(bx[2]),
                "creationHeight": block_attrs.height,
                "additionalRegisters": parse_additional_registers(bx[3]),
                "transactionId": tx_id,
                "index": index,
            }
            for index, bx in enumerate(output_boxes)
        ],
        "size": 344,
    }


def parse_assets(s: str) -> List[Dict]:
    m = re.findall(r"([\w-]+):\s*(\d+)", s)
    return [{"tokenId": t, "amount": int(a)} for t, a in m]


def parse_additional_registers(s: str) -> Dict:
    m = re.findall(r"([\w-]+)", s)
    return {f"R{i+4}": value for i, value in enumerate(m)}


def generate_tx_ids(blocks: Dict) -> Tuple[Dict, Dict]:
    """
    Replaces dummy tx id's with valid Digest32 strings.

    Returns modified blocks and a dummy-to-Digest32 map.
    """
    dg = Digest32Generator("a" * 60)

    # Map dummy id's to ergo id's
    dummy_tx_ids = [
        tx["id"]
        for block in blocks
        for tx in block["blockTransactions"]["transactions"]
    ]
    ergo_tx_ids = [dg.new() for _ in set(dummy_tx_ids)]
    tx_map = {dummy: ergo for dummy, ergo in zip(dummy_tx_ids, ergo_tx_ids)}

    # Replace dummy id's by ergo id's
    for block in blocks:
        for tx in block["blockTransactions"]["transactions"]:
            tx["id"] = tx_map[tx["id"]]
            for output in tx["outputs"]:
                output["transactionId"] = tx_map[output["transactionId"]]

    return blocks, tx_map


def generate_box_ids(blocks: Dict) -> Tuple[Dict, Dict]:
    """
    Replace dummy box id's with actual box id's.

    Returns modified blocks and a dummy-to-actual box id map.
    """
    # A Digest32 generator for pre-existing inputs
    input_id_generator = Digest32Generator("b" * 60)

    # A Digest32 generator for pre-existing tokens
    token_id_generator = Digest32Generator("c" * 60)

    # A dict mapping dummy box id's to sigpy generated ones
    id_map = {}

    # Pre-fill real box id of genesis box
    id_map["base-box1"] = GENESIS_BOX["boxId"]

    # Replace dummy id's by valid id's
    for block in blocks:
        for tx in block["blockTransactions"]["transactions"]:
            # Inputs and data-inputs
            for collection in (tx["inputs"], tx["dataInputs"]):
                for input in collection:
                    if input["boxId"] not in id_map:
                        id_map[input["boxId"]] = input_id_generator.new()
                    input["boxId"] = id_map[input["boxId"]]
            # Outputs tokens
            for output in tx["outputs"]:
                # Handle token id's first
                for token in output["assets"]:
                    if token["tokenId"] not in id_map:
                        id_map[token["tokenId"]] = token_id_generator.new()
                    token["tokenId"] = id_map[token["tokenId"]]
                # then calculate output box ids
                assert output["boxId"] not in id_map
                box_id = calculate_box_id(output)
                id_map[output["boxId"]] = box_id
                output["boxId"] = box_id

    return blocks, id_map


def calculate_box_id(output: Dict) -> str:
    """
    Returns actual Ergo box_id matching data in `output`.
    """
    box_candidate_data = copy.deepcopy(output)
    del box_candidate_data["transactionId"]
    del box_candidate_data["index"]
    serialized_candidate = str(box_candidate_data).replace("'", '"')
    return sigpy.calc_box_id(
        serialized_candidate,
        output["transactionId"],
        output["index"],
    )


if __name__ == "__main__":
    desc = """
        // comments
        block-a
            pub1-box1  100 (tokenx: 20, tokeny: 3)
            con1-box1  100 // inline comment
            >
            cex1-box1   10
            pub1-box2  190 (tokenx: 20, tokeny: 1) [0400, 0322]

            -- // Multiple txs are separated by 2 dashes
            pub1-box2 190
            >
            pub2-box1 189 [05a4c3edd9998866]
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
            // output with additional registers
            pub4-box1 189 [
                0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8,
                0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8,
                05a4c3edd9998877
            ]

        block-c 
            pub4-box1 189
            {pub3-box1} // data-input 
            >
            pub5-box1 189
    """
    blocks, lookup = parse(desc, 599_000, 12345600000)

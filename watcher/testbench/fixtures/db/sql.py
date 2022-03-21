"""
Test databases will need to contain some data to satisfy foreign keys
when actual test data gets processed by watcher.

Given a list of test blocks processed to be processed by Watcher:
DB should contain a header that is a parent of first test block.
DB should contain outputs for satisfy any data-inputs used in, but ont created by, any of the blocks.
DB should contain tokens for any assets references by, but not minted in, any of the blocks.
DB should contain outputs for any tokens present at bootstrap.
DB should contain headers and txs references by any outputs present at bootstrap.
"""

from textwrap import dedent
from dataclasses import dataclass
from typing import List, Dict

from fixtures.addresses import AddressCatalogue as AC

BOOTSTRAP_TX_ID = "bootstrap-tx"
DEFAULT_BOX_VALUE = 1_000
DEFAULT_TOKEN_EMISSION_AMOUNT = 5_000


@dataclass
class Header:
    height: int
    id: str
    parent_id: str
    timestamp: int


@dataclass
class Transaction:
    id: int
    header_id: str
    height: int
    index: int


@dataclass
class Output:
    box_id: int
    address: str
    index: int
    header_id: str
    creation_height: int
    tx_id: str = BOOTSTRAP_TX_ID
    value: int = DEFAULT_BOX_VALUE


@dataclass
class Token:
    id: str
    box_id: str
    emission_amount: int = DEFAULT_TOKEN_EMISSION_AMOUNT
    name: str = "name"
    description: str = "description"
    decimals: int = 0
    standard: str = "dummy-std"


def generate_bootstrap_sql(blocks: List[Dict]) -> str:
    """
    Generate sql statements to prepare a db to accept listed blocks.

    *blocks*: list of test blocks to be processed by Watcher

    DB should contain a header that is a parent of first test block.
    DB should contain outputs for satisfy any data-inputs used in, but ont created by, any of the blocks.
    DB should contain tokens for any assets references by, but not minted in, any of the blocks.
    DB should contain outputs for any tokens present at bootstrap.
    DB should contain headers and txs references by any outputs present at bootstrap.

    Inserts 1 block, ensures all foreign keys are satisfied.
    Height and header_id are derived from blocks[0].
    """
    if blocks[0]["header"]["height"] == 1:
        raise ValueError("Test DB should be empty when simulating start from 1st block")
    header = extract_existing_header(blocks)
    # A single tx that is supposed to have produced any outputs, tokens, etc.
    # tx = Transaction(id="1" * 64, header_id=header.id, height=header.height, index=0)
    tx = extract_existing_transaction(blocks)
    outputs = extract_existing_outputs(blocks)
    tokens = extract_existing_tokens(blocks)

    sql = ""
    # Core tables
    sql += format_header_sql(header)
    sql += format_transaction_sql(tx)
    for box in outputs:
        sql += format_output_sql(box)
    for token in tokens:
        sql += format_token_sql(token)

    # Other schemas
    sql += generate_bootstrap_sql_usp(outputs)
    sql += generate_bootstrap_sql_bal_erg(header, outputs)

    return sql


def generate_bootstrap_sql_usp(outputs: List[Output]) -> str:
    """
    Bootstrap sql for usp schema (unspent boxes)
    """
    qry = "insert into usp.boxes (box_id) values ('{}');"
    return "\n".join([qry.format(box.box_id) for box in outputs])


def generate_bootstrap_sql_bal_erg(header: Header, outputs: List[Output]) -> str:
    """
    Bootstrap sql for erg balance tables

    Watcher checks latest height in bal.erg_diffs to determine if bootstrapping
    is needed and if so, from which height. Here, we set it to same height as core tables
    meaning no bootstrap is needed.
    """
    qry_diffs = dedent(
        """
        insert into bal.erg_diffs (address, height, tx_id, value)
        values ('{}', {}, '{}', {});\n
    """
    )

    qry_bal = dedent(
        """
        insert into bal.erg(address, value)
        values ('{}', {});\n
    """
    )
    return "".join(
        [
            qry_diffs.format(box.address, header.height, box.tx_id, box.value)
            + qry_bal.format(box.address, box.value)
            for box in outputs
        ]
    )


def extract_existing_header(blocks: List[Dict]) -> Header:
    # Header of first test block
    h = blocks[0]["header"]
    return Header(
        # Height prior to first test block
        height=h["height"] - 1,
        # Header id is parent id of first test block header
        id=h["parentId"],
        # Dummy parent id
        parent_id="bootstrap-parent-header-id",
        # Timestamp is 100 seconds less than first test block
        timestamp=h["timestamp"] - 100_000,
    )


def extract_existing_transaction(blocks: List[Dict]) -> Header:
    """A single tx that is supposed to have produced any outputs, tokens, etc."""
    header = extract_existing_header(blocks)
    return Transaction(
        id=BOOTSTRAP_TX_ID, header_id=header.id, height=header.height, index=0
    )


def extract_existing_outputs(blocks: List[Dict]) -> List[Output]:
    """
    Any boxes referenced in transaction data-(inputs) and tokens
    """
    header = extract_existing_header(blocks)
    created_outputs = set()
    outputs = []
    index = 0
    for block in blocks:
        for tx in block["blockTransactions"]["transactions"]:
            for box in tx["inputs"]:
                if box["boxId"] not in created_outputs:
                    outputs.append(
                        Output(
                            box_id=box["boxId"],
                            address=AC.boxid2addr(box["boxId"]),
                            header_id=header.id,
                            creation_height=header.height,
                            index=index,
                        )
                    )
                    index += 1
            for box in tx["dataInputs"]:
                if box["boxId"] not in created_outputs:
                    outputs.append(
                        Output(
                            box_id=box["boxId"],
                            address="dummy-data-input-box-address",
                            header_id=header.id,
                            creation_height=header.height,
                            index=index,
                        )
                    )
                    index = +1
            for output in tx["outputs"]:
                created_outputs.add(output["boxId"])

    # Add a dummy output for each token, these wont be spent,
    # they're just there to satisfy the db constraints.
    tokens = extract_existing_tokens(blocks)
    for itok, token in enumerate(tokens):
        index = len(outputs)
        outputs.append(
            Output(
                box_id=token.box_id,
                address=f"dummy-token-minting-address",
                header_id=header.id,
                creation_height=header.height,
                index=index,
            )
        )
    return outputs


def extract_existing_tokens(blocks: List[Dict]) -> List[Token]:
    """
    Returns tokens that the db should contain to satisfy asset FKs
    """
    tokens = []
    minted_tokens = set()
    for block in blocks:
        for tx in block["blockTransactions"]["transactions"]:
            for output in tx["outputs"]:
                for asset in output["assets"]:
                    token_id = asset["tokenId"]
                    if token_id == tx["inputs"][0]["boxId"]:
                        minted_tokens.add(token_id)
                    elif token_id not in minted_tokens:
                        box_id = f"dummy-token-box-id-{len(tokens)+1}"
                        tokens.append(
                            Token(
                                id=token_id,
                                box_id=box_id,
                            )
                        )
    return tokens


def format_header_sql(h: Header):
    return dedent(
        f"""
        insert into core.headers (height, id, parent_id, timestamp)
        values (
            {h.height},
            '{h.id}',
            '{h.parent_id}',
            {h.timestamp}
        );
    """
    )


def format_transaction_sql(tx: Transaction):
    return dedent(
        f"""
        insert into core.transactions (id, header_id, height, index)
        values (
            '{tx.id}',
            '{tx.header_id}',
            {tx.height},
            {tx.index}
        );
    """
    )


def format_output_sql(box: Output):
    return dedent(
        f"""
        insert into core.outputs(box_id, tx_id, header_id, creation_height, address, index, value)
        values (
            '{box.box_id}',
            '{box.tx_id}',
            '{box.header_id}',
            {box.creation_height},
            '{box.address}',
            {box.index},
            {box.value}
        );
    """
    )


def format_token_sql(t: Token):
    return dedent(
        f"""
        insert into core.tokens (id, box_id, emission_amount, name, description, decimals, standard)
        values (
            '{t.id}',
            '{t.box_id}',
            {t.emission_amount},
            '{t.name}',
            '{t.description}',
            {t.decimals},
            '{t.standard}'
        );
    """
    )

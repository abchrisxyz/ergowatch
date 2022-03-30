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
from fixtures.registers import RegisterCatalogue as RC

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
class Input:
    box_id: int
    tx_id: str
    header_id: str
    index: int


@dataclass
class Token:
    id: str
    box_id: str
    emission_amount: int = DEFAULT_TOKEN_EMISSION_AMOUNT
    name: str = "name"
    description: str = "description"
    decimals: int = 0
    standard: str = "dummy-std"


@dataclass
class BoxAsset:
    token_id: str
    box_id: str
    amount: int


@dataclass
class Register:
    id: int
    box_id: str
    value_type: str
    serialized_value: str
    rendered_value: str


def generate_rev1_sql(blocks: List[Dict]) -> str:
    """
    Generate sql statements to fill a db as it would be by v0.1.

    *blocks*: list of test blocks to be processed by Watcher

    Use to test migrations.
    """
    if blocks[0]["header"]["height"] == 1:
        raise ValueError("Test DB should be empty when simulating start from 1st block")
    headers = extract_headers(blocks)
    transactions = extract_transactions(blocks)
    outputs = extract_outputs(blocks)
    inputs = extract_inputs(blocks)
    tokens = extract_tokens(blocks)
    assets = extract_assets(blocks)
    registers = extract_registers(blocks)

    sql = ""
    # Core tables
    for header in headers:
        sql += format_header_sql(header)
    for tx in transactions:
        sql += format_transaction_sql(tx)
    for box in outputs:
        sql += format_output_sql(box)
    for box in inputs:
        sql += format_input_sql(box)
    for token in tokens:
        sql += format_token_sql(token)
    for asset in assets:
        sql += format_asset_sql(asset)
    for register in registers:
        sql += format_register_sql(register)

    # Unspent
    sql += """
        insert into usp.boxes (box_id)
        select op.box_id
        from core.outputs op
        left join core.inputs ip on ip.box_id = op.box_id
        where ip.box_id is null;
        """

    # ERG balance diffs
    sql += """
        with transactions as (	
            select height, id
            from core.transactions
        ), inputs as (
            select tx.height
                , tx.id as tx_id
                , op.address
                , sum(op.value) as value
            from transactions tx
            join core.inputs ip on ip.tx_id = tx.id
            join core.outputs op on op.box_id = ip.box_id
            group by 1, 2, 3
        ), outputs as (
            select tx.height
                , tx.id as tx_id
                , op.address
                , sum(op.value) as value
            from transactions tx
            join core.outputs op on op.tx_id = tx.id
            group by 1, 2, 3
        )
        insert into bal.erg_diffs (address, height, tx_id, value)
        select coalesce(i.address, o.address) as address
            , coalesce(i.height, o.height) as height
            , coalesce(i.tx_id, o.tx_id) as tx_id
            , sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) as value
        from inputs i
        full outer join outputs o
            on o.address = i.address
            and o.tx_id = i.tx_id
        group by 1, 2, 3 having sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) <> 0
        order by 2, 3;
    """

    # ERG balances
    sql += """
        insert into bal.erg(address, value)
        select address,
            sum(value)
        from bal.erg_diffs
        group by 1 having sum(value) <> 0
        order by 1;
    """

    # Token balance diffs
    sql += """
        with inputs as (
            with transactions as (	
                select height, id
                from core.transactions
            )
            select tx.height
                , tx.id as tx_id
                , op.address
                , ba.token_id
                , sum(ba.amount) as value
            from transactions tx
            join core.inputs ip on ip.tx_id = tx.id
            join core.outputs op on op.box_id = ip.box_id
            join core.box_assets ba on ba.box_id = ip.box_id
            group by 1, 2, 3, 4
        ), outputs as (
            with transactions as (	
                select height, id
                from core.transactions
            )
            select tx.height
                , tx.id as tx_id
                , op.address
                , ba.token_id
                , sum(ba.amount) as value
            from transactions tx
            join core.outputs op on op.tx_id = tx.id
            join core.box_assets ba on ba.box_id = op.box_id
            group by 1, 2, 3, 4
        )
        insert into bal.tokens_diffs (address, token_id, height, tx_id, value)
        select coalesce(i.address, o.address) as address
            , coalesce(i.token_id, o.token_id ) as token_id
            , coalesce(i.height, o.height) as height
            , coalesce(i.tx_id, o.tx_id) as tx_id
            , sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) as value
        from inputs i
        full outer join outputs o
            on o.address = i.address
            and o.tx_id = i.tx_id
            and o.token_id = i.token_id
        group by 1, 2, 3, 4 having sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) <> 0;
    """

    # Token balances
    sql += """
        insert into bal.tokens(address, token_id, value)
        select address,
            token_id,
            sum(value)
        from bal.tokens_diffs
        group by 1, 2 having sum(value) <> 0
        order by 1, 2;
    """

    return sql


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
    # TODO: generate_bootstrap_sql_bal_tokens (works fine for now because no case has tokens initially)
    sql += generate_bootstrap_sql_mtr(header, outputs)

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


def generate_bootstrap_sql_mtr(header: Header, outputs: List[Output]) -> str:
    """
    Bootstrap sql for mtr tables.
    """
    # All outputs are still unspent
    qry_utxos = dedent(
        f"""
        insert into mtr.utxos(height, value)
        values ({header.height}, {len(outputs)});\n
    """
    )
    return "".join(
        [
            qry_utxos,
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


def extract_headers(blocks: List[Dict]) -> List[Header]:
    return [extract_existing_header(blocks),] + [
        Header(
            height=b["header"]["height"],
            id=b["header"]["id"],
            parent_id=b["header"]["parentId"],
            timestamp=b["header"]["timestamp"],
        )
        for b in blocks
    ]


def extract_existing_transaction(blocks: List[Dict]) -> Header:
    """A single tx that is supposed to have produced any outputs, tokens, etc."""
    header = extract_existing_header(blocks)
    return Transaction(
        id=BOOTSTRAP_TX_ID, header_id=header.id, height=header.height, index=0
    )


def extract_transactions(blocks: List[Dict]) -> Header:
    return [extract_existing_transaction(blocks),] + [
        Transaction(
            id=tx["id"],
            header_id=b["header"]["id"],
            height=b["header"]["height"],
            index=idx,
        )
        for b in blocks
        for idx, tx in enumerate(b["blockTransactions"]["transactions"])
    ]


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


def extract_outputs(blocks: List[Dict]) -> List[Output]:
    """
    All output boxes
    """
    return extract_existing_outputs(blocks) + [
        Output(
            box_id=box["boxId"],
            address=AC.boxid2addr(box["boxId"]),
            index=idx,
            header_id=b["header"]["id"],
            creation_height=box["creationHeight"],
            tx_id=tx["id"],
            value=box["value"],
        )
        for b in blocks
        for tx in b["blockTransactions"]["transactions"]
        for idx, box in enumerate(tx["outputs"])
    ]


def extract_inputs(blocks: List[Dict]) -> List[Input]:
    """
    All input boxes
    """
    return [
        Input(
            box_id=box["boxId"],
            header_id=b["header"]["id"],
            tx_id=tx["id"],
            index=idx,
        )
        for b in blocks
        for tx in b["blockTransactions"]["transactions"]
        for idx, box in enumerate(tx["inputs"])
    ]


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


def extract_tokens(blocks: List[Dict]) -> List[Token]:
    """
    All tokens
    """
    return extract_existing_tokens(blocks) + [
        Token(
            id=tk["tokenId"],
            box_id=op["boxId"],
            emission_amount=tk["amount"],
            # TODO: should be parsed from registers
            decimals=0,
        )
        for b in blocks
        for tx in b["blockTransactions"]["transactions"]
        for op in tx["outputs"]
        for tk in op["assets"]
        if tk["tokenId"] == tx["inputs"][0]["boxId"]
    ]


def extract_assets(blocks: List[Dict]) -> List[BoxAsset]:
    """
    All box assets
    """
    return extract_existing_tokens(blocks) + [
        BoxAsset(
            token_id=tk["tokenId"],
            box_id=op["boxId"],
            amount=tk["amount"],
        )
        for b in blocks
        for tx in b["blockTransactions"]["transactions"]
        for op in tx["outputs"]
        for tk in op["assets"]
    ]


def extract_registers(blocks: List[Dict]) -> List[Register]:
    """
    All box assets
    """
    return [
        Register(
            id=int(rid[1]),
            box_id=op["boxId"],
            value_type=RC.from_raw(raw).type,
            serialized_value=raw,
            rendered_value=RC.from_raw(raw).rendered,
        )
        for b in blocks
        for tx in b["blockTransactions"]["transactions"]
        for op in tx["outputs"]
        for (rid, raw) in op["additionalRegisters"].items()
    ]


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


def format_input_sql(box: Input):
    return dedent(
        f"""
        insert into core.inputs(box_id, tx_id, header_id, index)
        values (
            '{box.box_id}',
            '{box.tx_id}',
            '{box.header_id}',
            {box.index}
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


def format_asset_sql(a: BoxAsset):
    return dedent(
        f"""
        insert into core.box_assets (token_id, box_id, amount)
        values (
            '{a.token_id}',
            '{a.box_id}',
            {a.amount}
        );
    """
    )


def format_register_sql(r: Register):
    return dedent(
        f"""
        insert into core.box_registers (id, box_id, value_type, serialized_value, rendered_value)
        values (
            '{r.id}',
            '{r.box_id}',
            '{r.value_type}',
            '{r.serialized_value}',
            '{r.rendered_value}'
        );
    """
    )

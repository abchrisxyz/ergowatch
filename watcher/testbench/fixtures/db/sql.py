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
from typing import List
from typing import Dict
from typing import Tuple

from fixtures.scenario.addresses import CEX_BOXES
from fixtures.registers import RegisterCatalogue as RC
from fixtures.scenario import Scenario

BOOTSTRAP_TX_ID = "bootstrap-tx"
DEFAULT_BOX_VALUE = 1_000
DEFAULT_TOKEN_EMISSION_AMOUNT = 5_000
DEFAULT_BOX_SIZE = 123
DEFAULT_VOTE = 0


@dataclass
class Header:
    height: int
    id: str
    parent_id: str
    timestamp: int
    difficulty: int
    votes: Tuple[int, int, int]


@dataclass
class Transaction:
    id: int
    header_id: str
    height: int
    index: int


@dataclass
class Address:
    id: int
    address: str
    spot_height: int


@dataclass
class Output:
    box_id: int
    address: str
    address_id: int
    index: int
    header_id: str
    creation_height: int
    tx_id: str = BOOTSTRAP_TX_ID
    value: int = DEFAULT_BOX_VALUE
    size: int = DEFAULT_BOX_SIZE


@dataclass
class Input:
    box_id: int
    tx_id: str
    header_id: str
    index: int


@dataclass
class DataInput:
    box_id: int
    tx_id: str
    header_id: str
    index: int


@dataclass
class Token:
    id: str
    box_id: str
    emission_amount: int = DEFAULT_TOKEN_EMISSION_AMOUNT
    name: str = None
    description: str = None
    decimals: int = None
    standard: str = None


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


@dataclass
class SystemParameters:
    height: int
    storage_fee: int = None
    min_box_value: int = None
    max_block_size: int = None
    max_cost: int = None
    token_access_cost: int = None
    tx_input_cost: int = None
    tx_data_input_cost: int = None
    tx_output_cost: int = None
    block_version: int = None


@dataclass
class ExtensionField:
    height: int
    key: str = None
    value: str = None


def generate_rev0_sql(scenario: Scenario) -> str:
    """
    Generate sql statements to fill a db as it would be by v0.4.

    *scenario*: Scenario for which to bootstrap db

    Use to test migrations.
    """
    heights = [b["header"]["height"] for b in scenario.blocks]
    if len(heights) != len(set(heights)):
        raise ValueError(
            "generate_rev0_sql does not handle forks. Ensure 1 block per height only."
        )
    if heights[0] == 1:
        raise ValueError("Test DB should be empty when simulating start from 1st block")
    headers = extract_headers(scenario.blocks)
    transactions = extract_transactions(scenario.blocks)
    outputs = extract_outputs(scenario)
    inputs = extract_inputs(scenario.blocks)
    data_inputs = extract_data_inputs(scenario.blocks)
    tokens = extract_tokens(scenario.blocks)
    assets = extract_assets(scenario.blocks)
    registers = extract_registers(scenario.blocks)
    sys_params = extract_system_parameters(scenario.blocks)
    unhandled_ext_fields = extract_unhandled_extension_fields(scenario.blocks)
    addresses = extract_addresses(outputs)

    sql = ""
    # Core tables
    for header in headers:
        sql += format_header_sql(header)
    for tx in transactions:
        sql += format_transaction_sql(tx)
    for address in addresses:
        sql += format_address_sql(address)
    for box in outputs:
        sql += format_output_sql(box)
    for box in inputs:
        sql += format_input_sql(box)
    for box in data_inputs:
        sql += format_data_input_sql(box)
    for token in tokens:
        sql += format_token_sql(token)
    for asset in assets:
        sql += format_asset_sql(asset)
    for register in registers:
        sql += format_register_sql(register)
    for sp in sys_params:
        sql += format_system_parameters(sp)
    for f in unhandled_ext_fields:
        sql += format_unhandled_extension_fields(f)

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
                , op.address_id
                , sum(op.value) as value
            from transactions tx
            join core.inputs ip on ip.tx_id = tx.id
            join core.outputs op on op.box_id = ip.box_id
            group by 1, 2, 3
        ), outputs as (
            select tx.height
                , tx.id as tx_id
                , op.address_id
                , sum(op.value) as value
            from transactions tx
            join core.outputs op on op.tx_id = tx.id
            group by 1, 2, 3
        )
        insert into bal.erg_diffs (address_id, height, tx_id, value)
        select coalesce(i.address_id, o.address_id) as address_id
            , coalesce(i.height, o.height) as height
            , coalesce(i.tx_id, o.tx_id) as tx_id
            , sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) as value
        from inputs i
        full outer join outputs o
            on o.address_id = i.address_id
            and o.tx_id = i.tx_id
        group by 1, 2, 3 having sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) <> 0
        order by 2, 3;
    """

    # ERG balances
    sql += """
        insert into bal.erg(address_id, value)
        select address_id,
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
                , op.address_id
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
                , op.address_id
                , ba.token_id
                , sum(ba.amount) as value
            from transactions tx
            join core.outputs op on op.tx_id = tx.id
            join core.box_assets ba on ba.box_id = op.box_id
            group by 1, 2, 3, 4
        )
        insert into bal.tokens_diffs (address_id, token_id, height, tx_id, value)
        select coalesce(i.address_id, o.address_id) as address
            , coalesce(i.token_id, o.token_id ) as token_id
            , coalesce(i.height, o.height) as height
            , coalesce(i.tx_id, o.tx_id) as tx_id
            , sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) as value
        from inputs i
        full outer join outputs o
            on o.address_id = i.address_id
            and o.tx_id = i.tx_id
            and o.token_id = i.token_id
        group by 1, 2, 3, 4 having sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) <> 0;
    """

    # Token balances
    sql += """
        insert into bal.tokens(address_id, token_id, value)
        select address_id,
            token_id,
            sum(value)
        from bal.tokens_diffs
        group by 1, 2 having sum(value) <> 0
        order by 1, 2;
    """

    # Bal bootstrap flag
    sql += """
        update bal._log set bootstrapped = TRUE;
    """

    return sql


def generate_bootstrap_sql(scenario: Scenario) -> str:
    """
    Generate sql statements to prepare a db to accept listed blocks.

    `blocks`: list of test blocks to be processed by Watcher
    `id_map`: maps dummy box id's to their Digest32 representation

    DB should contain a header that is a parent of first test block.
    DB should contain outputs for satisfy any data-inputs used in, but ont created by, any of the blocks.
    DB should contain tokens for any assets references by, but not minted in, any of the blocks.
    DB should contain outputs for any tokens present at bootstrap.
    DB should contain headers and txs references by any outputs present at bootstrap.

    Inserts 1 block, ensures all foreign keys are satisfied.
    Height and header_id are derived from blocks[0].
    """
    if scenario.parent_height == 0:
        raise ValueError("Test DB should be empty when simulating start from 1st block")
    header = extract_existing_header(scenario.blocks)
    # A single tx that is supposed to have produced any outputs, tokens, etc.
    # tx = Transaction(id="1" * 64, header_id=header.id, height=header.height, index=0)
    tx = extract_existing_transaction(scenario.blocks)
    outputs = extract_existing_outputs(scenario)
    tokens = extract_existing_tokens(scenario.blocks)
    addresses = extract_addresses(outputs)

    sql = ""
    # Core tables
    sql += format_header_sql(header)
    sql += format_transaction_sql(tx)
    for address in addresses:
        sql += format_address_sql(address)
    for box in outputs:
        sql += format_output_sql(box)
    for token in tokens:
        sql += format_token_sql(token)

    # Other schemas
    sql += generate_bootstrap_sql_usp(outputs)
    sql += generate_bootstrap_sql_bal_erg(header, outputs)
    # TODO: generate_bootstrap_sql_bal_tokens (works fine for now because no case has tokens initially)
    # Bal bootstrap flag
    sql += """
        update adr._log set bootstrapped = TRUE;
    """
    sql += generate_bootstrap_sql_cex(header, outputs)
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

    Watcher checks latest height in adr.erg_diffs to determine if bootstrapping
    is needed and if so, from which height. Here, we set it to same height as core tables
    meaning no bootstrap is needed.
    """
    qry_diffs = dedent(
        """
        insert into adr.erg_diffs (address_id, height, tx_id, value)        values ('{}', {}, '{}', {});\n
    """
    )

    qry_adr = dedent(
        """
        insert into adr.erg(address_id, value)        values ('{}', {});\n
    """
    )
    return "".join(
        [
            qry_diffs.format(box.address_id, header.height, box.tx_id, box.value)
            + qry_adr.format(box.address_id, box.value)            for box in outputs
        ]
    )


def generate_bootstrap_sql_cex(header: Header, outputs: List[Output]) -> str:
    """
    Bootstrap sql for cex tables.
    """
    # Assuming no CEX addresses are involved yet,
    # so making sure they're not
    cex_addresses = set([box.address for box in CEX_BOXES])
    for op in outputs:
        assert op.address not in cex_addresses

    # Since no cex addresses are involved, no need fill cex.supply.

    # Mark existing block as processed
    qry_block_processing_log = dedent(
        f"""
        insert into cex.block_processing_log(header_id, height, invalidation_height, status)
        values ('{header.id}', {header.height}, null, 'processed');\n
    """
    )
    return "".join(
        [
            qry_block_processing_log,
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

    # Zero supply on cex's (because no cex addresses involved so far
    # see generate_bootstrap_sql_cex)
    qry_cex_supply = dedent(
        f"""
        insert into mtr.cex_supply(height, total, deposit)
        select height
            , 0 as total
            , 0 as deposit
        from core.headers
        order by 1;\n
    """
    )

    return "".join(
        [
            qry_utxos,
            qry_cex_supply,
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
        difficulty=h["difficulty"],
        votes=tuple([DEFAULT_VOTE, DEFAULT_VOTE, DEFAULT_VOTE]),
    )


def extract_headers(blocks: List[Dict]) -> List[Header]:
    def parse_votes(votes: str) -> Tuple[int, int, int]:
        return tuple([int(votes[i : i + 2], 16) for i in (0, 2, 4)])

    return [extract_existing_header(blocks),] + [
        Header(
            height=b["header"]["height"],
            id=b["header"]["id"],
            parent_id=b["header"]["parentId"],
            timestamp=b["header"]["timestamp"],
            difficulty=b["header"]["difficulty"],
            votes=parse_votes(b["header"]["votes"]),
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


def extract_existing_outputs(scenario: Scenario) -> List[Output]:
    """
    Any boxes referenced in transaction data-(inputs) and tokens.
    """
    header = extract_existing_header(scenario.blocks)
    created_outputs = set()
    outputs = []
    index = 0
    address_ids = {}
    last_address_id = 0
    for block in scenario.blocks:
        for tx in block["blockTransactions"]["transactions"]:
            for box in tx["inputs"]:
                if box["boxId"] not in created_outputs:
                    address = scenario.address(box["boxId"])
                    if address not in address_ids:
                        last_address_id += 1
                        address_ids[address] = last_address_id
                    output = Output(
                        box_id=box["boxId"],
                        address=address,
                        address_id=address_ids[address],
                        header_id=header.id,
                        creation_height=header.height,
                        index=index,
                    )
                    outputs.append(output)
                    index += 1
            for box in tx["dataInputs"]:
                if box["boxId"] not in created_outputs:
                    # address = "dummy-data-input-box-address"
                    address = scenario.address(box["boxId"])
                    if address not in address_ids:
                        last_address_id += 1
                        address_ids[address] = last_address_id
                    output = Output(
                        box_id=box["boxId"],
                        address=address,
                        address_id=address_ids[address],
                        header_id=header.id,
                        creation_height=header.height,
                        index=index,
                    )
                    outputs.append(output)
                    index = +1
            for output in tx["outputs"]:
                created_outputs.add(output["boxId"])

    # Add a dummy output for each token, these wont be spent,
    # they're just there to satisfy the db constraints.
    tokens = extract_existing_tokens(scenario.blocks)
    for token in tokens:
        index = len(outputs)
        address = "dummy-token-minting-address"
        if address not in address_ids:
            last_address_id += 1
            address_ids[address] = last_address_id
        output = Output(
            box_id=token.box_id,
            address=address,
            address_id=address_ids[address],
            header_id=header.id,
            creation_height=header.height,
            index=index,
        )
        outputs.append(output)
    return outputs


def extract_outputs(scenario: Scenario) -> List[Output]:
    """
    All output boxes.

    Returns list of outputs and address to address_id map.
    """
    outputs = extract_existing_outputs(scenario)
    address_ids = {b.address: b.address_id for b in outputs}
    last_address_id = max(address_ids.values())

    for b in scenario.blocks:
        for tx in b["blockTransactions"]["transactions"]:
            for idx, box in enumerate(tx["outputs"]):
                address = scenario.address(box["boxId"])
                if address not in address_ids:
                    last_address_id += 1
                    address_ids[address] = last_address_id
                output = Output(
                    box_id=box["boxId"],
                    address=address,
                    address_id=address_ids[address],
                    index=idx,
                    header_id=b["header"]["id"],
                    creation_height=box["creationHeight"],
                    tx_id=tx["id"],
                    value=box["value"],
                )
                outputs.append(output)
    return outputs


def extract_addresses(outputs: List[Output]) -> List[Address]:
    """
    Extract unique addresses from collection of outputs.
    """
    addresses = []
    known_ids = set()
    for box in outputs:
        if box.address_id in known_ids:
            continue
        addresses.append(
            Address(
                id=box.address_id,
                address=box.address,
                spot_height=box.creation_height,
            )
        )
        known_ids.add(box.address_id)
    return addresses


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


def extract_data_inputs(blocks: List[Dict]) -> List[DataInput]:
    """
    All data inputs
    """
    return [
        DataInput(
            box_id=box["boxId"],
            header_id=b["header"]["id"],
            tx_id=tx["id"],
            index=idx,
        )
        for b in blocks
        for tx in b["blockTransactions"]["transactions"]
        for idx, box in enumerate(tx["dataInputs"])
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
            emission_amount=tk["amount"]
            # name=None,
            # description=None,
            # TODO: should be parsed from registers
            # decimals=0,
            # standard="dummy-std",
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


def extract_system_parameters(blocks: List[Dict]) -> List[SystemParameters]:
    ps = []
    for block in blocks:
        p = SystemParameters(height=block["header"]["height"])

        for field in block["extension"]["fields"]:
            prefix = int(field[0][0:2], 16)
            if prefix != 0:
                continue
            key = int(field[0][2:4], 16)
            if key == 1:
                p.storage_fee = int(field[1], 16)
            elif key == 2:
                p.min_box_value = int(field[1], 16)
            elif key == 3:
                p.max_block_size = int(field[1], 16)
            elif key == 4:
                p.max_cost = int(field[1], 16)
            elif key == 5:
                p.token_access_cost = int(field[1], 16)
            elif key == 6:
                p.tx_input_cost = int(field[1], 16)
            elif key == 7:
                p.tx_data_input_cost = int(field[1], 16)
            elif key == 8:
                p.tx_output_cost = int(field[1], 16)
            elif key == 123:
                p.block_version = int(field[1], 16)
            elif key in (120, 121, 122, 124):
                continue
            else:
                raise ValueError(
                    f"Unknown system parameter extension field key ({key}) in field: {field}"
                )

        # Should ideally check all fields but assuming one these ones will always be set if others are.
        if p.storage_fee is not None or p.block_version is not None:
            ps.append(p)

    return ps


def extract_unhandled_extension_fields(blocks: List[Dict]) -> List[ExtensionField]:
    fields = []
    for block in blocks:
        for field in block["extension"]["fields"]:
            prefix = int(field[0][0:2], 16)
            if prefix == 1:
                continue
            if prefix == 0 and int(field[0][2:4], 16) in (1, 2, 3, 4, 5, 6, 7, 8, 123):
                continue
            fields.append(
                ExtensionField(
                    height=block["header"]["height"], key=field[0], value=field[1]
                )
            )
    return fields


def format_header_sql(h: Header):
    return dedent(
        f"""
        insert into core.headers (height, id, parent_id, timestamp, difficulty, vote1, vote2, vote3)
        values (
            {h.height},
            '{h.id}',
            '{h.parent_id}',
            {h.timestamp},
            {h.difficulty},
            {h.votes[0]},
            {h.votes[1]},
            {h.votes[2]}
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


def format_address_sql(a: List):
    return dedent(
        f"""
        insert into core.addresses (id, address, spot_height)
        values (
            '{a.id}',
            '{a.address}',
            '{a.spot_height}'
        );
    """
    )


def format_output_sql(box: Output):
    return dedent(
        f"""
        insert into core.outputs(box_id, tx_id, header_id, creation_height, address_id, index, value, size)
        values (
            '{box.box_id}',
            '{box.tx_id}',
            '{box.header_id}',
            {box.creation_height},
            {box.address_id},
            {box.index},
            {box.value},
            {box.size}
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


def format_data_input_sql(box: DataInput):
    return dedent(
        f"""
        insert into core.data_inputs(box_id, tx_id, header_id, index)
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
        insert into core.tokens (
            id,
            box_id,
            emission_amount,
            name,
            description,
            decimals,
            standard
        )
        values (
            '{t.id}',
            '{t.box_id}',
            {t.emission_amount},
            {'null' if t.name is None else f"'{t.name}'"},
            {'null' if t.name is None else f"'{t.description}'"},
            {'null' if t.decimals is None else t.decimals},
            {'null' if t.name is None else f"'{t.standard}'"}
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


def format_system_parameters(p: SystemParameters):
    def nullify(val):
        return "null" if val is None else val

    return dedent(
        f"""
        insert into core.system_parameters (
            height,
            storage_fee,
            min_box_value,
            max_block_size,
            max_cost,
            token_access_cost,
            tx_input_cost,
            tx_data_input_cost,
            tx_output_cost,
            block_version
        )
        values (
            {p.height},
            {nullify(p.storage_fee)},
            {nullify(p.min_box_value)},
            {nullify(p.max_block_size)},
            {nullify(p.max_cost)},
            {nullify(p.token_access_cost)},
            {nullify(p.tx_input_cost)},
            {nullify(p.tx_data_input_cost)},
            {nullify(p.tx_output_cost)},
            {nullify(p.block_version)}
        );
    """
    )


def format_unhandled_extension_fields(f: ExtensionField):
    assert f.key is not None
    assert f.value is not None
    return dedent(
        f"""
        insert into core.unhandled_extension_fields (
            height,
            key,
            value
        )
        values (
            {f.height},
            '{f.key}',
            '{f.value}'
        );
    """
    )

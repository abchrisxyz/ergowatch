from pathlib import Path
from typing import Dict
from typing import List

import psycopg as pg
from psycopg.sql import Identifier, SQL

from local import DB_HOST, DB_PORT, DB_USER, DB_PASS


SCHEMA_PATH = (
    Path(__file__).parent.parent.absolute() / Path("../db/schema.sql")
).absolute()

CONSTRAINTS_PATH = (
    Path(__file__).parent.parent.absolute() / Path("../db/constraints.sql")
).absolute()


def conn_str(dbname: str) -> str:
    """
    Return connection string for given db name.
    """
    return f"host={DB_HOST} port={DB_PORT} dbname={dbname} user={DB_USER} password={DB_PASS}"


class TestDB:
    def __init__(self, set_constraints=True):
        self._dbname: str = "ew_pytest"  # TODO: use random name
        with open(SCHEMA_PATH) as f:
            self._sql = f.read()
        if set_constraints:
            with open(CONSTRAINTS_PATH) as f:
                self._sql += f.read()

    def _create_db(self):
        with pg.connect(conn_str("postgres"), autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(SQL("create database {};").format(Identifier(self._dbname)))

    def _drop_db(self):
        with pg.connect(conn_str("postgres"), autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    SQL("drop database {} with (force);").format(
                        Identifier(self._dbname)
                    )
                )

    def _init_db(self):
        with pg.connect(conn_str(self._dbname)) as conn:
            with conn.cursor() as cur:
                cur.execute(self._sql)

    def __enter__(self) -> pg.Cursor:
        self._create_db()
        self._init_db()
        return self._dbname

    def __exit__(self, exception_type, exception_value, traceback):
        self._drop_db()
        pass


def generate_bootstrap_sql(blocks: List[Dict]) -> str:
    """
    Generate sql statements to prepare a db to accept listed blocks.

    Blocks should be dict objects as defined in fixtures.blocks.

    Inserts 1 block, ensures all foreign keys are satisfied.
    Height and header_id are derived from blocks[0].
    """
    height = blocks[0]["header"]["height"] - 1
    if height == 0:
        # Starting with genesis block, keep db empty
        return None

    header_id = blocks[0]["header"]["parentId"]
    timestamp = blocks[0]["header"]["timestamp"] - 120_000
    sql = f"""
        insert into core.headers (height, id, parent_id, timestamp)
        values (
            {height},
            '{header_id}',
            '0000000000000000000000000000000000000000000000000000000000000000',
            {timestamp}
        );
    """

    # Adding a single tx that is supposed to have produced any outputs, tokens, etc.
    tx_id = "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308"
    sql += f"""
        insert into core.transactions (id, header_id, height, index)
        values (
            '{tx_id}',
            '{header_id}',
            {height},
            0
        );
    """

    # Collect outputs and tokens to be present in bootstrapped db
    # Doesn't collect all spent outputs, only the ones to satisfy
    # FK on data inputs.
    created_outputs = set()
    existing_outputs = set()
    minted_tokens = set()
    existing_tokens = set()
    for block in blocks:
        for tx in block["blockTransactions"]["transactions"]:
            for output in tx["outputs"]:
                created_outputs.add(output["boxId"])
                for asset in output["assets"]:
                    token_id = asset["tokenId"]
                    if token_id == tx["inputs"][0]["boxId"]:
                        minted_tokens.add(token_id)
                    if token_id not in minted_tokens:
                        existing_tokens.add(token_id)
            for di in tx["dataInputs"]:
                if di["boxId"] not in created_outputs:
                    existing_outputs.add(di["boxId"])

    # First output to have a known box_id we can use should there be any tokens to add.
    first_output_box_id = (
        "eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f"
    )

    sql += f"""
        insert into core.outputs(box_id, tx_id, header_id, creation_height, address, index, value)
        values (
            '{first_output_box_id}',
            '{tx_id}',
            '{header_id}',
            {height},
            'dummy-address-0',
            0,
            93000000000000000
        );
        insert into bal.erg(address, value)
        values (
            'dummy-address-0',
            93000000000000000
        );
    """

    existing_outputs = list(existing_outputs)
    existing_outputs.sort()
    for index, box_id in enumerate(existing_outputs):
        sql += f"""
            insert into core.outputs(box_id, tx_id, header_id, creation_height, address, index, value)
            values (
                '{box_id}',
                '{tx_id}',
                '{header_id}',
                {height},
                -- Each box has a unique address, so we don't have to compute balances (see below)
                'dummy-address-{index+1}',
                {index + 1},
                67500000000
            );
            insert into bal.erg(address, value)
            values (
                'dummy-address-{index+1}',
                67500000000
            );
        """

    existing_tokens = list(existing_tokens)
    existing_tokens.sort()
    for token_id in existing_tokens:
        sql += f"""
        insert into core.tokens (id, box_id, emission_amount, name, description, decimals, standard)
        values (
            '{token_id}',
            '{first_output_box_id}',
            20,
            'name',
            'description',
            0,
            'EIP-004'
        );
    """

    return sql

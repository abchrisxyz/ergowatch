from pathlib import Path

import psycopg as pg
from psycopg.sql import Identifier, SQL

from local import DB_HOST, DB_PORT, DB_USER, DB_PASS


SCHEMA_PATH = (
    Path(__file__).parent.parent.absolute() / Path("../db/schema.sql")
).absolute()


def conn_str(dbname: str) -> str:
    """
    Return connection string for given db name.
    """
    return f"host={DB_HOST} port={DB_PORT} dbname={dbname} user={DB_USER} password={DB_PASS}"


class TestDB:
    def __init__(self):
        self._dbname: str = "test2"  # TODO: use random name
        with open(SCHEMA_PATH) as f:
            self._sql = f.read()

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


def bootstrap_sql():
    """
    Generates sql to bootstrap db to a valid state.

    All boxes spent in mock transactions must be included here.
    """
    header_id = "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1"
    height = 599_999
    sql = f"""
        insert into core.headers (height, id, parent_id, timestamp)
        values (
            {height},
            '{header_id}',
            '0000000000000000000000000000000000000000000000000000000000000000',
            1561978977137
        );
    """

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

    sql += f"""
        insert into core.outputs(box_id, tx_id, header_id, creation_height, address, index, value)
        values (
            '71bc9534d4a4fe8ff67698a5d0f29782836970635de8418da39fee1cd964fcbe',
            '{tx_id}',
            '{header_id}',
            {height},
            '2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU',
            0,
            93409065000000000
        ), (
            '45dc27302332bcb93604ae63c0a543894b38af31e6aebdb40291e3e8ecaef031',
            '{tx_id}',
            '{header_id}',
            {height},
            '88dhgzEuTXaVTz3coGyrAbJ7DNqH37vUMzpSe2vZaCEeBzA6K2nKTZ2JQJhEFgoWmrCQEQLyZNDYMby5',
            1,
            67500000000
        ),
        --
        -- dummy outputs, spent in block 600k
        --
        (
            'eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f',
            '{tx_id}',
            '{header_id}',
            {height},
            'dummy_address_1',
            1,
            67500000000
        ), (
            'c739a3294d592377a131840d491bd2b66c27f51ae2c62c66be7bb41b248f321e',
            '{tx_id}',
            '{header_id}',
            {height},
            'dummy_address_2',
            1,
            67500000000
        ), (
            '6ca2a9d63f2f08663c09d99126ec1be7b65ce2e8f34e283c4d5af78485b47c91',
            '{tx_id}',
            '{header_id}',
            {height},
            'dummy_address_3',
            1,
            67500000000
        ), (
            '98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8',
            '{tx_id}',
            '{header_id}',
            {height},
            'dummy_address_4',
            1,
            67500000000
        );
        """

    sql += f"""
        insert into core.inputs (box_id, tx_id, header_id, index)
        values (
            'b69575e11c5c43400bfead5976ee0d6245a1168396b2e2a4f384691f275d501c',
            '{tx_id}',
            '{header_id}',
            0
        );
    """

    sql += f"""
        insert into core.tokens (id, box_id, emission_amount, name, description, decimals, standard)
        values (
            '01e6498911823f4d36deaf49a964e883b2c4ae2a4530926f18b9c1411ab2a2c2',
            'eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f',
            20,
            'ORACLE',
            'datapoint token',
            0,
            'EIP-004'
        );
    """

    return sql

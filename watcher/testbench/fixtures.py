from pathlib import Path

import psycopg as pg
from psycopg.sql import Identifier, SQL
import pytest

from local import DB_HOST, DB_USER, DB_PASS
from blocks import bootstrap_block

SCHEMA_PATH = (Path(__file__).parent.absolute() / Path("../db/schema.sql")).absolute()


def conn_str(dbname: str) -> str:
    """
    Return connection string for given db name.
    """
    return f"host={DB_HOST} dbname={dbname} user={DB_USER} password={DB_PASS}"


class TestDB:
    def __init__(self):
        self._dbname: str = "test2"
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
        with pg.connect(
            f"host={DB_HOST} dbname={self._dbname} user={DB_USER} password={DB_PASS}"
        ) as conn:
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

    return sql


@pytest.fixture
def blank_db():
    with TestDB() as db:
        with pg.connect(conn_str(db)) as conn:
            yield conn


@pytest.fixture
def bootstrapped_db():
    with TestDB() as db:
        with pg.connect(conn_str(db)) as conn:
            with conn.cursor() as cur:
                cur.execute(bootstrap_sql())
            yield conn


print(bootstrap_sql())

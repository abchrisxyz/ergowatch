import os
from pathlib import Path
import platform
import asyncio

import psycopg as pg
from psycopg.sql import Identifier, SQL

from local import DB_HOST, DB_PORT, DB_USER, DB_PASS

# Fix warnings on Windows
# https://github.com/encode/httpx/issues/1287
# https://github.com/encode/httpx/issues/914
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

TEST_DB_NAME = "ew_api_pytest"

os.environ["POSTGRES_HOST"] = DB_HOST
os.environ["POSTGRES_PORT"] = str(DB_PORT)
os.environ["POSTGRES_USER"] = DB_USER
os.environ["POSTGRES_PASSWORD"] = DB_PASS
os.environ["POSTGRES_DB"] = TEST_DB_NAME

SCHEMA_PATH = (
    Path(__file__).parent.parent.absolute() / Path("../../db/schema.sql")
).absolute()

CONSTRAINTS_PATH = (
    Path(__file__).parent.parent.absolute() / Path("../../db/_constraints.sql")
).absolute()


def conn_str(dbname: str) -> str:
    """
    Return connection string for given db name.
    """
    return f"host={DB_HOST} port={DB_PORT} dbname={dbname} user={DB_USER} password={DB_PASS}"


class MockDB:
    def __init__(self, set_constraints: bool = True, sql: str = None):
        self._dbname: str = TEST_DB_NAME
        with open(SCHEMA_PATH) as f:
            self._sql = f.read()
        if set_constraints:
            with open(CONSTRAINTS_PATH) as f:
                self._sql += f.read()
        if sql is not None:
            self._sql += sql

    def _create_db(self):
        with pg.connect(conn_str("postgres"), autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    SQL("drop database if exists {};").format(Identifier(self._dbname))
                )
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

    def __enter__(self) -> str:
        self._create_db()
        self._init_db()
        return self._dbname

    def __exit__(self, exception_type, exception_value, traceback):
        self._drop_db()

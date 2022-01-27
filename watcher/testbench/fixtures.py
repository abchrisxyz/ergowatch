from pathlib import Path

import psycopg as pg
from psycopg.sql import Identifier, SQL
import pytest

from local import DBHOST, DBUSER, DBPASS

SCHEMA_PATH = (Path(__file__).parent.absolute() / Path("../db/schema.sql")).absolute()


def conn_str(dbname: str) -> str:
    """
    Return connection string for given db name.
    """
    return f"host={DBHOST} dbname={dbname} user={DBUSER} password={DBPASS}"


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
            f"host={DBHOST} dbname={self._dbname} user={DBUSER} password={DBPASS}"
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


@pytest.fixture
def temp_db():
    with TestDB() as db:
        with pg.connect(conn_str(db)) as conn:
            yield conn

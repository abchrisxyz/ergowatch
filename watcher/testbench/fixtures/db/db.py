from pathlib import Path

import psycopg as pg
from psycopg.sql import Identifier, SQL
import pytest

from local import DB_HOST, DB_PORT, DB_USER, DB_PASS
from .sql import generate_bootstrap_sql
from .sql import generate_rev0_sql
from fixtures.scenario import Scenario

# Latest schema
SCHEMA_PATH = (
    Path(__file__).parent.parent.absolute() / Path("../../../db/schema.sql")
).absolute()

CONSTRAINTS_PATH = (
    Path(__file__).parent.parent.absolute() / Path("../../../db/_constraints.sql")
).absolute()

# Initial schema (to test migrations)
SCHEMA_PATH_REV0 = (
    Path(__file__).parent.parent.absolute() / Path("../../../db/v0.4/schema_rev1.0.sql")
).absolute()

CONSTRAINTS_PATH_REV0 = (
    Path(__file__).parent.parent.absolute()
    / Path("../../../db/v0.4/constraints_rev1.0.sql")
).absolute()


TEST_DB_NAME = "ew_pytest"


@pytest.fixture
def temp_db():
    with TempDB() as db_name:
        yield conn_str(db_name)


@pytest.fixture
def temp_db_class_scoped():
    with TempDB() as db_name:
        yield conn_str(db_name)


@pytest.fixture(scope="class")
def temp_db_class_scoped():
    with TempDB() as db_name:
        yield conn_str(db_name)


@pytest.fixture(scope="class")
def unconstrained_db_class_scoped():
    with TempDB(set_constraints=False) as db_name:
        yield conn_str(db_name)


# Rev 1 mocks
@pytest.fixture
def temp_db_rev0():
    with TempDB(rev0=True) as db_name:
        yield conn_str(db_name)


@pytest.fixture(scope="class")
def temp_db_rev0_class_scoped():
    with TempDB(rev0=True) as db_name:
        yield conn_str(db_name)


@pytest.fixture(scope="class")
def unconstrained_db_rev0_class_scoped():
    with TempDB(set_constraints=False, rev0=True) as db_name:
        yield conn_str(db_name)


def conn_str(dbname: str) -> str:
    """
    Return connection string for given db name.
    """
    return f"host={DB_HOST} port={DB_PORT} dbname={dbname} user={DB_USER} password={DB_PASS}"


class TempDB:
    # Most mocks will represent a db with some data in it already,
    # so have constraints set as default.
    def __init__(self, set_constraints=True, rev0=False):
        self._dbname: str = TEST_DB_NAME
        schema_path = SCHEMA_PATH_REV0 if rev0 else SCHEMA_PATH
        constraints_path = CONSTRAINTS_PATH_REV0 if rev0 else CONSTRAINTS_PATH
        with open(schema_path) as f:
            self._sql = f.read()
        if set_constraints:
            with open(constraints_path) as f:
                self._sql += f.read()

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
        pass


def load_sql(conn: pg.Connection, sql: str):
    """
    Convenience function to load and commit sql.
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def bootstrap_db(conn: pg.Connection, scenario: Scenario):
    """
    Initialize db data to satisfy any constraints for incoming blocks.
    """
    load_sql(conn, generate_bootstrap_sql(scenario))


def fill_rev0_db(conn: pg.Connection, scenario: Scenario):
    """
    Initialize db data to satisfy any constraints for incoming blocks.
    """
    load_sql(conn, generate_rev0_sql(scenario))

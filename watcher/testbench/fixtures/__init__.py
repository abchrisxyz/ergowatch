from pathlib import Path
from collections import namedtuple

import pytest
import psycopg as pg

from .api import MockApi
from .db import conn_str
from .db import TestDB
from .db import bootstrap_sql
from .config import format_config

_MockEnv = namedtuple("MockEnv", ["db_conn", "cfg_path"])
MockEnv = lambda db_conn, cfg_path: _MockEnv(db_conn, str(cfg_path))


@pytest.fixture
def genesis_env(tmp_path):
    with TestDB() as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            cfg_path = tmp_path / Path("genesis.toml")
            cfg_path.write_text(format_config(db_name))
            with MockApi("genesis"):
                yield MockEnv(conn, cfg_path)


@pytest.fixture
def bootstrapped_env(tmp_path):
    with TestDB() as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            with conn.cursor() as cur:
                cur.execute(bootstrap_sql())
            conn.commit()
            cfg_path = tmp_path / Path("bootstrapped.toml")
            cfg_path.write_text(format_config(db_name))
            with MockApi("bootstrapped"):
                yield MockEnv(conn, cfg_path)

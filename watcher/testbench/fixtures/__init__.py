from pathlib import Path
from collections import namedtuple

import pytest
import psycopg as pg

from .api import MockApi
from .api import get_api_blocks
from .db import conn_str
from .db import TestDB
from .db import generate_bootstrap_sql
from .config import format_config

_MockEnv = namedtuple("MockEnv", ["db_conn", "cfg_path"])
MockEnv = lambda db_conn, cfg_path: _MockEnv(db_conn, str(cfg_path))


@pytest.fixture
def genesis_env(tmp_path):
    api = "genesis"
    mock_api = MockApi(api)
    with TestDB() as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            conn.commit()
            cfg_path = tmp_path / Path("genesis.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)


@pytest.fixture
def block_600k_env(tmp_path):
    api = "600k"
    mock_api = MockApi(api)
    blocks = get_api_blocks(api)
    with TestDB() as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            with conn.cursor() as cur:
                cur.execute(generate_bootstrap_sql(blocks))
            conn.commit()
            cfg_path = tmp_path / Path("600k.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)


@pytest.fixture
def token_minting_env(tmp_path):
    api = "token_minting"
    mock_api = MockApi(api)
    blocks = get_api_blocks(api)
    with TestDB() as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            with conn.cursor() as cur:
                cur.execute(generate_bootstrap_sql(blocks))
            conn.commit()
            cfg_path = tmp_path / Path("token-minting.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)


@pytest.fixture
def fork_env(tmp_path):
    api = "fork"
    mock_api = MockApi(api)
    blocks = get_api_blocks(api)
    with TestDB() as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            with conn.cursor() as cur:
                cur.execute(generate_bootstrap_sql(blocks))
            conn.commit()
            cfg_path = tmp_path / Path("fork.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)

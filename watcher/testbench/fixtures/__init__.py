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
            cfg_path = tmp_path / Path("genesis.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)


@pytest.fixture
def genesis_unconstrained_env(tmp_path):
    api = "genesis"
    mock_api = MockApi(api)
    with TestDB(set_constraints=False) as db_name:
        with pg.connect(conn_str(db_name)) as conn:
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


@pytest.fixture
def unconstrained_db_env(tmp_path):
    api = "fork"
    mock_api = MockApi(api)
    blocks = get_api_blocks(api)
    with TestDB(set_constraints=False) as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            with conn.cursor() as cur:
                cur.execute(generate_bootstrap_sql(blocks))
            conn.commit()
            cfg_path = tmp_path / Path("unconstrained-db.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)


@pytest.fixture
def bootstrap_empty_db_env(tmp_path):
    api = "bootstrap"
    mock_api = MockApi(api)
    blocks = get_api_blocks(api)
    with TestDB(set_constraints=False) as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            cfg_path = tmp_path / Path("bootstrap.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)


@pytest.fixture
def balances_bootstrap_env(tmp_path):
    api = "balances"
    mock_api = MockApi(api)
    blocks = get_api_blocks(api)
    with TestDB(set_constraints=False) as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            with conn.cursor() as cur:
                cur.execute(generate_bootstrap_sql(blocks))
            conn.commit()
            cfg_path = tmp_path / Path("balances.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)


@pytest.fixture
def balances_env(tmp_path):
    api = "balances"
    mock_api = MockApi(api)
    blocks = get_api_blocks(api)
    with TestDB() as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            with conn.cursor() as cur:
                cur.execute(generate_bootstrap_sql(blocks))
            conn.commit()
            cfg_path = tmp_path / Path("balances.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)


@pytest.fixture
def unspent_env(tmp_path):
    api = "600k"
    mock_api = MockApi(api)
    blocks = get_api_blocks(api)
    with TestDB() as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            with conn.cursor() as cur:
                cur.execute(generate_bootstrap_sql(blocks))
                # Add 4 boxes spent in block 600k, so they can be deleted
                cur.execute(
                    """
                    insert into usp.boxes (box_id)
                    values
                        ('eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f'),
                        ('c739a3294d592377a131840d491bd2b66c27f51ae2c62c66be7bb41b248f321e'),
                        ('6ca2a9d63f2f08663c09d99126ec1be7b65ce2e8f34e283c4d5af78485b47c91'),
                        ('5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4');
                """
                )
            conn.commit()
            cfg_path = tmp_path / Path("unspent.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)


@pytest.fixture
def unspent_bootstrap_env(tmp_path):
    api = "600k"
    mock_api = MockApi(api)
    blocks = get_api_blocks(api)
    with TestDB(set_constraints=False) as db_name:
        with pg.connect(conn_str(db_name)) as conn:
            with conn.cursor() as cur:
                cur.execute(generate_bootstrap_sql(blocks))
                cur.execute(
                    """
                    insert into usp.boxes (box_id)
                    values
                        ('dummy-box-ids'),
                        ('should-get-overwritten-by-bootstrapping-process');
                """
                )
            conn.commit()
            cfg_path = tmp_path / Path("unspent.toml")
            cfg_path.write_text(format_config(db_name))
            with mock_api:
                yield MockEnv(conn, cfg_path)

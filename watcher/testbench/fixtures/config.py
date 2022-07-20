import pytest
import textwrap
from pathlib import Path

from fixtures.api import MOCK_APIS_HOST
from local import DB_HOST, DB_PORT, DB_USER, DB_PASS
from .db import TEST_DB_NAME


CONFIG = textwrap.dedent(
    f"""
    debug = false

    [database]
    host = "{DB_HOST}"
    port = {DB_PORT}
    name = "{TEST_DB_NAME}"
    user = "{DB_USER}"
    pw = "{DB_PASS}"
    bootstrapping_work_mem_kb = 4000

    [node]
    url = "http://{MOCK_APIS_HOST}"
    poll_interval = 5

    [repairs]
    interval = 5 # blocks
    offset = 0 # blocks - will repair up to last height

    [coingecko]
    url = "http://{MOCK_APIS_HOST}/coingecko"
    interval = 0 # mock api is not rate-limited
    """
)


@pytest.fixture(scope="session")
def temp_cfg(tmp_path_factory):
    cfg_path = tmp_path_factory.getbasetemp() / Path("ew_test.toml")
    cfg_path.write_text(CONFIG)
    return cfg_path

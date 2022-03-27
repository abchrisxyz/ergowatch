import pytest
import textwrap
from pathlib import Path

from fixtures.api import MOCK_NODE_HOST
from local import DB_HOST, DB_PORT, DB_USER, DB_PASS
from .db import TEST_DB_NAME


def format_config(db_name: str) -> str:
    """
    Returns TOML formatted string.
    """
    return textwrap.dedent(
        f"""
        [database]
        host = "{DB_HOST}"
        port = {DB_PORT}
        name = "{db_name}"
        user = "{DB_USER}"
        pw = "{DB_PASS}"

        [node]
        url = "http://{MOCK_NODE_HOST}"
    """
    )


CONFIG = textwrap.dedent(
    f"""
    debug = false

    [database]
    host = "{DB_HOST}"
    port = {DB_PORT}
    name = "{TEST_DB_NAME}"
    user = "{DB_USER}"
    pw = "{DB_PASS}"

    [node]
    url = "http://{MOCK_NODE_HOST}"
    poll_interval = 5
    """
)


# @pytest.fixture
# def temp_cfg(tmp_path):
#     with tmp_path:
#         cfg_path = tmp_path / Path("ew_test.toml")
#         cfg_path.write_text(CONFIG)
#         yield cfg_path


@pytest.fixture(scope="session")
def temp_cfg(tmp_path_factory):
    cfg_path = tmp_path_factory.getbasetemp() / Path("ew_test.toml")
    cfg_path.write_text(CONFIG)
    return cfg_path

import textwrap

from fixtures.api import MOCK_NODE_HOST
from local import DB_HOST, DB_PORT, DB_USER, DB_PASS


def format_config(db_name: str) -> str:
    """
    Returns TOML formatted string.
    """
    return textwrap.dedent(
        f"""
        debug = false

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

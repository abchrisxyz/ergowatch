import pytest

from fixtures import genesis_env
from fixtures import bootstrapped_env


@pytest.mark.order(1)
class TestGenesisDB:
    def test_db_is_empty(self, genesis_env):
        """
        Check connection works and db is blank
        """
        with genesis_env.db_conn.cursor() as cur:
            cur.execute("select count(*) as cnt from core.headers;")
            row = cur.fetchone()
        assert row[0] == 0


@pytest.mark.order(1)
class TestBootsrappedDB:
    def test_db_is_at_599999(self, bootstrapped_env):
        """
        Check connection works and db is bootstrapped
        """
        with bootstrapped_env.db_conn.cursor() as cur:
            cur.execute("select height from core.headers order by 1 desc limit 1;")
            row = cur.fetchone()
        assert row[0] == 599_999

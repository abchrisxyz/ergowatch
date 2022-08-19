import pytest
import psycopg as pg

from fixtures.db import temp_db_class_scoped


@pytest.mark.skip("old")
def test_this_should_work(temp_db_class_scoped):
    long_address = "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"
    tx_id = "9edee97a3eaa688ad2c1b31f6c6fbe4b9b3f736c34b16057088a4252308131d0"
    token_id = "9edee97a3eaa688ad2c1b31f6c6fbe4b9b3f736c34b16057088a4252308131d0"
    with pg.connect(temp_db_class_scoped) as conn:
        with conn.cursor() as cur:
            # cur.execute(
            #     "alter table bal.tokens_diffs drop constraint tokens_diffs_pkey;"
            # )

            cur.execute(
                "create index on bal.tokens_diffs (address, token_id, height, tx_id);"
            )

            cur.execute(
                f"""
                insert into bal.tokens_diffs (address, token_id, height, tx_id, value) values
                    ('{long_address}', '{token_id}', 100, '{tx_id}', 1000);
                """
            )

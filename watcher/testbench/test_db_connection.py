from fixtures import blank_db, bootstrapped_db


def test_blank_db_connection(blank_db):
    """
    Check connection works and db is blank
    """
    with blank_db.cursor() as cur:
        cur.execute("select 1 + 1;")
        row = cur.fetchone()
    assert row[0] == 2


def test_boostrapped_db_connection(bootstrapped_db):
    """
    Check connection works and db is bootstrapped
    """
    with bootstrapped_db.cursor() as cur:
        cur.execute("select height from core.headers order by 1 desc limit 1;")
        row = cur.fetchone()
    assert row[0] == 599_999

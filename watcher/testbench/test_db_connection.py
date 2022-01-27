from fixtures import temp_db


def test_db_connection(temp_db):
    with temp_db.cursor() as cur:
        cur.execute("select 1 + 1;")
        row = cur.fetchone()
    assert row[0] == 2

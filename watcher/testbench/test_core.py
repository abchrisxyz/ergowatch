import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil, GENESIS_ID
from fixtures.config import temp_cfg
from fixtures.db import bootstrap_db
from fixtures.db import fill_rev1_db
from fixtures.db import temp_db_class_scoped
from fixtures.db import temp_db_rev1_class_scoped
from fixtures.db import unconstrained_db_class_scoped
from fixtures.addresses import AddressCatalogue as AC
from utils import run_watcher
from utils import assert_pk
from utils import assert_fk
from utils import assert_unique
from utils import assert_column_not_null
from utils import assert_index
from utils import assert_column_ge
from utils import assert_column_le

ORDER = 10


def make_blocks(height: int):
    """
    Returns test blocks starting at giving height

    block a - coinbase tx:
        base-box1 1000 --> base-box2  950
                           con1-box1   50

    ----------------------fork-of-b----------------------
    block x - fork of block b to be ignored/rolled back:
        con1-box1   50 --> con9-box1   30
                           pub9-box1   20 (3000 con1-box1)
    ------------------------------------------------------

    block b - minting a token and using registers:
        con1-box1   50 --> con2-box1   40
                           pub1-box1   10 (2000 con1-box1)

    block c using a datainput (in {}) and spending tokens:
        pub1-box1   10 --> pub1-box2    5 (1500 con1-box1)
       {con2-box1}         pub2-box1    4 ( 500 con1-box1)
                           fees-box1    1

        fees-box1    1 --> con1-box2    1
    """
    base = AC.coinbase
    fees = AC.fees
    con1 = AC.get("con1")
    con2 = AC.get("con2")
    pub1 = AC.get("pub1")
    pub2 = AC.get("pub2")

    h = height + 1
    tx_a1 = {
        "id": "tx-a1",
        "inputs": [
            {
                "boxId": "base-box1",
            }
        ],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "base-box2",
                "value": 950,
                "ergoTree": base.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-a1",
                "index": 0,
            },
            {
                "boxId": "con1-box1",
                "value": 50,
                "ergoTree": con1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-a1",
                "index": 1,
            },
        ],
        "size": 344,
    }

    h += 1
    tx_x1 = {
        "id": "tx-x1",
        "inputs": [{"boxId": "con1-box1"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "con9-box1",
                "value": 30,
                "ergoTree": con2.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {
                    "R4": "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
                    "R5": "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
                    "R6": "05a4c3edd9998877",
                },
                "transactionId": "tx-x1",
                "index": 0,
            },
            {
                "boxId": "pub9-box1",
                "value": 20,
                "ergoTree": pub1.ergo_tree,
                "assets": [
                    {
                        "tokenId": "con1-box1",
                        "amount": 3000,
                    }
                ],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-x1",
                "index": 1,
            },
        ],
        "size": 674,
    }

    tx_b1 = {
        "id": "tx-b1",
        "inputs": [{"boxId": "con1-box1"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "con2-box1",
                "value": 40,
                "ergoTree": con2.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {
                    "R4": "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
                    "R5": "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
                    "R6": "05a4c3edd9998877",
                },
                "transactionId": "tx-b1",
                "index": 0,
            },
            {
                "boxId": "pub1-box1",
                "value": 10,
                "ergoTree": pub1.ergo_tree,
                "assets": [
                    {
                        "tokenId": "con1-box1",
                        "amount": 2000,
                    }
                ],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-b1",
                "index": 1,
            },
        ],
        "size": 674,
    }

    h += 1
    tx_c1 = {
        "id": "tx-c1",
        "inputs": [{"boxId": "pub1-box1"}],
        "dataInputs": [{"boxId": "con2-box1"}],
        "outputs": [
            {
                "boxId": "pub1-box2",
                "value": 5,
                "ergoTree": pub1.ergo_tree,
                "assets": [
                    {
                        "tokenId": "con1-box1",
                        "amount": 1500,
                    }
                ],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c1",
                "index": 0,
            },
            {
                "boxId": "pub2-box1",
                "value": 4,
                "ergoTree": pub2.ergo_tree,
                "assets": [
                    {
                        "tokenId": "con1-box1",
                        "amount": 500,
                    }
                ],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c1",
                "index": 1,
            },
            {
                "boxId": "fees-box1",
                "value": 1,
                "ergoTree": fees.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c1",
                "index": 2,
            },
        ],
        "size": 100,
    }

    tx_c2 = {
        "id": "tx-c2",
        "inputs": [
            {
                "boxId": "fees-box1",
            }
        ],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "con1-box2",
                "value": 1,
                "ergoTree": con1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c2",
                "index": 0,
            }
        ],
        "size": 100,
    }

    block_a = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560100000,
            "size": 123,
            "height": height + 1,
            "id": "block-a",
            "parentId": GENESIS_ID,
        },
        "blockTransactions": {
            "headerId": "block-a",
            "transactions": [tx_a1],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    block_x = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560200000,
            "size": 123,
            "height": height + 2,
            "id": "block-x",
            "parentId": "block-a",
        },
        "blockTransactions": {
            "headerId": "block-x",
            "transactions": [tx_x1],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    block_b = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560200000,
            "size": 123,
            "height": height + 2,
            "id": "block-b",
            "parentId": "block-a",
        },
        "blockTransactions": {
            "headerId": "block-b",
            "transactions": [tx_b1],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    block_c = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560300000,
            "size": 123,
            "height": height + 3,
            "id": "block-c",
            "parentId": "block-b",
        },
        "blockTransactions": {
            "headerId": "block-c",
            "transactions": [tx_c1, tx_c2],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    return [block_a, block_x, block_b, block_c]


@pytest.mark.order(ORDER)
class TestSync:
    """
    Start with bootstrapped db.
    """

    start_height = 599_999

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, blocks)

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-c" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


@pytest.mark.order(ORDER)
class TestSyncRollback:
    """
    Start with bootstrapped db.
    Forking scenario triggering a rollback.
    """

    start_height = 599_999

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()

            # Initially have blocks a and x only
            first_blocks = blocks[0:2]
            api.set_blocks(first_blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, first_blocks)

            # Run to include block x
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-x" in cp.stdout.decode()

            # Now make all blocks visible
            api.set_blocks(blocks)

            # Run again
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Rolling back block block-x" in cp.stdout.decode()
            assert "Including block block-b" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


@pytest.mark.order(ORDER)
class TestSyncNoForkChild:
    """
    Start with bootstrapped db.
    Scenario where node has two block candidates for last height.
    """

    start_height = 599_999

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()

            # Initially have blocks a, b and x
            first_blocks = blocks[0:3]
            api.set_blocks(first_blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, first_blocks)

            # 1 st run
            # No way to tell fork appart, should pick 1st block in order of appearance (block-x)
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-x" in cp.stdout.decode()
            assert "Including block block-b" not in cp.stdout.decode()
            assert "no child" not in cp.stdout.decode()

            # Now make all blocks visible
            api.set_blocks(blocks)

            # Run again
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-c" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


@pytest.mark.order(ORDER)
class TestGenesis:
    """
    Start with empty, unconstrained db.
    """

    start_height = 0

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, unconstrained_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(blocks)

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Bootstrapping step 1/2 - syncing core tables" in cp.stdout.decode()

            with pg.connect(unconstrained_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


@pytest.mark.order(ORDER)
class TestMigrations:
    """
    Aplly migration to synced db
    """

    start_height = 599_999

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_rev1_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(blocks)

            # Prepare db
            with pg.connect(temp_db_rev1_class_scoped) as conn:
                fill_rev1_db(conn, blocks)

            # Run
            cp = run_watcher(temp_cfg, allow_migrations=True)
            assert cp.returncode == 0
            assert "Applying migration 1 (revision 2)" in cp.stdout.decode()
            assert "Applying migration 2 (revision 3)" in cp.stdout.decode()

            with pg.connect(temp_db_rev1_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


def _test_db_state(conn: pg.Connection, start_height: int):
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_headers(cur, start_height)
        assert_transactions(cur, start_height)
        assert_inputs(cur)
        assert_data_inputs(cur)
        assert_outputs(cur, start_height)
        assert_box_registers(cur)
        assert_tokens(cur)
        assert_box_assets(cur)


def assert_db_constraints(conn: pg.Connection):
    # Headers
    assert_pk(conn, "core", "headers", ["height"])
    assert_column_not_null(conn, "core", "headers", "id")
    assert_column_not_null(conn, "core", "headers", "parent_id")
    assert_column_not_null(conn, "core", "headers", "timestamp")
    assert_unique(conn, "core", "headers", ["id"])
    assert_unique(conn, "core", "headers", ["parent_id"])

    # Transactions
    assert_pk(conn, "core", "transactions", ["id"])
    assert_fk(conn, "core", "transactions", "transactions_header_id_fkey")
    assert_index(conn, "core", "transactions", "transactions_height_idx")

    # Outputs
    assert_pk(conn, "core", "outputs", ["box_id"])
    assert_column_not_null(conn, "core", "outputs", "tx_id")
    assert_column_not_null(conn, "core", "outputs", "header_id")
    assert_column_not_null(conn, "core", "outputs", "address")
    assert_fk(conn, "core", "outputs", "outputs_tx_id_fkey")
    assert_fk(conn, "core", "outputs", "outputs_header_id_fkey")
    assert_index(conn, "core", "outputs", "outputs_tx_id_idx")
    assert_index(conn, "core", "outputs", "outputs_header_id_idx")
    assert_index(conn, "core", "outputs", "outputs_address_idx")
    assert_index(conn, "core", "outputs", "outputs_index_idx")

    # Inputs
    assert_pk(conn, "core", "inputs", ["box_id"])
    assert_column_not_null(conn, "core", "inputs", "tx_id")
    assert_column_not_null(conn, "core", "inputs", "header_id")
    assert_fk(conn, "core", "inputs", "inputs_tx_id_fkey")
    assert_fk(conn, "core", "inputs", "inputs_header_id_fkey")
    assert_index(conn, "core", "inputs", "inputs_tx_id_idx")
    assert_index(conn, "core", "inputs", "inputs_header_id_idx")
    assert_index(conn, "core", "inputs", "inputs_index_idx")

    # Data-inputs
    assert_pk(conn, "core", "data_inputs", ["box_id", "tx_id"])
    assert_column_not_null(conn, "core", "data_inputs", "header_id")
    assert_fk(conn, "core", "data_inputs", "data_inputs_tx_id_fkey")
    assert_fk(conn, "core", "data_inputs", "data_inputs_header_id_fkey")
    assert_fk(conn, "core", "data_inputs", "data_inputs_box_id_fkey")
    assert_index(conn, "core", "data_inputs", "data_inputs_tx_id_idx")
    assert_index(conn, "core", "data_inputs", "data_inputs_header_id_idx")

    # Box registers
    assert_pk(conn, "core", "box_registers", ["id", "box_id"])
    assert_fk(conn, "core", "box_registers", "box_registers_box_id_fkey")
    assert_column_ge(conn, "core", "box_registers", "id", 4)
    assert_column_le(conn, "core", "box_registers", "id", 9)

    # Tokens
    assert_pk(conn, "core", "tokens", ["id", "box_id"])
    assert_column_not_null(conn, "core", "tokens", "box_id")
    assert_fk(conn, "core", "tokens", "tokens_box_id_fkey")
    assert_column_ge(conn, "core", "tokens", "emission_amount", 0)

    # Box assets
    assert_pk(conn, "core", "box_assets", ["box_id", "token_id"])
    assert_column_not_null(conn, "core", "box_assets", "box_id")
    assert_column_not_null(conn, "core", "box_assets", "token_id")
    assert_fk(conn, "core", "box_assets", "box_assets_box_id_fkey")
    assert_column_ge(conn, "core", "box_assets", "amount", 0)


def assert_headers(cur: pg.Cursor, start_height: int):
    # 4 headers: 1 parent + 3 from blocks
    cur.execute(
        "select height, id, parent_id, timestamp from core.headers order by 1, 2;"
    )
    rows = cur.fetchall()
    assert len(rows) == 4
    if start_height == 0:
        # Genesis parent_id and timestamp are set by watcher
        assert rows[0] == (
            start_height,
            GENESIS_ID,
            "genesis",
            1561978800000,
        )
    else:
        # Genesis parent_id and timestamp are set by db fixture
        assert rows[0] == (
            start_height,
            GENESIS_ID,
            "bootstrap-parent-header-id",
            1234560000000,
        )
    assert rows[1] == (start_height + 1, "block-a", GENESIS_ID, 1234560100000)
    assert rows[2] == (start_height + 2, "block-b", "block-a", 1234560200000)
    assert rows[3] == (start_height + 3, "block-c", "block-b", 1234560300000)


def assert_transactions(cur: pg.Cursor, start_height: int):
    # 5 txs: 1 bootstrap + 4 from blocks
    cur.execute(
        "select height, header_id, index, id from core.transactions order by 1, 3;"
    )
    rows = cur.fetchall()
    assert len(rows) == 5
    bootstrap_tx_id = GENESIS_ID if start_height == 0 else "bootstrap-tx"
    assert rows[0] == (start_height, GENESIS_ID, 0, bootstrap_tx_id)
    assert rows[1] == (start_height + 1, "block-a", 0, "tx-a1")
    assert rows[2] == (start_height + 2, "block-b", 0, "tx-b1")
    assert rows[3] == (start_height + 3, "block-c", 0, "tx-c1")
    assert rows[4] == (start_height + 3, "block-c", 1, "tx-c2")


def assert_inputs(cur: pg.Cursor):
    # 4 inputs
    cur.execute(
        "select header_id, tx_id, index, box_id from core.inputs order by 1, 2;"
    )
    rows = cur.fetchall()
    assert len(rows) == 4
    assert rows[0] == ("block-a", "tx-a1", 0, "base-box1")
    assert rows[1] == ("block-b", "tx-b1", 0, "con1-box1")
    assert rows[2] == ("block-c", "tx-c1", 0, "pub1-box1")
    assert rows[3] == ("block-c", "tx-c2", 0, "fees-box1")


def assert_data_inputs(cur: pg.Cursor):
    # 1 data-inputs
    cur.execute(
        "select header_id, tx_id, index, box_id from core.data_inputs order by 1, 2;"
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0] == ("block-c", "tx-c1", 0, "con2-box1")


def assert_outputs(cur: pg.Cursor, start_height: int):
    # 9 outputs: 1 bootstrap + 8 from blocks
    cur.execute(
        """
        select creation_height
            , header_id
            , tx_id
            , index
            , box_id
            , value
            , address
        from core.outputs
        order by creation_height, tx_id, index;
    """
    )
    rows = cur.fetchall()
    assert len(rows) == 9
    bootstrap_tx_id = GENESIS_ID if start_height == 0 else "bootstrap-tx"
    assert rows[0] == (
        start_height + 0,
        GENESIS_ID,
        bootstrap_tx_id,
        0,
        "base-box1",
        1000,
        AC.coinbase.address,
    )
    assert rows[1] == (
        start_height + 1,
        "block-a",
        "tx-a1",
        0,
        "base-box2",
        950,
        AC.boxid2addr("base-box2"),
    )
    assert rows[2] == (
        start_height + 1,
        "block-a",
        "tx-a1",
        1,
        "con1-box1",
        50,
        AC.boxid2addr("con1-box1"),
    )
    assert rows[3] == (
        start_height + 2,
        "block-b",
        "tx-b1",
        0,
        "con2-box1",
        40,
        AC.boxid2addr("con2-box1"),
    )
    assert rows[4] == (
        start_height + 2,
        "block-b",
        "tx-b1",
        1,
        "pub1-box1",
        10,
        AC.boxid2addr("pub1-box1"),
    )
    assert rows[5] == (
        start_height + 3,
        "block-c",
        "tx-c1",
        0,
        "pub1-box2",
        5,
        AC.boxid2addr("pub1-box2"),
    )
    assert rows[6] == (
        start_height + 3,
        "block-c",
        "tx-c1",
        1,
        "pub2-box1",
        4,
        AC.boxid2addr("pub2-box1"),
    )
    assert rows[7] == (
        start_height + 3,
        "block-c",
        "tx-c1",
        2,
        "fees-box1",
        1,
        AC.boxid2addr("fees-box1"),
    )
    assert rows[8] == (
        start_height + 3,
        "block-c",
        "tx-c2",
        0,
        "con1-box2",
        1,
        AC.boxid2addr("con1-box2"),
    )


def assert_box_registers(cur: pg.Cursor):
    # 3 registers
    cur.execute(
        """
        select id
            , box_id
            , value_type
            , serialized_value
            , rendered_value
        from core.box_registers
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 3
    assert rows[0] == (
        4,
        "con2-box1",
        "SGroupElement",
        "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
        "03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
    )
    assert rows[1] == (
        5,
        "con2-box1",
        "Coll[SByte]",
        "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
        "98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
    )
    assert rows[2] == (
        6,
        "con2-box1",
        "SLong",
        "05a4c3edd9998877",
        "261824656027858",
    )


def assert_tokens(cur: pg.Cursor):
    # 1 minted token
    cur.execute(
        """
        select id
            , box_id
            , emission_amount
            , name
            , description
            , decimals
            , standard
        from core.tokens
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0] == ("con1-box1", "pub1-box1", 2000, None, None, None, None)


def assert_box_assets(cur: pg.Cursor):
    # 3 boxes containing some tokens
    cur.execute(
        """
        select box_id
            , token_id
            , amount
        from core.box_assets
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 3
    assert rows[0] == ("pub1-box1", "con1-box1", 2000)
    assert rows[1] == ("pub1-box2", "con1-box1", 1500)
    assert rows[2] == ("pub2-box1", "con1-box1", 500)

import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil, GENESIS_ID
from fixtures.config import temp_cfg
from fixtures.db import bootstrap_db
from fixtures.db import fill_rev0_db
from fixtures.db import temp_db_class_scoped
from fixtures.db import temp_db_rev0_class_scoped
from fixtures.db import unconstrained_db_class_scoped
from fixtures.scenario import Scenario
from utils import run_watcher
from utils import assert_pk
from utils import assert_fk
from utils import assert_excl
from utils import assert_unique
from utils import assert_column_not_null
from utils import assert_index
from utils import assert_index_def
from utils import assert_column_ge
from utils import assert_column_le

ORDER = 10


SCENARIO_DESCRIPTION = """
    block-a
        // coinbase tx:
        base-box1 1000
        >
        tres-box1    0
        base-box2  950
        con1-box1   50

    //----------------------fork-of-b----------------------
    // fork of block b to be ignored/rolled back:
    block-x
        con1-box1   50
        >
        con9-box1   30
        pub9-box1   20 (con1-box1: 3000)
    //------------------------------------------------------

    // minting a token and using registers:
    block-b-a
        con1-box1   50
        >
        con2-box1   40
        pub1-box1   10 (con1-box1: 2000) [
            0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8,
            0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8,
            05a4c3edd9998877
        ]

    // using a datainput (in {}) and spending tokens
    block-c
        pub1-box1   10
        {con2-box1}
        >
        pub1-box2    5 (con1-box1: 1500)
        pub2-box1    4 (con1-box1: 500)
        fees-box1    1
        --
        fees-box1    1
        >
        con1-box2    1
    """


def set_scenario_headers_and_extension(s: Scenario):
    """
    Modifies headers and extension
    """
    set_header_difficulty(s)
    set_header_votes(s)
    set_block_extensions(s)


def set_header_votes(s: Scenario):
    """
    Modifies votes in scenario block headers.
    """
    assert len(s._blocks) in (3, 4)
    if len(s._blocks) == 4:
        # Block x
        s._blocks[1]["header"]["votes"] = "030500"
        # Block b
        s._blocks[2]["header"]["votes"] = "080400"
    if len(s._blocks) == 3:
        # Block b
        s._blocks[1]["header"]["votes"] = "080400"


def set_header_difficulty(s: Scenario):
    """
    Modifies dificulty in scenario block headers.
    """
    assert len(s._blocks) in (3, 4)
    if len(s._blocks) == 4:
        # Block x
        s._blocks[1]["header"]["difficulty"] = f"{s.DEFAULT_DIFFICULTY + 2}"
        # Block b
        s._blocks[2]["header"]["difficulty"] = f"{s.DEFAULT_DIFFICULTY + 1}"
    if len(s._blocks) == 3:
        # Block b
        s._blocks[1]["header"]["difficulty"] = f"{s.DEFAULT_DIFFICULTY + 1}"


def set_block_extensions(s: Scenario):
    """
    Modifies block extensions
    """
    assert len(s._blocks) in (3, 4)
    system_parameters = [
        ["0001", "00000001"],
        ["0002", "00000002"],
        ["0003", "00000003"],
        ["0004", "00000004"],
        ["0005", "00000005"],
        ["0006", "00000006"],
        ["0007", "00000007"],
        ["0008", "00000008"],
        ["0078", "should-be-unhandled-120"],  # 120
        ["0079", "should-be-unhandled-121"],  # 121
        ["007a", "should-be-unhandled-122"],  # 122
        ["007b", "00000002"],  # 123
        ["007c", "should-be-unhandled-124"],  # 124
        ["0201", "should-be-unhandled-too"],
    ]
    if len(s._blocks) == 4:
        # Block x - with block version = 3
        s._blocks[1]["extension"]["fields"].append(["007b", "00000003"])
        # Block b
        s._blocks[2]["extension"]["fields"].extend(system_parameters)
        assert s._blocks[2]["header"]["id"] == "block-b"
    if len(s._blocks) == 3:
        # Block b
        s._blocks[1]["extension"]["fields"].extend(system_parameters)


def set_invalid_ergo_tree(s: Scenario):
    """
    Modifies ergo tree of a specific box to trigger error handling.
    """
    # First block is alwats block a
    box = s._blocks[0]["blockTransactions"]["transactions"][0]["outputs"][0]
    box["ergoTree"] = box["ergoTree"][:-2]


@pytest.mark.order(ORDER)
class TestSync:
    """
    Start with bootstrapped db.
    """

    parent_height = 599_999
    first_ts = 1234560000000 + Scenario.DT
    scenario = Scenario(
        SCENARIO_DESCRIPTION,
        parent_height,
        first_ts,
    )
    set_scenario_headers_and_extension(scenario)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """

        with MockApi():
            api = ApiUtil()
            api.set_blocks(self.scenario.blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, self.scenario)

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-c" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


@pytest.mark.order(ORDER)
class TestSyncRollback:
    """
    Start with bootstrapped db.
    Forking scenario triggering a rollback.
    """

    parent_height = 599_999
    first_ts = 1234560000000 + Scenario.DT
    scenario = Scenario(
        SCENARIO_DESCRIPTION,
        parent_height,
        first_ts,
    )
    set_scenario_headers_and_extension(scenario)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi():
            api = ApiUtil()

            # Initially have blocks a and x only
            self.scenario.mask(2)
            api.set_blocks(self.scenario.blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, self.scenario)

            # Run to include block x
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-x" in cp.stdout.decode()

            # Now make all blocks visible
            self.scenario.unmask()
            api.set_blocks(self.scenario.blocks)

            # Run again
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Rolling back block block-x" in cp.stdout.decode()
            assert "Including block block-b" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


@pytest.mark.order(ORDER)
class TestSyncNoForkChild:
    """
    Start with bootstrapped db.
    Scenario where node has two block candidates for last height.
    """

    parent_height = 599_999
    first_ts = 1234560000000 + Scenario.DT
    scenario = Scenario(
        SCENARIO_DESCRIPTION,
        parent_height,
        first_ts,
    )
    set_scenario_headers_and_extension(scenario)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi():
            api = ApiUtil()

            # Initially have blocks a, b and x
            self.scenario.mask(3)
            api.set_blocks(self.scenario.blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, self.scenario)

            # 1 st run
            # No way to tell fork appart, should pick 1st block in order of appearance (block-x)
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-x" in cp.stdout.decode()
            assert "Including block block-b" not in cp.stdout.decode()
            assert "no child" not in cp.stdout.decode()

            # Now make all blocks visible
            self.scenario.unmask()
            api.set_blocks(self.scenario.blocks)

            # Run again
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-c" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


@pytest.mark.order(ORDER)
class TestGenesis:
    """
    Start with empty, unconstrained db.
    """

    parent_height = 0
    first_ts = 1234560000000 + Scenario.DT
    scenario = Scenario(
        SCENARIO_DESCRIPTION,
        parent_height,
        first_ts,
    )
    set_scenario_headers_and_extension(scenario)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, unconstrained_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi():
            api = ApiUtil()
            api.set_blocks(self.scenario.blocks)

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Bootstrapping step 1/2 - syncing core tables" in cp.stdout.decode()

            with pg.connect(unconstrained_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


@pytest.mark.order(ORDER)
class TestMigrations:
    """
    Aplly migration to synced db
    """

    parent_height = 599_999
    first_ts = 1234560000000 + Scenario.DT
    scenario = Scenario(
        SCENARIO_DESCRIPTION,
        parent_height,
        first_ts,
        main_only=True,
    )
    set_scenario_headers_and_extension(scenario)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_rev0_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi():
            api = ApiUtil()
            api.set_blocks(self.scenario.blocks)

            # Prepare db
            with pg.connect(temp_db_rev0_class_scoped) as conn:
                fill_rev0_db(conn, self.scenario)

            # Run
            cp = run_watcher(temp_cfg, allow_migrations=True)
            assert cp.returncode == 0
            assert "Applying migration 1" in cp.stdout.decode()
            assert "Applying migration 2" in cp.stdout.decode()

            with pg.connect(temp_db_rev0_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


@pytest.mark.order(ORDER)
class TestSyncDeserError:
    """
    Separate scenario to check ergo box deserialization error handling.
    """

    desc = """
        block-a
        // coinbase tx:
        base-box1 1001
        >
        base-box2  950
        pub1-box1   50
    """

    parent_height = 599_999
    first_ts = 1234560000000 + Scenario.DT
    scenario = Scenario(desc, parent_height, first_ts)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """

        with MockApi():
            # Mess with the ergo tree
            s = self.scenario
            box = s._blocks[0]["blockTransactions"]["transactions"][0]["outputs"][0]
            box["ergoTree"] = box["ergoTree"][:-2]

            api = ApiUtil()
            api.set_blocks(self.scenario.blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, self.scenario)

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-a" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_box_size(self, synced_db: pg.Connection):
        with synced_db.cursor() as cur:
            cur.execute(
                f"""
                select size
                from core.outputs
                where box_id = '{self.scenario.id("base-box2")}';
            """
            )
            row = cur.fetchone()
        # Undeserializable box has size 0
        assert row[0] == 0


def _test_db_state(conn: pg.Connection, s: Scenario):
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_headers(cur, s)
        assert_transactions(cur, s)
        assert_addresses(cur, s)
        assert_inputs(cur, s)
        assert_data_inputs(cur, s)
        assert_outputs(cur, s)
        assert_box_registers(cur, s)
        assert_tokens(cur, s)
        assert_box_assets(cur, s)
        assert_system_parameters(cur, s)
        assert_unhandled_extension_fields(cur, s)


def assert_db_constraints(conn: pg.Connection):
    # Headers
    assert_pk(conn, "core", "headers", ["height"])
    assert_column_not_null(conn, "core", "headers", "height")
    assert_column_not_null(conn, "core", "headers", "id")
    assert_column_not_null(conn, "core", "headers", "parent_id")
    assert_column_not_null(conn, "core", "headers", "timestamp")
    assert_column_not_null(conn, "core", "headers", "difficulty")
    assert_column_not_null(conn, "core", "headers", "vote1")
    assert_column_not_null(conn, "core", "headers", "vote2")
    assert_column_not_null(conn, "core", "headers", "vote3")
    assert_unique(conn, "core", "headers", ["id"])
    assert_unique(conn, "core", "headers", ["parent_id"])

    # Transactions
    assert_pk(conn, "core", "transactions", ["id"])
    assert_column_not_null(conn, "core", "transactions", "id")
    assert_column_not_null(conn, "core", "transactions", "header_id")
    assert_column_not_null(conn, "core", "transactions", "height")
    assert_column_not_null(conn, "core", "transactions", "index")
    assert_fk(conn, "core", "transactions", "transactions_header_id_fkey")
    assert_index(conn, "core", "transactions", "transactions_height_idx")

    # Addresses
    assert_pk(conn, "core", "addresses", ["id"])
    assert_column_ge(conn, "core", "addresses", "id", 1)
    assert_excl(conn, "core", "addresses", "addresses_address_excl")
    assert_column_not_null(conn, "core", "addresses", "id")
    assert_column_not_null(conn, "core", "addresses", "address")
    assert_column_not_null(conn, "core", "addresses", "spot_height")
    assert_column_not_null(conn, "core", "addresses", "p2pk")
    assert_column_not_null(conn, "core", "addresses", "miner")
    assert_index_def(
        conn,
        "core",
        "addresses",
        "CREATE INDEX addresses_md5_idx ON core.addresses USING btree (md5(address))",
    )
    assert_index_def(
        conn,
        "core",
        "addresses",
        "CREATE INDEX addresses_spot_height_idx ON core.addresses USING brin (spot_height)",
    )
    ...

    # Outputs
    assert_pk(conn, "core", "outputs", ["box_id"])
    assert_column_not_null(conn, "core", "outputs", "box_id")
    assert_column_not_null(conn, "core", "outputs", "tx_id")
    assert_column_not_null(conn, "core", "outputs", "header_id")
    assert_column_not_null(conn, "core", "outputs", "creation_height")
    assert_column_not_null(conn, "core", "outputs", "address_id")
    assert_column_not_null(conn, "core", "outputs", "index")
    assert_column_not_null(conn, "core", "outputs", "value")
    assert_column_not_null(conn, "core", "outputs", "size")
    assert_fk(conn, "core", "outputs", "outputs_tx_id_fkey")
    assert_fk(conn, "core", "outputs", "outputs_header_id_fkey")
    assert_index(conn, "core", "outputs", "outputs_tx_id_idx")
    assert_index(conn, "core", "outputs", "outputs_header_id_idx")
    assert_index(conn, "core", "outputs", "outputs_address_id_idx")
    assert_index(conn, "core", "outputs", "outputs_index_idx")

    # Inputs
    assert_pk(conn, "core", "inputs", ["box_id"])
    assert_column_not_null(conn, "core", "inputs", "box_id")
    assert_column_not_null(conn, "core", "inputs", "tx_id")
    assert_column_not_null(conn, "core", "inputs", "header_id")
    assert_column_not_null(conn, "core", "inputs", "index")
    assert_fk(conn, "core", "inputs", "inputs_tx_id_fkey")
    assert_fk(conn, "core", "inputs", "inputs_header_id_fkey")
    assert_index(conn, "core", "inputs", "inputs_tx_id_idx")
    assert_index(conn, "core", "inputs", "inputs_header_id_idx")
    assert_index(conn, "core", "inputs", "inputs_index_idx")

    # Data-inputs
    assert_pk(conn, "core", "data_inputs", ["box_id", "tx_id"])
    assert_column_not_null(conn, "core", "data_inputs", "box_id")
    assert_column_not_null(conn, "core", "data_inputs", "tx_id")
    assert_column_not_null(conn, "core", "data_inputs", "header_id")
    assert_column_not_null(conn, "core", "data_inputs", "index")
    assert_fk(conn, "core", "data_inputs", "data_inputs_tx_id_fkey")
    assert_fk(conn, "core", "data_inputs", "data_inputs_header_id_fkey")
    assert_fk(conn, "core", "data_inputs", "data_inputs_box_id_fkey")
    assert_index(conn, "core", "data_inputs", "data_inputs_tx_id_idx")
    assert_index(conn, "core", "data_inputs", "data_inputs_header_id_idx")

    # Box registers
    assert_pk(conn, "core", "box_registers", ["id", "box_id"])
    assert_column_not_null(conn, "core", "box_registers", "id")
    assert_column_not_null(conn, "core", "box_registers", "box_id")
    assert_column_not_null(conn, "core", "box_registers", "value_type")
    assert_column_not_null(conn, "core", "box_registers", "serialized_value")
    assert_column_not_null(conn, "core", "box_registers", "rendered_value")
    assert_fk(conn, "core", "box_registers", "box_registers_box_id_fkey")
    assert_column_ge(conn, "core", "box_registers", "id", 4)
    assert_column_le(conn, "core", "box_registers", "id", 9)

    # Tokens
    assert_pk(conn, "core", "tokens", ["id", "box_id"])
    assert_column_not_null(conn, "core", "tokens", "id")
    assert_column_not_null(conn, "core", "tokens", "box_id")
    assert_column_not_null(conn, "core", "tokens", "emission_amount")
    assert_fk(conn, "core", "tokens", "tokens_box_id_fkey")
    assert_column_ge(conn, "core", "tokens", "emission_amount", 0)

    # Box assets
    assert_pk(conn, "core", "box_assets", ["box_id", "token_id"])
    assert_column_not_null(conn, "core", "box_assets", "box_id")
    assert_column_not_null(conn, "core", "box_assets", "token_id")
    assert_column_not_null(conn, "core", "box_assets", "amount")
    assert_fk(conn, "core", "box_assets", "box_assets_box_id_fkey")
    assert_column_ge(conn, "core", "box_assets", "amount", 0)
    assert_index(conn, "core", "box_assets", "box_assets_box_id_idx")

    # System parameters
    assert_pk(conn, "core", "system_parameters", ["height"])


def assert_headers(cur: pg.Cursor, s):
    # 4 headers: 1 parent + 3 from blocks
    cur.execute(
        "select height, id, parent_id, timestamp, difficulty, vote1, vote2, vote3 from core.headers order by 1, 2;"
    )
    rows = cur.fetchall()
    assert len(rows) == 4
    if s.parent_height == 0:
        # Genesis parent_id and timestamp are set by watcher
        assert rows[0] == (
            s.parent_height,
            GENESIS_ID,
            "genesis",
            s.genesis_ts,
            0,  # zero difficulty
            0,
            0,
            0,
        )
    else:
        # Genesis parent_id and timestamp are set by db fixture
        assert rows[0] == (
            s.parent_height,
            GENESIS_ID,
            "bootstrap-parent-header-id",
            s.first_ts - s.dt,
            s.DEFAULT_DIFFICULTY,
            0,
            0,
            0,
        )
    assert rows[1] == (
        s.parent_height + 1,
        "block-a",
        GENESIS_ID,
        s.first_ts + s.DT * 0,
        s.DEFAULT_DIFFICULTY,
        0,
        0,
        0,
    )
    assert rows[2] == (
        s.parent_height + 2,
        "block-b",
        "block-a",
        s.first_ts + s.dt * 1,
        s.DEFAULT_DIFFICULTY + 1,
        8,
        4,
        0,
    )
    assert rows[3] == (
        s.parent_height + 3,
        "block-c",
        "block-b",
        s.first_ts + s.dt * 2,
        s.DEFAULT_DIFFICULTY,
        0,
        0,
        0,
    )


def assert_transactions(cur: pg.Cursor, s: Scenario):
    # 5 txs: 1 bootstrap + 4 from blocks
    cur.execute(
        "select height, header_id, index, id from core.transactions order by 1, 3;"
    )
    rows = cur.fetchall()
    assert len(rows) == 5
    bootstrap_tx_id = GENESIS_ID if s.parent_height == 0 else "bootstrap-tx"
    assert rows[0] == (s.parent_height, GENESIS_ID, 0, bootstrap_tx_id)
    assert rows[1] == (s.parent_height + 1, "block-a", 0, s.id("tx-a1"))
    assert rows[2] == (s.parent_height + 2, "block-b", 0, s.id("tx-b1"))
    assert rows[3] == (s.parent_height + 3, "block-c", 0, s.id("tx-c1"))
    assert rows[4] == (s.parent_height + 3, "block-c", 1, s.id("tx-c2"))


def assert_addresses(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select id
            , address
            , spot_height
            , p2pk
            , miner
        from core.addresses
        order by 1;
    """
    )
    rows = cur.fetchall()
    assert len(rows) == 7
    assert rows[0] == (1, s.address("base"), s.parent_height + 0, False, False)
    assert rows[1] == (2, s.address("tres"), s.parent_height + 1, False, False)
    assert rows[2] == (3, s.address("con1"), s.parent_height + 1, False, False)
    assert rows[3] == (4, s.address("con2"), s.parent_height + 2, False, False)
    assert rows[4] == (5, s.address("pub1"), s.parent_height + 2, True, False)
    assert rows[5] == (6, s.address("pub2"), s.parent_height + 3, True, False)
    assert rows[6] == (7, s.address("fees"), s.parent_height + 3, False, False)


def assert_inputs(cur: pg.Cursor, s: Scenario):
    # 4 inputs
    cur.execute(
        "select header_id, tx_id, index, box_id from core.inputs order by 1, 2;"
    )
    rows = cur.fetchall()
    assert len(rows) == 4
    assert rows[0] == ("block-a", s.id("tx-a1"), 0, s.id("base-box1"))
    assert rows[1] == ("block-b", s.id("tx-b1"), 0, s.id("con1-box1"))
    assert rows[2] == ("block-c", s.id("tx-c1"), 0, s.id("pub1-box1"))
    assert rows[3] == ("block-c", s.id("tx-c2"), 0, s.id("fees-box1"))


def assert_data_inputs(cur: pg.Cursor, s: Scenario):
    # 1 data-inputs
    cur.execute(
        "select header_id, tx_id, index, box_id from core.data_inputs order by 1, 2;"
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0] == ("block-c", s.id("tx-c1"), 0, s.id("con2-box1"))


def assert_outputs(cur: pg.Cursor, s: Scenario):
    # 9 outputs: 1 bootstrap + 8 from blocks
    cur.execute(
        """
        select creation_height
            , o.header_id
            , o.tx_id
            , o.index
            , o.box_id
            , o.value
            , a.address
            , o.size is not null and o.size >= 63 and o.size <= 268
        from core.outputs o
        join core.addresses a on a.id = o.address_id
        order by creation_height, tx_id, index;
    """
    )
    rows = cur.fetchall()
    assert len(rows) == 10
    bootstrap_tx_id = GENESIS_ID if s.parent_height == 0 else "bootstrap-tx"
    assert rows[0] == (
        s.parent_height + 0,
        GENESIS_ID,
        bootstrap_tx_id,
        0,
        s.id("base-box1"),
        1000,
        s.address("base-box1"),
        True,
    )
    assert rows[1] == (
        s.parent_height + 1,
        "block-a",
        s.id("tx-a1"),
        0,
        s.id("tres-box1"),
        0,
        s.address("tres"),
        True,
    )
    assert rows[2] == (
        s.parent_height + 1,
        "block-a",
        s.id("tx-a1"),
        1,
        s.id("base-box2"),
        950,
        s.address("base-box2"),
        True,
    )
    assert rows[3] == (
        s.parent_height + 1,
        "block-a",
        s.id("tx-a1"),
        2,
        s.id("con1-box1"),
        50,
        s.address("con1-box1"),
        True,
    )
    assert rows[4] == (
        s.parent_height + 2,
        "block-b",
        s.id("tx-b1"),
        0,
        s.id("con2-box1"),
        40,
        s.address("con2-box1"),
        True,
    )
    assert rows[5] == (
        s.parent_height + 2,
        "block-b",
        s.id("tx-b1"),
        1,
        s.id("pub1-box1"),
        10,
        s.address("pub1-box1"),
        True,
    )
    assert rows[6] == (
        s.parent_height + 3,
        "block-c",
        s.id("tx-c1"),
        0,
        s.id("pub1-box2"),
        5,
        s.address("pub1-box2"),
        True,
    )
    assert rows[7] == (
        s.parent_height + 3,
        "block-c",
        s.id("tx-c1"),
        1,
        s.id("pub2-box1"),
        4,
        s.address("pub2-box1"),
        True,
    )
    assert rows[8] == (
        s.parent_height + 3,
        "block-c",
        s.id("tx-c1"),
        2,
        s.id("fees-box1"),
        1,
        s.address("fees-box1"),
        True,
    )
    assert rows[9] == (
        s.parent_height + 3,
        "block-c",
        s.id("tx-c2"),
        0,
        s.id("con1-box2"),
        1,
        s.address("con1-box2"),
        True,
    )


def assert_box_registers(cur: pg.Cursor, s: Scenario):
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
        s.id("pub1-box1"),
        "SGroupElement",
        "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
        "03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
    )
    assert rows[1] == (
        5,
        s.id("pub1-box1"),
        "Coll[SByte]",
        "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
        "98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
    )
    assert rows[2] == (
        6,
        s.id("pub1-box1"),
        "SLong",
        "05a4c3edd9998877",
        "261824656027858",
    )


def assert_tokens(cur: pg.Cursor, s: Scenario):
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
    assert rows[0] == (
        s.id("con1-box1"),
        s.id("pub1-box1"),
        2000,
        None,
        None,
        None,
        None,
    )


def assert_box_assets(cur: pg.Cursor, s: Scenario):
    # 3 boxes containing some tokens
    cur.execute(
        """
        select box_id
            , token_id
            , amount
        from core.box_assets
        order by amount desc;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 3
    assert rows[0] == (s.id("pub1-box1"), s.id("con1-box1"), 2000)
    assert rows[1] == (s.id("pub1-box2"), s.id("con1-box1"), 1500)
    assert rows[2] == (s.id("pub2-box1"), s.id("con1-box1"), 500)


def assert_system_parameters(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select height
            , storage_fee
            , min_box_value
            , max_block_size
            , max_cost
            , token_access_cost
            , tx_input_cost
            , tx_data_input_cost
            , tx_output_cost
            , block_version
        from core.system_parameters
        order by height;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0] == (s.parent_height + 2, 1, 2, 3, 4, 5, 6, 7, 8, 2)


def assert_unhandled_extension_fields(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select height
            , key
            , value
        from core.unhandled_extension_fields
        order by key;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 5
    assert rows[0] == (s.parent_height + 2, "0078", "should-be-unhandled-120")
    assert rows[1] == (s.parent_height + 2, "0079", "should-be-unhandled-121")
    assert rows[2] == (s.parent_height + 2, "007a", "should-be-unhandled-122")
    assert rows[3] == (s.parent_height + 2, "007c", "should-be-unhandled-124")
    assert rows[4] == (s.parent_height + 2, "0201", "should-be-unhandled-too")

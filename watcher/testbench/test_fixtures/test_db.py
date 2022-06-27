import pytest
import psycopg as pg

from fixtures.db import bootstrap_db
from fixtures.db import fill_rev0_db
from fixtures.db import temp_db_class_scoped
from fixtures.db import unconstrained_db_class_scoped
from fixtures.db import temp_db_rev0_class_scoped
from fixtures.db.sql import BOOTSTRAP_TX_ID
from fixtures.db.sql import extract_existing_header
from fixtures.db.sql import extract_existing_transaction
from fixtures.db.sql import extract_existing_outputs
from fixtures.db.sql import extract_existing_tokens
from fixtures.scenario import Scenario
from fixtures.scenario.genesis import GENESIS_ID
from fixtures.registers import RegisterCatalogue as RC
from utils import table_has_pk


@pytest.mark.order(3)
class TestGenesisDB:
    @pytest.fixture(scope="class")
    def cur(self, unconstrained_db_class_scoped):
        with pg.connect(unconstrained_db_class_scoped) as conn:
            with conn.cursor() as cur:
                yield cur

    def test_db_is_empty(self, cur):
        cur.execute("select count(*) as cnt from core.outputs;")
        row = cur.fetchone()
        assert row[0] == 0

    def test_core_constraints_not_set(self, cur):
        assert table_has_pk(cur.connection, "core", "headers") == False


@pytest.mark.order(3)
class TestPopulatedDB:
    @pytest.fixture(scope="class")
    def scenario(self):
        desc = """
        block-a
            base-box1 1000
            >
            base-box2  950
            con1-box1   50
            --
            con2-box1 1000 (token-1: 1)
            pub1-box1 1000
            {pub9-box1}
            >
            con2-box2 1000 (token-1: 1)
            pub1-box2  900
            pub2-box1   99
            fees-box1    1
            --
            fees-box1    1
            >
            con1-box2    1

        block-b
            pub2-box1  99
            >
            pub2-box2   99
        """
        return Scenario(desc, 599_999, 1234560100000)

    @pytest.fixture(scope="class")
    def cur(self, temp_db_class_scoped, scenario):
        with pg.connect(temp_db_class_scoped) as conn:
            bootstrap_db(conn, scenario)
            with conn.cursor() as cur:
                yield cur

    def test_core_constraints_are_set(self, cur):
        assert table_has_pk(cur.connection, "core", "headers") == True

    def test_db_state_core(self, cur, scenario):
        """Check core tables state"""
        # Single header, height set to previous block
        cur.execute("select height, id from core.headers;")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (599_999, GENESIS_ID)

        # Single tx
        cur.execute("select height, id from core.transactions;")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (599_999, "bootstrap-tx")

        # 5 pre-existing outputs
        cur.execute("select creation_height, value, box_id from core.outputs;")
        rows = cur.fetchall()
        assert len(rows) == 5
        # All height at 599,999
        assert set([r[0] for r in rows]) == {599_999}
        # All values at 1000
        assert set([r[1] for r in rows]) == {1000}
        # Box id's
        assert [r[2] for r in rows] == [
            scenario.id("base-box1"),
            scenario.id("con2-box1"),
            scenario.id("pub1-box1"),
            scenario.id("pub9-box1"),
            "dummy-token-box-id-1",
        ]

        # No inputs (impossible in real life, but ok here)
        cur.execute("select count(*) from core.inputs;")
        assert cur.fetchone()[0] == 0

        # No data-inputs
        cur.execute("select count(*) from core.data_inputs;")
        assert cur.fetchone()[0] == 0

        # 1 pre-existing token
        cur.execute("select id, box_id, emission_amount from core.tokens;")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (scenario.id("token-1"), "dummy-token-box-id-1", 5000)

        # No assets
        cur.execute("select count(*) from core.box_assets;")
        assert cur.fetchone()[0] == 0

    def test_db_state_usp(self, cur, scenario):
        cur.execute("select box_id from usp.boxes;")
        box_ids = [r[0] for r in cur.fetchall()]
        assert len(box_ids) == 5
        assert scenario.id("base-box1") in box_ids
        assert scenario.id("con2-box1") in box_ids
        assert scenario.id("pub9-box1") in box_ids
        assert "dummy-token-box-id-1" in box_ids
        assert scenario.id("pub1-box1") in box_ids

    def test_db_state_bal(self, cur, scenario):
        # Erg diffs
        cur.execute("select height, value, address from bal.erg_diffs;")
        rows = cur.fetchall()
        assert len(rows) == 5
        assert rows == [
            (599_999, 1000, scenario.address("base")),
            (599_999, 1000, scenario.address("con2")),
            (599_999, 1000, scenario.address("pub1")),
            (599_999, 1000, "dummy-data-input-box-address"),
            (599_999, 1000, "dummy-token-minting-address"),
        ]

        # Erg balances
        cur.execute("select value, address from bal.erg;")
        rows = cur.fetchall()
        assert len(rows) == 5
        assert rows == [
            (1000, scenario.address("base")),
            (1000, scenario.address("con2")),
            (1000, scenario.address("pub1")),
            (1000, "dummy-data-input-box-address"),
            (1000, "dummy-token-minting-address"),
        ]


@pytest.mark.order(3)
class TestRev0DB:
    @pytest.fixture(scope="class")
    def scenario(self):
        desc = """
        block-a
            base-box1 1000
            >
            base-box2  950
            con1-box1   50
            --
            con2-box1 1000
            pub1-box1 1000
            >
            con2-box2 1000 (con2-box1: 50) [
                0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8,
                0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8,
                05a4c3edd9998877
            ]
            pub1-box2  999
            fees-box1    1

        //----------------fork-----------------
        block-x
            base-box1 1000
            >
            base-box3  950
            con1-box2   50
        //-------------------------------------

        block-b-a
            pub1-box2  999
            >
            pub2-box1  999

        
        """
        return Scenario(desc, 599_999, 1234560100000, main_only=True)

    @pytest.fixture(scope="class")
    def blocks(self):
        """
        block a:
            base-box1 1000 --> base-box2  950
                               con1-box1   50

            con2-box1 1000 --> con2-box2 1000 (con2-box1 50)
            pub1-box1 1000     pub1-box2  999
                               fees-box1    1

        ----------------fork-----------------
        block x:
            base-box1 1000 --> base-box2  950
                               con1-box1   50
        -------------------------------------

        block b:
            pub1-box2  999 --> pub2-box1  999
        """
        con1 = AC.get("con1")
        con2 = AC.get("con2")
        pub1 = AC.get("pub1")
        pub2 = AC.get("pub2")

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
                    "ergoTree": AC.coinbase.ergo_tree,
                    "assets": [],
                    "creationHeight": 600000,
                    "additionalRegisters": {},
                    "transactionId": "tx-a1",
                    "index": 0,
                },
                {
                    "boxId": "con1-box1",
                    "value": 50,
                    "ergoTree": con1.ergo_tree,
                    "assets": [],
                    "creationHeight": 600000,
                    "additionalRegisters": {},
                    "transactionId": "tx-a1",
                    "index": 1,
                },
            ],
            "size": 344,
        }

        tx_a2 = {
            "id": "tx-a2",
            "inputs": [
                {
                    "boxId": "con2-box1",
                },
                {
                    "boxId": "pub1-box1",
                },
            ],
            "dataInputs": [],
            "outputs": [
                {
                    "boxId": "con2-box2",
                    "value": 1000,
                    "ergoTree": con2.ergo_tree,
                    "assets": [
                        {
                            "tokenId": "con2-box1",
                            "amount": 50,
                        }
                    ],
                    "creationHeight": 599998,
                    "additionalRegisters": {
                        "R4": "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
                        "R5": "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
                        "R6": "05a4c3edd9998877",
                    },
                    "transactionId": "tx-a2",
                    "index": 0,
                },
                {
                    "boxId": "pub1-box2",
                    "value": 999,
                    "ergoTree": pub1.ergo_tree,
                    "assets": [],
                    "creationHeight": 599998,
                    "additionalRegisters": {},
                    "transactionId": "tx-a2",
                    "index": 1,
                },
                {
                    "boxId": "fees-box1",
                    "value": 1,
                    "ergoTree": AC.fees.ergo_tree,
                    "assets": [],
                    "creationHeight": 599998,
                    "additionalRegisters": {},
                    "transactionId": "tx-a2",
                    "index": 2,
                },
            ],
            "size": 674,
        }

        tx_b1 = {
            "id": "tx-b1",
            "inputs": [
                {
                    "boxId": "pub1-box2",
                }
            ],
            "dataInputs": [],
            "outputs": [
                {
                    "boxId": "pub2-box1",
                    "value": 999,
                    "ergoTree": pub2.ergo_tree,
                    "assets": [],
                    "creationHeight": 600001,
                    "additionalRegisters": {},
                    "transactionId": "tx-b1",
                    "index": 0,
                }
            ],
            "size": 100,
        }

        block_a = {
            "header": {
                "votes": "000000",
                "timestamp": 1234560100000,
                "height": 600000,
                "id": "block-a",
                "parentId": "parent-of-block-a",
            },
            "blockTransactions": {
                "headerId": "block-a",
                "transactions": [tx_a1, tx_a2],
                "blockVersion": 2,
                "size": 1155,
            },
        }

        block_x = {
            "header": {
                "votes": "000000",
                "timestamp": 1234560100000,
                "height": 600000,
                "id": "block-x",
                "parentId": "parent-of-block-a",
            },
            "blockTransactions": {
                "headerId": "block-a",
                "transactions": [tx_a1],
                "blockVersion": 2,
                "size": 1155,
            },
        }

        block_b = {
            "header": {
                "votes": "000000",
                "timestamp": 1234560200000,
                "height": 600001,
                "id": "block-b",
                "parentId": "block-a",
            },
            "blockTransactions": {
                "headerId": "block-b",
                "transactions": [tx_b1],
                "blockVersion": 2,
                "size": 1155,
            },
        }

        return [block_a, block_x, block_b]

    @pytest.fixture(scope="class")
    def cur(self, temp_db_rev0_class_scoped, scenario):
        with pg.connect(temp_db_rev0_class_scoped) as conn:
            fill_rev0_db(conn, scenario)
            with conn.cursor() as cur:
                yield cur

    def test_core_constraints_are_set(self, cur):
        assert table_has_pk(cur.connection, "core", "headers") == True

    def test_core_headers(self, cur):
        """Check core tables state"""
        # Headers
        cur.execute("select height, id from core.headers;")
        rows = cur.fetchall()
        assert len(rows) == 3
        assert rows[0] == (599_999, GENESIS_ID)
        assert rows[1] == (600_000, "block-a")
        assert rows[2] == (600_001, "block-b")

    def test_core_transactions(self, cur, scenario):
        cur.execute("select height, id from core.transactions;")
        rows = cur.fetchall()
        assert len(rows) == 4
        assert rows[0] == (599_999, "bootstrap-tx")
        assert rows[1] == (600_000, scenario.id("tx-a1"))
        assert rows[2] == (600_000, scenario.id("tx-a2"))
        assert rows[3] == (600_001, scenario.id("tx-b1"))

    def test_core_outputs(self, cur, scenario):
        cur.execute(
            """
            select header_id
                , creation_height
                , tx_id
                , index
                , box_id
                , value
                , address
            from core.outputs order by creation_height, tx_id, index;
            """
        )
        rows = cur.fetchall()
        assert len(rows) == 9
        assert rows[0] == (
            GENESIS_ID,
            599_999,
            BOOTSTRAP_TX_ID,
            0,
            scenario.id("base-box1"),
            1000,
            scenario.address("base-box1"),
        )
        assert rows[1] == (
            GENESIS_ID,
            599_999,
            BOOTSTRAP_TX_ID,
            1,
            scenario.id("con2-box1"),
            1000,
            scenario.address("con2-box1"),
        )
        assert rows[2] == (
            GENESIS_ID,
            599_999,
            BOOTSTRAP_TX_ID,
            2,
            scenario.id("pub1-box1"),
            1000,
            scenario.address("pub1-box1"),
        )
        assert rows[3] == (
            "block-a",
            600_000,
            scenario.id("tx-a1"),
            0,
            scenario.id("base-box2"),
            950,
            scenario.address("base-box2"),
        )
        assert rows[4] == (
            "block-a",
            600_000,
            scenario.id("tx-a1"),
            1,
            scenario.id("con1-box1"),
            50,
            scenario.address("con1-box1"),
        )
        assert rows[5] == (
            "block-a",
            600_000,
            scenario.id("tx-a2"),
            0,
            scenario.id("con2-box2"),
            1000,
            scenario.address("con2-box2"),
        )
        assert rows[6] == (
            "block-a",
            600_000,
            scenario.id("tx-a2"),
            1,
            scenario.id("pub1-box2"),
            999,
            scenario.address("pub1-box2"),
        )
        assert rows[7] == (
            "block-a",
            600_000,
            scenario.id("tx-a2"),
            2,
            scenario.id("fees-box1"),
            1,
            scenario.address("fees-box1"),
        )
        assert rows[8] == (
            "block-b",
            600_001,
            scenario.id("tx-b1"),
            0,
            scenario.id("pub2-box1"),
            999,
            scenario.address("pub2-box1"),
        )

    def test_core_inputs(self, cur, scenario):
        cur.execute("select tx_id, index, box_id from core.inputs order by 1, 2;")
        rows = cur.fetchall()
        assert len(rows) == 4
        assert rows[0] == (scenario.id("tx-a1"), 0, scenario.id("base-box1"))
        assert rows[1] == (scenario.id("tx-a2"), 0, scenario.id("con2-box1"))
        assert rows[2] == (scenario.id("tx-a2"), 1, scenario.id("pub1-box1"))
        assert rows[3] == (scenario.id("tx-b1"), 0, scenario.id("pub1-box2"))

    def test_core_tokens(self, cur, scenario):
        cur.execute(
            """
            select id
                , box_id
                , emission_amount
                , decimals
            from core.tokens order by 1;
            """
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (scenario.id("con2-box1"), scenario.id("con2-box2"), 50, None)

    def test_core_assets(self, cur, scenario):
        cur.execute(
            """
            select box_id
                , token_id
                , amount
            from core.box_assets order by 1;
            """
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (scenario.id("con2-box2"), scenario.id("con2-box1"), 50)

    def test_core_registers(self, cur, scenario):
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

        raw = "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"
        assert rows[0] == (
            4,
            scenario.id("con2-box2"),
            "SGroupElement",
            raw,
            "03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
        )

        raw = "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"
        assert rows[1] == (
            5,
            scenario.id("con2-box2"),
            "Coll[SByte]",
            raw,
            "98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
        )

        raw = "05a4c3edd9998877"
        assert rows[2] == (
            6,
            scenario.id("con2-box2"),
            "SLong",
            raw,
            "261824656027858",
        )

    def test_usp(self, cur, scenario):
        cur.execute("select box_id from usp.boxes;")
        box_ids = [r[0] for r in cur.fetchall()]
        assert len(box_ids) == 5
        assert scenario.id("base-box2") in box_ids
        assert scenario.id("con1-box1") in box_ids
        assert scenario.id("con2-box2") in box_ids
        assert scenario.id("fees-box1") in box_ids
        assert scenario.id("pub2-box1") in box_ids

    def test_bal_erg_diffs(self, cur, scenario):
        cur.execute("select height, tx_id, value, address from bal.erg_diffs;")
        rows = cur.fetchall()
        assert len(rows) == 9
        assert (599_999, BOOTSTRAP_TX_ID, 1000, scenario.address("base")) in rows
        assert (599_999, BOOTSTRAP_TX_ID, 1000, scenario.address("con2")) in rows
        assert (599_999, BOOTSTRAP_TX_ID, 1000, scenario.address("pub1")) in rows
        assert (600_000, scenario.id("tx-a1"), -50, scenario.address("base")) in rows
        assert (600_000, scenario.id("tx-a1"), 50, scenario.address("con1")) in rows
        assert (600_000, scenario.id("tx-a2"), 1, scenario.address("fees")) in rows
        assert (600_000, scenario.id("tx-a2"), -1, scenario.address("pub1")) in rows
        assert (600_001, scenario.id("tx-b1"), -999, scenario.address("pub1")) in rows
        assert (600_001, scenario.id("tx-b1"), 999, scenario.address("pub2")) in rows

    def test_bal_erg(self, cur, scenario):
        cur.execute("select value, address from bal.erg order by 2;")
        rows = cur.fetchall()
        assert len(rows) == 5
        assert rows == [
            (1, scenario.address("fees")),
            (950, scenario.address("base")),
            (50, scenario.address("con1")),
            (1000, scenario.address("con2")),
            (999, scenario.address("pub2")),
        ]

    def test_bal_tokens_diffs(self, cur, scenario):
        cur.execute(
            "select height, token_id, tx_id, value, address from bal.tokens_diffs;"
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows == [
            (
                600_000,
                scenario.id("con2-box1"),
                scenario.id("tx-a2"),
                50,
                scenario.address("con2"),
            ),
        ]

    def test_bal_tokens(self, cur, scenario):
        cur.execute("select value, token_id, address from bal.tokens order by 2;")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows == [
            (50, scenario.id("con2-box1"), scenario.address("con2")),
        ]


@pytest.mark.order(2)
class TestHelpers:
    @pytest.fixture
    def scenario(self):
        desc = """
        block-a
            base-box1 1000
            >
            base-box2  950
            con1-box1   50
            -- 
            con2-box1 1000
            pub1-box1 1000
            {pub9-box1}
            >
            con2-box2 1000 (token-1: 1)
            pub1-box2  900
            pub2-box1   99
            fees-box1    1
            --
            fees-box1    1
            >
            con1-box2    1

        block-b
            pub2-box1  99
            >
            pub2-box2   99
        """
        return Scenario(desc, 599_999, 1234560100000)

    def test_extract_existing_header(self, scenario):
        header = extract_existing_header(scenario.blocks)
        assert header.height == 599_999
        assert header.id == scenario.blocks[0]["header"]["parentId"]
        assert header.parent_id == "bootstrap-parent-header-id"
        assert header.timestamp == 1234560100000 - 100_000

    def test_extract_existing_transaction(self, scenario):
        tx = extract_existing_transaction(scenario.blocks)
        assert tx.id == "bootstrap-tx"
        assert tx.header_id == scenario.blocks[0]["header"]["parentId"]
        assert tx.height == 599_999
        assert tx.index == 0

    def test_extract_existing_tokens(self, scenario):
        tokens = extract_existing_tokens(scenario.blocks)
        assert len(tokens) == 1
        t = tokens[0]
        assert t.id == scenario.id("token-1")
        assert t.box_id == "dummy-token-box-id-1"
        assert t.emission_amount == 5000
        assert t.name == None
        assert t.description == None
        assert t.decimals == None
        assert t.standard == None

    def test_extract_existing_outputs(self, scenario):
        boxes = extract_existing_outputs(scenario)
        genesis_header_id = scenario.blocks[0]["header"]["parentId"]
        assert len(boxes) == 5

        box = boxes[0]
        assert box.box_id == scenario.id("base-box1")
        assert box.header_id == genesis_header_id
        assert box.creation_height == 599_999
        assert box.address == scenario.address("base-box1")
        assert box.index == 0
        assert box.value == 1000

        box = boxes[1]
        assert box.box_id == scenario.id("con2-box1")
        assert box.header_id == genesis_header_id
        assert box.creation_height == 599_999
        assert box.address == scenario.address("con2-box1")
        assert box.index == 1
        assert box.value == 1000

        box = boxes[2]
        assert box.box_id == scenario.id("pub1-box1")
        assert box.header_id == genesis_header_id
        assert box.creation_height == 599_999
        assert box.address == scenario.address("pub1-box1")
        assert box.index == 2
        assert box.value == 1000

        box = boxes[3]
        assert box.box_id == scenario.id("pub9-box1")
        assert box.header_id == genesis_header_id
        assert box.creation_height == 599_999
        assert box.address == "dummy-data-input-box-address"
        assert box.index == 3
        assert box.value == 1000

        box = boxes[4]
        assert box.box_id == "dummy-token-box-id-1"
        assert box.header_id == genesis_header_id
        assert box.creation_height == 599_999
        assert box.address == "dummy-token-minting-address"
        assert box.index == 4
        assert box.value == 1000

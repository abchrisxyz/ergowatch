import pytest
import psycopg as pg

from fixtures.db2 import bootstrap_db
from fixtures.db2 import temp_db_class_scoped
from fixtures.db2 import unconstrained_db_class_scoped
from fixtures.db2.sql import extract_existing_header
from fixtures.db2.sql import extract_existing_transaction
from fixtures.db2.sql import extract_existing_outputs
from fixtures.db2.sql import extract_existing_tokens
from fixtures.addresses import AddressCatalogue as AC


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

    def test_constraints_flag_not_set(self, cur):
        cur.execute("select constraints_set from ew.revision;")
        row = cur.fetchone()
        assert row[0] == False


@pytest.mark.order(3)
class TestPopulatedDB:
    @pytest.fixture(scope="class")
    def blocks(self):
        """
        block a:
            base-box1 1000 --> base-box2  950
                               con1-box1   50

            con2-box1 1000 --> con2-box2 1000
                 token-1 1          token-1 1
            pub1-box1 1000     pub1-box2  900
                               pub2-box1   99
                               fees-box1    1

            fees-box1    1 --> con1-box2    1

        block b:
            pub2-box1  99  --> pub2-box2   99
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
            "dataInputs": [{"boxId": "data-input-1"}],
            "outputs": [
                {
                    "boxId": "con2-box2",
                    "value": 1000,
                    "ergoTree": con2.ergo_tree,
                    "assets": [
                        {
                            "tokenId": "token-1",
                            "amount": 1,
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

        tx_a3 = {
            "id": "tx-a3",
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
                    "creationHeight": 60000,
                    "additionalRegisters": {},
                    "transactionId": "tx-a3",
                    "index": 0,
                }
            ],
            "size": 100,
        }

        tx_b1 = {
            "id": "tx-b1",
            "inputs": [
                {
                    "boxId": "fees-box1",
                }
            ],
            "dataInputs": [],
            "outputs": [
                {
                    "boxId": "pub2-box2",
                    "value": 1,
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
                "transactions": [tx_a1, tx_a2, tx_a3],
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

        return [block_a, block_b]

    @pytest.fixture(scope="class")
    def cur(self, temp_db_class_scoped, blocks):
        with pg.connect(temp_db_class_scoped) as conn:
            bootstrap_db(conn, blocks)
            with conn.cursor() as cur:
                yield cur

    def test_constraints_flag_is_set(self, cur):
        cur.execute("select constraints_set from ew.revision;")
        row = cur.fetchone()
        assert row[0] == True

    def test_db_state_core(self, cur):
        """Check core tables state"""
        # Single header, height set to previous block
        cur.execute("select height, id from core.headers;")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (599_999, "parent-of-block-a")

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
            "base-box1",
            "con2-box1",
            "pub1-box1",
            "data-input-1",
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
        assert rows[0] == ("token-1", "dummy-token-box-id-1", 5000)

        # No assets
        cur.execute("select count(*) from core.box_assets;")
        assert cur.fetchone()[0] == 0

    @pytest.mark.skip("TODO")
    def test_db_state_usp(self, cur):
        pass

    def test_db_state_bal(self, cur):
        # Erg diffs
        cur.execute("select height, value, address from bal.erg_diffs;")
        rows = cur.fetchall()
        assert len(rows) == 5
        assert rows == [
            (599_999, 1000, AC.get("base").address),
            (599_999, 1000, AC.get("con2").address),
            (599_999, 1000, AC.get("pub1").address),
            (599_999, 1000, "dummy-data-input-box-address"),
            (599_999, 1000, "dummy-token-minting-address"),
        ]

        # Erg balances
        cur.execute("select value, address from bal.erg;")
        rows = cur.fetchall()
        assert len(rows) == 5
        assert rows == [
            (1000, AC.get("base").address),
            (1000, AC.get("con2").address),
            (1000, AC.get("pub1").address),
            (1000, "dummy-data-input-box-address"),
            (1000, "dummy-token-minting-address"),
        ]


@pytest.mark.order(2)
class TestHelpers:
    @pytest.fixture
    def blocks(self):
        """
        block a:
            base-box1 1000 --> base-box2  950
                               con1-box1   50

            con2-box1 1000 --> con2-box2 1000
            pub1-box1 1000     pub1-box2  900
                               pub2-box1   99
                               fees-box1    1

            fees-box1    1 --> con1-box2    1

        block b:
            pub2-box1  99  --> pub2-box2   99
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
            "dataInputs": [{"boxId": "data-input-1"}],
            "outputs": [
                {
                    "boxId": "con2-box2",
                    "value": 1000,
                    "ergoTree": con2.ergo_tree,
                    "assets": [
                        {
                            "tokenId": "token-1",
                            "amount": 1,
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

        tx_a3 = {
            "id": "tx-a3",
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
                    "creationHeight": 60000,
                    "additionalRegisters": {},
                    "transactionId": "tx-a3",
                    "index": 0,
                }
            ],
            "size": 100,
        }

        tx_b1 = {
            "id": "tx-b1",
            "inputs": [
                {
                    "boxId": "fees-box1",
                }
            ],
            "dataInputs": [],
            "outputs": [
                {
                    "boxId": "pub2-box2",
                    "value": 1,
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
                "transactions": [tx_a1, tx_a2, tx_a3],
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

        return [block_a, block_b]

    def test_extract_existing_header(self, blocks):
        header = extract_existing_header(blocks)
        assert header.height == 599_999
        assert header.id == "parent-of-block-a"
        assert header.parent_id == "bootstrap-parent-header-id"
        assert header.timestamp == 1234560100000 - 100_000

    def test_extract_transaction(self, blocks):
        tx = extract_existing_transaction(blocks)
        assert tx.id == "bootstrap-tx"
        assert tx.header_id == "parent-of-block-a"
        assert tx.height == 599_999
        assert tx.index == 0

    def test_extract_tokens(self, blocks):
        tokens = extract_existing_tokens(blocks)
        assert len(tokens) == 1
        t = tokens[0]
        assert t.id == "token-1"
        assert t.box_id == "dummy-token-box-id-1"
        assert t.emission_amount == 5000
        assert t.name == "name"
        assert t.description == "description"
        assert t.decimals == 0
        assert t.standard == "dummy-std"

    def test_extract_outputs(self, blocks):
        boxes = extract_existing_outputs(blocks)
        assert len(boxes) == 5

        box = boxes[0]
        assert box.box_id == "base-box1"
        assert box.header_id == "parent-of-block-a"
        assert box.creation_height == 599_999
        assert box.address == AC.coinbase.address
        assert box.index == 0
        assert box.value == 1000

        box = boxes[1]
        assert box.box_id == "con2-box1"
        assert box.header_id == "parent-of-block-a"
        assert box.creation_height == 599_999
        assert box.address == AC.get("con2").address
        assert box.index == 1
        assert box.value == 1000

        box = boxes[2]
        assert box.box_id == "pub1-box1"
        assert box.header_id == "parent-of-block-a"
        assert box.creation_height == 599_999
        assert box.address == AC.get("pub1").address
        assert box.index == 2
        assert box.value == 1000

        box = boxes[3]
        assert box.box_id == "data-input-1"
        assert box.header_id == "parent-of-block-a"
        assert box.creation_height == 599_999
        assert box.address == "dummy-data-input-box-address"
        assert box.index == 3
        assert box.value == 1000

        box = boxes[4]
        assert box.box_id == "dummy-token-box-id-1"
        assert box.header_id == "parent-of-block-a"
        assert box.creation_height == 599_999
        assert box.address == "dummy-token-minting-address"
        assert box.index == 4
        assert box.value == 1000

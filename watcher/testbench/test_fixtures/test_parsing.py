import pytest
from fixtures.syntax import parse
from fixtures.api import GENESIS_ID
from fixtures.addresses import AddressCatalogue as AC


@pytest.mark.order(1)
class TestParsing:
    """
    Test test description parsing
    """

    @pytest.fixture
    def desc(self):
        return """
            // comments
            block-a
            pub1-box1  100 (tokenx: 20, tokeny: 3)
            con1-box1  100 // inline comment
            >
            cex1-box1   10
            pub1-box2  190 (tokenx: 20, tokeny: 1)

            -- // Multiple txs are separated by 2 dashes
            pub1-box2 190
            >
            pub2-box1 189
            fees-box1   1

            // This block is not part of main chain
            block-x
            pub2-box1 189
            >
            pub3-box1 189

            // Child of block-a
            block-b-a
            pub2-box1 189
            >
            pub4-box1 189

            block-c
            pub4-box1 189
            {pub3-box1} // data input
            >
            pub5-box1 189
        """

    def test_block_headers(self, desc):
        start_height = 100
        d = parse(desc, start_height)
        assert len(d) == 4
        assert d[0]["header"] == {
            "votes": "000000",
            "timestamp": 1234560000000,
            "size": 123,
            "height": start_height,
            "id": "block-a",
            "parentId": GENESIS_ID,
        }

        assert d[1]["header"] == {
            "votes": "000000",
            "timestamp": 1234560000000 + 100000,
            "size": 123,
            "height": start_height + 1,
            "id": "block-x",
            "parentId": "block-a",
        }

        assert d[2]["header"] == {
            "votes": "000000",
            "timestamp": 1234560000000 + 100000,
            "size": 123,
            "height": start_height + 1,
            "id": "block-b",
            "parentId": "block-a",
        }

        assert d[3]["header"] == {
            "votes": "000000",
            "timestamp": 1234560000000 + 100000 * 2,
            "size": 123,
            "height": start_height + 2,
            "id": "block-c",
            "parentId": "block-b",
        }

    def test_block_transactions(self, desc):
        start_height = 100
        d = parse(desc, start_height)
        assert len(d) == 4

        assert d[0]["blockTransactions"] == {
            "blockVersion": 2,
            "headerId": "block-a",
            "size": 234,
            "transactions": [
                {
                    "dataInputs": [],
                    "id": "tx-a1",
                    "inputs": [{"boxId": "con1-box1"}],
                    "outputs": [
                        {
                            "additionalRegisters": {},
                            "assets": [],
                            "boxId": "cex1-box1",
                            "creationHeight": 100,
                            "ergoTree": AC.boxid2box("cex1-box1").ergo_tree,
                            "index": 0,
                            "transactionId": "tx-a1",
                            "value": 10,
                        },
                        {
                            "additionalRegisters": {},
                            "assets": [
                                {"amount": 20, "tokenId": "tokenx"},
                                {"amount": 1, "tokenId": "tokeny"},
                            ],
                            "boxId": "pub1-box2",
                            "creationHeight": 100,
                            "ergoTree": AC.boxid2box("pub1-box2").ergo_tree,
                            "index": 1,
                            "transactionId": "tx-a1",
                            "value": 190,
                        },
                    ],
                    "size": 344,
                },
                {
                    "dataInputs": [],
                    "id": "tx-a2",
                    "inputs": [{"boxId": "pub1-box2"}],
                    "outputs": [
                        {
                            "additionalRegisters": {},
                            "assets": [],
                            "boxId": "pub2-box1",
                            "creationHeight": 100,
                            "ergoTree": AC.boxid2box("pub2-box1").ergo_tree,
                            "index": 0,
                            "transactionId": "tx-a2",
                            "value": 189,
                        },
                        {
                            "additionalRegisters": {},
                            "assets": [],
                            "boxId": "fees-box1",
                            "creationHeight": 100,
                            "ergoTree": AC.boxid2box("fees-box1").ergo_tree,
                            "index": 1,
                            "transactionId": "tx-a2",
                            "value": 1,
                        },
                    ],
                    "size": 344,
                },
            ],
        }

        assert d[1]["blockTransactions"] == {
            "blockVersion": 2,
            "headerId": "block-x",
            "size": 234,
            "transactions": [
                {
                    "dataInputs": [],
                    "id": "tx-x1",
                    "inputs": [{"boxId": "pub2-box1"}],
                    "outputs": [
                        {
                            "additionalRegisters": {},
                            "assets": [],
                            "boxId": "pub3-box1",
                            "creationHeight": 101,
                            "ergoTree": AC.boxid2box("pub3-box1").ergo_tree,
                            "index": 0,
                            "transactionId": "tx-x1",
                            "value": 189,
                        }
                    ],
                    "size": 344,
                }
            ],
        }

        assert d[2]["blockTransactions"] == {
            "blockVersion": 2,
            "headerId": "block-b",
            "size": 234,
            "transactions": [
                {
                    "dataInputs": [],
                    "id": "tx-b1",
                    "inputs": [{"boxId": "pub2-box1"}],
                    "outputs": [
                        {
                            "additionalRegisters": {},
                            "assets": [],
                            "boxId": "pub4-box1",
                            "creationHeight": 101,
                            "ergoTree": AC.boxid2box("pub4-box1").ergo_tree,
                            "index": 0,
                            "transactionId": "tx-b1",
                            "value": 189,
                        }
                    ],
                    "size": 344,
                }
            ],
        }

        assert d[3]["blockTransactions"] == {
            "blockVersion": 2,
            "headerId": "block-c",
            "size": 234,
            "transactions": [
                {
                    "dataInputs": [{"boxId": "pub3-box1"}],
                    "id": "tx-c1",
                    "inputs": [{"boxId": "pub4-box1"}],
                    "outputs": [
                        {
                            "additionalRegisters": {},
                            "assets": [],
                            "boxId": "pub5-box1",
                            "creationHeight": 102,
                            "ergoTree": AC.boxid2box("pub5-box1").ergo_tree,
                            "index": 0,
                            "transactionId": "tx-c1",
                            "value": 189,
                        }
                    ],
                    "size": 344,
                }
            ],
        }

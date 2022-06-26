import pytest
from fixtures.scenario.syntax import parse
from fixtures.scenario.genesis import GENESIS_ID
from fixtures.scenario.addresses import AddressCatalogue as AC


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
                cex1-box1   10 [0400]
                // tokens should come before registers
                pub1-box2  190 (tokenx: 20, tokeny: 1) [0401, 0402]

                -- // Multiple txs are separated by 2 dashes
                pub1-box2 190
                >
                pub2-box1 189 (tokenz: 5)
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
        start_ts = 1234560000000 + 100_000
        d, m = parse(desc, start_height, start_ts)
        assert len(d) == 4
        assert d[0]["header"] == {
            "votes": "000000",
            "timestamp": start_ts,
            "size": 123,
            "height": start_height,
            "id": "block-a",
            "parentId": GENESIS_ID,
        }

        assert d[1]["header"] == {
            "votes": "000000",
            "timestamp": start_ts + 100000,
            "size": 123,
            "height": start_height + 1,
            "id": "block-x",
            "parentId": "block-a",
        }

        assert d[2]["header"] == {
            "votes": "000000",
            "timestamp": start_ts + 100000,
            "size": 123,
            "height": start_height + 1,
            "id": "block-b",
            "parentId": "block-a",
        }

        assert d[3]["header"] == {
            "votes": "000000",
            "timestamp": start_ts + 200000,
            "size": 123,
            "height": start_height + 2,
            "id": "block-c",
            "parentId": "block-b",
        }

    def test_block_transactions(self, desc):
        start_height = 100
        start_ts = 1234560000000
        d, m = parse(desc, start_height, start_ts)
        assert len(d) == 4

        assert d[0]["blockTransactions"] == {
            "blockVersion": 2,
            "headerId": "block-a",
            "size": 234,
            "transactions": [
                {
                    "dataInputs": [],
                    "id": m["tx-a1"],
                    "inputs": [{"boxId": m["pub1-box1"]}, {"boxId": m["con1-box1"]}],
                    "outputs": [
                        {
                            "additionalRegisters": {"R4": "0400"},
                            "assets": [],
                            "boxId": m["cex1-box1"],
                            "creationHeight": 100,
                            "ergoTree": AC.boxid2box("cex1-box1").ergo_tree,
                            "index": 0,
                            "transactionId": m["tx-a1"],
                            "value": 10,
                        },
                        {
                            "additionalRegisters": {
                                "R4": "0401",
                                "R5": "0402",
                            },
                            "assets": [
                                {"amount": 20, "tokenId": m["tokenx"]},
                                {"amount": 1, "tokenId": m["tokeny"]},
                            ],
                            "boxId": m["pub1-box2"],
                            "creationHeight": 100,
                            "ergoTree": AC.boxid2box("pub1-box2").ergo_tree,
                            "index": 1,
                            "transactionId": m["tx-a1"],
                            "value": 190,
                        },
                    ],
                    "size": 344,
                },
                {
                    "dataInputs": [],
                    "id": m["tx-a2"],
                    "inputs": [{"boxId": m["pub1-box2"]}],
                    "outputs": [
                        {
                            "additionalRegisters": {},
                            "assets": [
                                {"amount": 5, "tokenId": m["tokenz"]},
                            ],
                            "boxId": m["pub2-box1"],
                            "creationHeight": 100,
                            "ergoTree": AC.boxid2box("pub2-box1").ergo_tree,
                            "index": 0,
                            "transactionId": m["tx-a2"],
                            "value": 189,
                        },
                        {
                            "additionalRegisters": {},
                            "assets": [],
                            "boxId": m["fees-box1"],
                            "creationHeight": 100,
                            "ergoTree": AC.boxid2box("fees-box1").ergo_tree,
                            "index": 1,
                            "transactionId": m["tx-a2"],
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
                    "id": m["tx-x1"],
                    "inputs": [{"boxId": m["pub2-box1"]}],
                    "outputs": [
                        {
                            "additionalRegisters": {},
                            "assets": [],
                            "boxId": m["pub3-box1"],
                            "creationHeight": 101,
                            "ergoTree": AC.boxid2box("pub3-box1").ergo_tree,
                            "index": 0,
                            "transactionId": m["tx-x1"],
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
                    "id": m["tx-b1"],
                    "inputs": [{"boxId": m["pub2-box1"]}],
                    "outputs": [
                        {
                            "additionalRegisters": {},
                            "assets": [],
                            "boxId": m["pub4-box1"],
                            "creationHeight": 101,
                            "ergoTree": AC.boxid2box("pub4-box1").ergo_tree,
                            "index": 0,
                            "transactionId": m["tx-b1"],
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
                    "dataInputs": [{"boxId": m["pub3-box1"]}],
                    "id": m["tx-c1"],
                    "inputs": [{"boxId": m["pub4-box1"]}],
                    "outputs": [
                        {
                            "additionalRegisters": {},
                            "assets": [],
                            "boxId": m["pub5-box1"],
                            "creationHeight": 102,
                            "ergoTree": AC.boxid2box("pub5-box1").ergo_tree,
                            "index": 0,
                            "transactionId": m["tx-c1"],
                            "value": 189,
                        }
                    ],
                    "size": 344,
                }
            ],
        }

import pytest

from fixtures.scenario.addresses import AddressCatalogue as AC


@pytest.mark.order(1)
class TestAddressCatalogue:
    def test_coinbase_address(self):
        base = AC.coinbase
        assert AC.get("base") == base
        assert AC.boxid2addr("base-box1") == base.address
        assert base.address.startswith("2Z4YBkDs")

    def test_fees_address(self):
        fees = AC.fees
        assert AC.get("fees") == fees
        assert AC.boxid2addr("fees-box1") == fees.address
        assert fees.address.startswith("2iHkR7CW")

    def test_reemission_address(self):
        reem = AC.reemission
        assert AC.get("reem") == reem
        assert AC.boxid2addr("reem-box1") == reem.address
        assert reem.address.startswith("22WkKcVU")

    def test_pay_to_reemission_address(self):
        p2re = AC.pay2reemission
        assert AC.get("p2re") == p2re
        assert AC.boxid2addr("p2re-box1") == p2re.address
        assert p2re.address.startswith("6KxusedL")

    def test_treasury_address(self):
        tres = AC.treasury
        assert AC.get("tres") == tres
        assert AC.boxid2addr("tres-box1") == tres.address
        assert tres.address.startswith("4L1ktFSzm3SH1UioDuUf")

    def test_p2pk_address(self):
        pub5 = AC.get("pub5")
        assert AC.boxid2addr("pub5-box1") == pub5.address
        assert pub5.address.startswith(
            "9eXazefQtmGqo2HpkF5hYTCAmqidJgusLpN7c5K6C9EPz7aTXkU"
        )
        assert pub5.ergo_tree.startswith(
            "0008cd0201317841f15c80bf12a588c79315fd96696dad96db9f4e9d8f51f6d5b93a48a4"
        )

    def test_contract_address(self):
        con5 = AC.get("con5")
        assert AC.boxid2addr("con5-box1") == con5.address
        assert con5.address.startswith("3i6Vwjab1VJaWJh1fNN2p1noNWK65VC8NfUizzv3wj")
        assert con5.ergo_tree.startswith(
            "10030404040205a09c01d1ed93b1a5730093c1b2a57301007302"
        )

    def test_miner_address(self):
        min2 = AC.get("min2")
        assert AC.boxid2addr("min2-box1") == min2.address
        assert (
            min2.address
            == "88dhgzEuTXaSMfvRg39TK9MK5DAn6fzemXbstAmcRyxzEjnmkpDuCZTXKPfrEiRMZGVsia8CoqCix3PZ"
        )
        assert (
            min2.ergo_tree
            == "100204a00b08cd02c22a3385ff58d705d9c5d94fe7167460d9a86a4d552fc204ed7043583c495c80ea02d192a39a8cc7a70173007301"
        )

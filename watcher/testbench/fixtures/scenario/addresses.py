"""
Repository of addresses and their Ergo tree.

Addresses are sorted alphabetically for predictability.

Fees address starts with 2iH
Coinbase starts with 2Z4
Selected contracts start with 3 or 4.
P2PK's start with a 9.
CEX addresses are P2PK's and match real main CEX addresses.
When sorted alphabetically, CEX addresses appear after dummy P2PK's.
"""

from collections import namedtuple

Box = namedtuple("Box", ["address", "ergo_tree"])

P2PK_BOXES = [
    Box(
        "9eaFpf4DR1Fj3WnCvDdgfNNdfa8tAZ1Ga21YchCZpeFSEFtkKDq",
        "0008cd020741296f1bf88bab2270929be88f742bb0f6b267643588af85639e1a8c982a41",
    ),
    Box(
        "9ehRhY3wTdX2cSPrUd2BikruCnDbQecT8Ltbv6rySPHcobckEEy",
        "0008cd0217891b7295b1a4a1dcf58d66e2c16bc406823b4c7e257d480e28ed26e355805c",
    ),
    Box(
        "9ej1aU6oszgKH6rUgPxt3mfRLcVom6G6mz1bsPzvfJbDCzYyxip",
        "0008cd021b21ea475540947456e3be710d4da1b517814c5a5e9889799e53a9a1534aad5f",
    ),
    Box(
        "9ejxqxQ4hh9zrda4WFgiCfWDZrvUeBMq854XoUhp5drrkxEXfSx",
        "0008cd021d4bcdf4822eb2dc18d3e323c9787f4a03f2a22909f9a79cb9672d7e6220a9c9",
    ),
    Box(
        "9eXazefQtmGqo2HpkF5hYTCAmqidJgusLpN7c5K6C9EPz7aTXkU",
        "0008cd0201317841f15c80bf12a588c79315fd96696dad96db9f4e9d8f51f6d5b93a48a4",
    ),
    Box(
        "9f43wgkYMmUsx8XiwX41oAPHzpkUkGw1A4yuqyBP9vaBzJZHJZf",
        "0008cd02465deec2a8fdf350c7af6d9ea242cc7618392237f646ea09b4736e397e512912",
    ),
    Box(
        "9fehjwA5KmbX28vv5fXXSAEmxuQaDNa4ioaAY59GfbwcFhBkLHj",
        "0008cd02950c3cac76ff441631ef050e406cdbb53ee3f85f96fd97d76e834aaf6c1cb4bb",
    ),
    Box(
        "9ffUoQkXbzzFp7s6797Vpr7qGhF6RTgJyJQqaEu8cFiJNYGVvi4",
        "0008cd0296cfd48615366742ff718c779348aeffc7910fbebdd93de4ea68aa7ce3e0dcd5",
    ),
    Box(
        "9fhr6v9RFAcvSpbmPJPXYa3Ym13yeWGd8F4k1bEvB8B3FKVJxoP",
        "0008cd029c2fe0883dd195ff884e60d4960bf28d85aee48575b2b17a746d977b2cb81e63",
    ),
    Box(
        "9fjoLKBiuRAf1vWzhjnYEFhDrNuFoxXPPLjFHtrTxNXmiiMXkuS",
        "0008cd02a09eaf3bbaa2d7a00a6f745146a8d5860ba930b103c972cec054ad9181705258",
    ),
]

CEX_BOXES = [
    # Coinex (CEX ID 1)
    Box(
        "9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe",
        "0008cd027304abbaebe8bb3a9e963dfa9fa4964d7d001e6a1bd225eadc84048ae49b627c",
    ),
    # Gate (CEX ID 2)
    Box(
        "9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC",
        "0008cd03f3f44c9e80e2cedc1a2909631a3adea8866ee32187f74d0912387359b0ff36a2",
    ),
    # KuCoin (CEX ID 3)
    Box(
        "9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3",
        "0008cd03db3ac4dccd3546c949e23a2c1f49cd2bb2559c298d6babd451e7469c57e92507",
    ),
]

CONTRACT_BOXES = [
    Box(
        "3i6QAH4GMJDnicSdUo1UotuugYePfQTPDipHCZ67U7",
        "1000d1aea5d9010163ed93c27201c2a793db63087201db6308a7",
    ),
    Box(
        "3i6Vwj3EMRVuNu5dpiZvktX12hYRR9BY7QTmLtEDDS",
        "1003040004000400d1937eb28cc7b2a573000002730100047302",
    ),
    Box(
        "3i6Vwj3ENEB7M7gDF5VgPkoB5YD4PPj7NQyeqZA6uW",
        "1003040004020400d1937eb28cc7b2a573000002730100047302",
    ),
    Box(
        "3i6Vwjab1VHv5sttXnLXWHLDeF3cqJJgbTPKocSvoC",
        "1003040404020580897ad1ed93b1a5730093c1b2a57301007302",
    ),
    Box(
        "3i6Vwjab1VJaWJh1fNN2p1noNWK65VC8NfUizzv3wj",
        "10030404040205a09c01d1ed93b1a5730093c1b2a57301007302",
    ),
    Box(
        "3i6VwjJv8cJCHoJbZYv9nGniTfpa6QvLJk3okZk9qf",
        "1003040204420402d1937eb28cc7b2a573000002730100047302",
    ),
    Box("3UXeVWcMjnaYEpUJnimUAV5EYRZ", "1001049e8116d193e4c6a704047300"),
    Box("4fnfW5TPLXpNs4s7eArzjV8yb7QE9Ne", "10010400d193c2b2a5730000c2e4c6a70463"),
    Box("4fnfXXHKacV6wSF5N6JdsBZUY1nSrPP", "100105cab58be609d1937ce4c6a7040e7300"),
    Box("4fnj6HBm9WNjNkwUZ5dudBMbThbNi4E", "100205aad4180504d1939e7cc5a773007301"),
]

MINER_BOXES = [
    Box(
        "88dhgzEuTXaR43bqdpnDceosv2jRJBMtbKnxMG9c1KGrya7d4q3aGcntv7AAoyJmxzX9jgjrFCxzToYA",
        "100204a00b08cd025856eaacbeb7ab863620bdd3599700d08067a93eda175ba818644146142ddffbea02d192a39a8cc7a70173007301",
    ),
    Box(
        "88dhgzEuTXaSMfvRg39TK9MK5DAn6fzemXbstAmcRyxzEjnmkpDuCZTXKPfrEiRMZGVsia8CoqCix3PZ",
        "100204a00b08cd02c22a3385ff58d705d9c5d94fe7167460d9a86a4d552fc204ed7043583c495c80ea02d192a39a8cc7a70173007301",
    ),
    Box(
        "88dhgzEuTXaW1EuNPMaucRHhxCjLU4zsWrswdLkn3M8AJxu198SytmR9o3bUPC2WecQgv6yqevYu2exk",
        "100204a00b08cd03ea379d9ced5777531f1488f62420cae56526dc18f3b8da5bef69cf7cf7f0803cea02d192a39a8cc7a70173007301",
    ),
]

COINBASE_BOX = Box(
    "2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU",
    "101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f",
)

FEES_BOX = Box(
    "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe",
    "1005040004000e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a701730073011001020402d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304",
)

REEMISSION_BOX = Box(
    "22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77",
    "19870210040004000e20d3feeffa87f2df63a7a15b4905e618ae3ce4c69a7975f171bd314d0b877927b80400040004020580dac4090404040004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a70173007301100102040204c080fe010580f882ad160400d196830301938cb2e4c6b2a5730000020c4d0e73010001730293c2a7c2b2a5730300978302019683030191c1b2a5730400c1a790c1b2a5730500730693b1a5730796830501938cc7b2a573080001a39683020193c2b2a573090074730a730b830108cdeeac93a38cc7b2a5730c000192a3730d91a38cc7a70193730e99c1a7c1b2a5730f00",
)

PAY_TO_REEMISSION_BOX = Box(
    "6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p",
    "193c03040004000e20d3feeffa87f2df63a7a15b4905e618ae3ce4c69a7975f171bd314d0b877927b8d1938cb2e4c6b2a5730000020c4d0e730100017302",
)


# Ensure addresses are unique
assert len(P2PK_BOXES) == len(set([b.address for b in P2PK_BOXES]))
assert len(CONTRACT_BOXES) == len(set([b.address for b in CONTRACT_BOXES]))
assert len(MINER_BOXES) == len(set([b.address for b in MINER_BOXES]))


class _AddressCatalogue:
    """
    Provides a dict maping actual ergotrees to addresses.

    Node data doesn't contain addresses, only box id's and ergotrees.
    When composing test blocks, and verifying resulting
    db records, it is useful to know what addresses to
    expect.
    """

    def __init__(self):
        self._key2box = {
            **{f"pub{i+1}": box for i, box in enumerate(P2PK_BOXES)},
            **{f"con{i+1}": box for i, box in enumerate(CONTRACT_BOXES)},
            **{f"min{i+1}": box for i, box in enumerate(MINER_BOXES)},
            **{f"cex{i+1}": box for i, box in enumerate(CEX_BOXES)},
            "base": COINBASE_BOX,
            "fees": FEES_BOX,
            "reem": REEMISSION_BOX,
            "p2re": PAY_TO_REEMISSION_BOX,
        }
        self._tree2addr = {
            **{box.ergo_tree: box.address for box in P2PK_BOXES},
            **{box.ergo_tree: box.address for box in CONTRACT_BOXES},
            **{box.ergo_tree: box.address for box in MINER_BOXES},
            **{box.ergo_tree: box.address for box in CEX_BOXES},
            COINBASE_BOX.ergo_tree: COINBASE_BOX.address,
            FEES_BOX.ergo_tree: FEES_BOX.address,
            REEMISSION_BOX.ergo_tree: REEMISSION_BOX.address,
            PAY_TO_REEMISSION_BOX.ergo_tree: PAY_TO_REEMISSION_BOX.address,
        }
        self.coinbase = COINBASE_BOX
        self.fees = FEES_BOX
        self.reemission = REEMISSION_BOX
        self.pay2reemission = PAY_TO_REEMISSION_BOX

    def get(self, key: str) -> Box:
        """
        pub<i> | con<i> | base | fees | cex<i>
        """
        return self._key2box[key]

    def box_from_short_id(self, short_id: str) -> Box:
        """
        pub<i> | con<i> | base | fees | cex<i>
        """
        key = short_id.split("-")[0]
        return self._key2box[key]

    def boxid2addr(self, box_id: str) -> str:
        """
        Return the address corresponding to a mock box_id pattern.

        The pattern is <key>-box<box-number>.
        Anything after first hyphen is ignored.
        Keys are mapped to boxes like so:
            base --> coinbase address
            reem --> reemission address
            p2re --> pay 2 reemission address
            fees --> fees collection address
            pub<i> --> P2PK address at index i-1
            con<i> --> contract address at index i-1
            min<i> --> miner contract address at index i-1
            cex<i> --> cex address at index i-1
        """
        key = box_id.split("-")[0]
        return self._key2box[key].address

    def boxid2box(self, box_id: str) -> Box:
        """
        Return the Box corresponding to a mock box_id pattern.

        The pattern is <key>-box<box-number>.
        Anything after first hyphen is ignored.
        Keys are mapped to boxes like so:
            base --> coinbase address
            reem --> reemission address
            p2re --> pay 2 reemission address
            fees --> fees collection address
            pub<i> --> P2PK address at index i-1
            con<i> --> contract address at index i-1
            min<i> --> miner contract address at index i-1
            cex<i> --> cex address at index i-1
        """
        key = box_id.split("-")[0]
        return self._key2box[key]


AddressCatalogue = _AddressCatalogue()

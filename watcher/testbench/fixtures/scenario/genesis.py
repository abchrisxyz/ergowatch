import sigpy

# Genesis header and tx id.
# This value is hard coded in Watcher, so has to match.
GENESIS_ID = "0" * 64

# Single dummy genesis box
GENESIS_BOX = {
    "value": 1000,
    "ergoTree": "101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f",
    "assets": [],
    "creationHeight": 0,
    "additionalRegisters": {},
}
serialized_candidate = str(GENESIS_BOX).replace("'", '"')
GENESIS_BOX["boxId"] = sigpy.calc_box_id(serialized_candidate, GENESIS_ID, 0)
GENESIS_BOX["transactionId"] = GENESIS_ID
GENESIS_BOX["index"] = 0

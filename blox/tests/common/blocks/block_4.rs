pub const BLOCK_4: &str = r#"
  {
    "header": {
      "extensionId": "954216c296b4d6efff19a4f971b72a5439057c07b3254858b4dc3d03f86d64b5",
      "difficulty": "1199990374400",
      "votes": "000000",
      "timestamp": 1561979010515,
      "size": 279,
      "stateRoot": "c5410cbb9dff9a4a98f4c9016a156a6696e609601e8c81e9c19e79816c698e9904",
      "height": 4,
      "nBits": 100734821,
      "version": 1,
      "id": "d46df95124a711724990f40299bb166babc56d86de624db48776e2afb80e0302",
      "adProofsRoot": "66a0ed18269ae22ff110eafed64e6c45cfe8fdf2815d06ccd98afd4d3bed9504",
      "transactionsRoot": "92bea47dc72d2bc33e4f5a05cb5ac99876534d23537742e6fbdfd3cea455c81a",
      "extensionHash": "df4ff3b77824042f5c16a5da006c992258bd8574e8429b59cd02fc59ff0d22ce",
      "powSolutions": {
        "pk": "0337024f9dd20621ecd32baeee4741130d24797eda8cad0d09c794cb4458c4f2a3",
        "w": "0369f2e10d3a65b5c275bf5ce7ea5105ca9a5a25f81905305454a1196a2a01d427",
        "n": "00000012006a2db9",
        "d": 7.308870309452721e+64
      },
      "adProofsId": "a5d4fdfe86ae89f6aebc8d3a66bea419cbb74910e6738454b5d5daa937b8ebff",
      "transactionsId": "25faf5ef8774a0cd9c77ce5477ee86498247c54f8f9ba8e033b9a12b2735ed91",
      "parentId": "3ff49e2419f779390a9347e8c3ee6391dd3f9e543c12dabcb0f1ebc8168754f4"
    },
    "blockTransactions": {
      "headerId": "d46df95124a711724990f40299bb166babc56d86de624db48776e2afb80e0302",
      "transactions": [
        {
          "id": "d7b5bd08ab15dbe5bb651b8f3b29b17fc19d501de9b8ae473b9d0cff659036bc",
          "inputs": [
            {
              "boxId": "44efc324ccc69e914a331eceef0b97c6ee9ebab30eec13cc6d7ab0ba65a615c1",
              "spendingProof": {
                "proofBytes": "",
                "extension": {}
              }
            }
          ],
          "dataInputs": [],
          "outputs": [
            {
              "boxId": "337001bdfb9c0c14cc1f56f59ceba2054cf6a7ad449567192b92e1502f995ccb",
              "value": 93408862500000000,
              "ergoTree": "101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f",
              "assets": [],
              "creationHeight": 4,
              "additionalRegisters": {},
              "transactionId": "d7b5bd08ab15dbe5bb651b8f3b29b17fc19d501de9b8ae473b9d0cff659036bc",
              "index": 0
            },
            {
              "boxId": "366474b8065e5e51a9b760c45c369c2efdf02045712cdb7341e3c11dd6c8ea48",
              "value": 67500000000,
              "ergoTree": "100204a00b08cd0337024f9dd20621ecd32baeee4741130d24797eda8cad0d09c794cb4458c4f2a3ea02d192a39a8cc7a70173007301",
              "assets": [],
              "creationHeight": 4,
              "additionalRegisters": {},
              "transactionId": "d7b5bd08ab15dbe5bb651b8f3b29b17fc19d501de9b8ae473b9d0cff659036bc",
              "index": 1
            }
          ],
          "size": 341
        }
      ],
      "blockVersion": 1,
      "size": 374
    },
    "extension": {
      "headerId": "d46df95124a711724990f40299bb166babc56d86de624db48776e2afb80e0302",
      "digest": "df4ff3b77824042f5c16a5da006c992258bd8574e8429b59cd02fc59ff0d22ce",
      "fields": [
        [
          "0100",
          "01b0244dfc267baca974a4caee06120321562784303a8a688976ae56170e4d175b"
        ],
        [
          "0101",
          "05855fc5c9eed868b43ea2c3df99ec17dd9d903187d891e2365a89b98125c994b2"
        ]
      ]
    },
    "adProofs": {
      "headerId": "d46df95124a711724990f40299bb166babc56d86de624db48776e2afb80e0302",
      "proofBytes": "039c29caa2dda1d7b201ed6e67b1836738cc9eb06e347c8f6a5cf007132d71bbb50211b1524584ce58e0e1c3f3acca72ecde927381826f59d1274bd501cff86b653644efc324ccc69e914a331eceef0b97c6ee9ebab30eec13cc6d7ab0ba65a615c1000000608086c1bafb01100204a00b08cd02d3a9410ac758ad45dfc85af8626efdacf398439c73977b13064aa8e6c8f2ac88ea02d192a39a8cc7a701730073010300004f2d69416edcabf31a01a36f72a7b1c9333df3f2e2b5357af0a923e6a3a08c4201000245dc27302332bcb93604ae63c0a543894b38af31e6aebdb40291e3e8ecaef0310000011180a8ffdbd4ddf6a501101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f0300004f2d69416edcabf31a01a36f72a7b1c9333df3f2e2b5357af0a923e6a3a08c420003062fc6c904b40635e002a6dc1c4f78cb7627979a8c34af0a9ee10dca39464d7400000386de97496f2cad56fc2eab02cec39d0f2c28b192133cdc8f17ef105d29e3ffe400046d01",
      "digest": "66a0ed18269ae22ff110eafed64e6c45cfe8fdf2815d06ccd98afd4d3bed9504",
      "size": 615
    },
    "size": 1268
  }
"#;

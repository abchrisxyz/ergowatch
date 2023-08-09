// Block 3 with an extra output taken from tx:
// f03ae1b3cd153407cb69be9354d143fb39e8c5fd1ccbf0f41eeebeb2b7bf56b3.
//
// Extra outputs have different addresses and tokens to test
// address/token id handling during a rollback.
pub const BLOCK_3: &str = r#"
  {
    "header": {
      "extensionId": "ba9e62510642427c33a8efe637f007094860ae70c403658da38f98e9db922fa1",
      "difficulty": "1199990374400",
      "votes": "000000",
      "timestamp": 1561979000032,
      "size": 279,
      "stateRoot": "7bb6a177e849e45ce5313ab7fa08e90daed00ceeadec13f271eea500df3e801303",
      "height": 3,
      "nBits": 100734821,
      "version": 1,
      "id": "3ff49e2419f779390a9347e8c3ee6391dd3f9e543c12dabcb0f1ebc8168754f4",
      "adProofsRoot": "d80fc4ec24e7874760c6e42a8bce13791f15fe7f83d4cd055f614c25527a304b",
      "transactionsRoot": "4202efb982197ef2b6629c2202796584a7351bbc0563b27ed35c295e95021b94",
      "extensionHash": "df4ff3b77824042f5c16a5da006c992258bd8574e8429b59cd02fc59ff0d22ce",
      "powSolutions": {
        "pk": "02d3a9410ac758ad45dfc85af8626efdacf398439c73977b13064aa8e6c8f2ac88",
        "w": "0255d213ecba5fd74e52002e08a69a2e5e08378f2e43fbbf3f1130dde976db3426",
        "n": "0000000900cb491a",
        "d": 6.367629973866463e+64
      },
      "adProofsId": "f4fea40fa3aaa497119cc83203a859103ee278c7b3f77e7df5ad3655a8436597",
      "transactionsId": "cdf0d223b332a9e75ad42e26b319ce490152590db3965eb27c58089898ec43ca",
      "parentId": "855fc5c9eed868b43ea2c3df99ec17dd9d903187d891e2365a89b98125c994b2"
    },
    "blockTransactions": {
      "headerId": "3ff49e2419f779390a9347e8c3ee6391dd3f9e543c12dabcb0f1ebc8168754f4",
      "transactions": [
        {
          "id": "4f2d69416edcabf31a01a36f72a7b1c9333df3f2e2b5357af0a923e6a3a08c42",
          "inputs": [
            {
              "boxId": "3294ce445f73066476748bdb2a5aeae3692dcf3a24a5b259f45f419b514c4a1f",
              "spendingProof": {
                "proofBytes": "",
                "extension": {}
              }
            }
          ],
          "dataInputs": [],
          "outputs": [
            {
              "boxId": "44efc324ccc69e914a331eceef0b97c6ee9ebab30eec13cc6d7ab0ba65a615c1",
              "value": 93408930000000000,
              "ergoTree": "101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f",
              "assets": [],
              "creationHeight": 3,
              "additionalRegisters": {},
              "transactionId": "4f2d69416edcabf31a01a36f72a7b1c9333df3f2e2b5357af0a923e6a3a08c42",
              "index": 0
            },
            {
              "boxId": "11b1524584ce58e0e1c3f3acca72ecde927381826f59d1274bd501cff86b6536",
              "value": 67000000000,
              "ergoTree": "100204a00b08cd02d3a9410ac758ad45dfc85af8626efdacf398439c73977b13064aa8e6c8f2ac88ea02d192a39a8cc7a70173007301",
              "assets": [],
              "creationHeight": 3,
              "additionalRegisters": {},
              "transactionId": "4f2d69416edcabf31a01a36f72a7b1c9333df3f2e2b5357af0a923e6a3a08c42",
              "index": 1
            },
            {
              "boxId": "60379316cd598cbc4b42f15ab7e634762e4263fce70ffe8b324344ba27e78245",
              "value": 500000000,
              "ergoTree": "0008cd0321440ba64346c25a575543cff4bfac1ba89b6c916f47989f5d8a6c2c7ce689ea",
              "assets": [
                {
                  "tokenId": "6a3f36ddaaa792b1937146003c6bbb7af204b351e737f80a93d4aa7854e7b403",
                  "amount": 1
                },
                {
                  "tokenId": "2514c37d6996b87daa90eb00f0a23692d21a610b63a14603ea6605f2bc3dee2d",
                  "amount": 1
                }
              ],
              "creationHeight": 34329,
              "additionalRegisters": {},
              "transactionId": "4f2d69416edcabf31a01a36f72a7b1c9333df3f2e2b5357af0a923e6a3a08c42",
              "index": 2
            }
          ],
          "size": 341
        }
      ],
      "blockVersion": 1,
      "size": 374
    },
    "extension": {
      "headerId": "3ff49e2419f779390a9347e8c3ee6391dd3f9e543c12dabcb0f1ebc8168754f4",
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
      "headerId": "3ff49e2419f779390a9347e8c3ee6391dd3f9e543c12dabcb0f1ebc8168754f4",
      "proofBytes": "0200000000000000000000000000000000000000000000000000000000000000003294ce445f73066476748bdb2a5aeae3692dcf3a24a5b259f45f419b514c4a1f000000000245dc27302332bcb93604ae63c0a543894b38af31e6aebdb40291e3e8ecaef0310000011180aec096d0dff6a501101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f02000034a247d3657df7ff5318164747e338ddef9526ab8da430f4c81246f132a68b60000003062fc6c904b40635e002a6dc1c4f78cb7627979a8c34af0a9ee10dca39464d74ff0386de97496f2cad56fc2eab02cec39d0f2c28b192133cdc8f17ef105d29e3ffe400047b",
      "digest": "d80fc4ec24e7874760c6e42a8bce13791f15fe7f83d4cd055f614c25527a304b",
      "size": 484
    },
    "size": 1137
  }
"#;

// Variant of BLOCK_3 with a different header id and extra output
// Extra output coming from this tx: f04dd00b1cb28cb85967eec78c65a3c5f83e3b334c2aac22a61b1a72dbae239a
pub const BLOCK_3BIS: &str = r#"
  {
    "header": {
      "extensionId": "ba9e62510642427c33a8efe637f007094860ae70c403658da38f98e9db922fa1",
      "difficulty": "1199990374400",
      "votes": "000000",
      "timestamp": 1561979000032,
      "size": 279,
      "stateRoot": "7bb6a177e849e45ce5313ab7fa08e90daed00ceeadec13f271eea500df3e801303",
      "height": 3,
      "nBits": 100734821,
      "version": 1,
      "id": "3ff49e2419f779390a9347e8c3ee6391dd3f9e543c12dabcbsomeotherheader",
      "adProofsRoot": "d80fc4ec24e7874760c6e42a8bce13791f15fe7f83d4cd055f614c25527a304b",
      "transactionsRoot": "4202efb982197ef2b6629c2202796584a7351bbc0563b27ed35c295e95021b94",
      "extensionHash": "df4ff3b77824042f5c16a5da006c992258bd8574e8429b59cd02fc59ff0d22ce",
      "powSolutions": {
        "pk": "02d3a9410ac758ad45dfc85af8626efdacf398439c73977b13064aa8e6c8f2ac88",
        "w": "0255d213ecba5fd74e52002e08a69a2e5e08378f2e43fbbf3f1130dde976db3426",
        "n": "0000000900cb491a",
        "d": 6.367629973866463e+64
      },
      "adProofsId": "f4fea40fa3aaa497119cc83203a859103ee278c7b3f77e7df5ad3655a8436597",
      "transactionsId": "cdf0d223b332a9e75ad42e26b319ce490152590db3965eb27c58089898ec43ca",
      "parentId": "855fc5c9eed868b43ea2c3df99ec17dd9d903187d891e2365a89b98125c994b2"
    },
    "blockTransactions": {
      "headerId": "3ff49e2419f779390a9347e8c3ee6391dd3f9e543c12dabcb0f1ebc8168754f4",
      "transactions": [
        {
          "id": "4f2d69416edcabf31a01a36f72a7b1c9333df3f2e2b5357af0a923e6a3a08c42",
          "inputs": [
            {
              "boxId": "3294ce445f73066476748bdb2a5aeae3692dcf3a24a5b259f45f419b514c4a1f",
              "spendingProof": {
                "proofBytes": "",
                "extension": {}
              }
            }
          ],
          "dataInputs": [],
          "outputs": [
            {
              "boxId": "44efc324ccc69e914a331eceef0b97c6ee9ebab30eec13cc6d7ab0ba65a615c1",
              "value": 93408930000000000,
              "ergoTree": "101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f",
              "assets": [],
              "creationHeight": 3,
              "additionalRegisters": {},
              "transactionId": "4f2d69416edcabf31a01a36f72a7b1c9333df3f2e2b5357af0a923e6a3a08c42",
              "index": 0
            },
            {
              "boxId": "11b1524584ce58e0e1c3f3acca72ecde927381826f59d1274bd501cff86b6536",
              "value": 67000000000,
              "ergoTree": "100204a00b08cd02d3a9410ac758ad45dfc85af8626efdacf398439c73977b13064aa8e6c8f2ac88ea02d192a39a8cc7a70173007301",
              "assets": [],
              "creationHeight": 3,
              "additionalRegisters": {},
              "transactionId": "4f2d69416edcabf31a01a36f72a7b1c9333df3f2e2b5357af0a923e6a3a08c42",
              "index": 1
            },
            {
              "boxId": "089105a867391d773a57d500dab9aef255b0292ec66ce1d9c9813d108d7283e7",
              "value": 500000000,
              "ergoTree": "0008cd02dada811a888cd0dc7a0a41739a3ad9b0f427741fe6ca19700cf1a51200c96bf7",
              "assets": [
                {
                  "tokenId": "ceb57ebd00fe060b67ca56948c685477d7e64273efbaadd2ad7e7d3bf1e62dd9",
                  "amount": 1
                }
              ],
              "creationHeight": 8010,
              "additionalRegisters": {
                "R4": "0e03545354",
                "R5": "0e134b75736874692773205465737420746f6b656e",
                "R6": "0400"
              },
              "transactionId": "4f2d69416edcabf31a01a36f72a7b1c9333df3f2e2b5357af0a923e6a3a08c42",
              "index": 2
            }
          ],
          "size": 341
        }
      ],
      "blockVersion": 1,
      "size": 374
    },
    "extension": {
      "headerId": "3ff49e2419f779390a9347e8c3ee6391dd3f9e543c12dabcb0f1ebc8168754f4",
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
      "headerId": "3ff49e2419f779390a9347e8c3ee6391dd3f9e543c12dabcb0f1ebc8168754f4",
      "proofBytes": "0200000000000000000000000000000000000000000000000000000000000000003294ce445f73066476748bdb2a5aeae3692dcf3a24a5b259f45f419b514c4a1f000000000245dc27302332bcb93604ae63c0a543894b38af31e6aebdb40291e3e8ecaef0310000011180aec096d0dff6a501101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f02000034a247d3657df7ff5318164747e338ddef9526ab8da430f4c81246f132a68b60000003062fc6c904b40635e002a6dc1c4f78cb7627979a8c34af0a9ee10dca39464d74ff0386de97496f2cad56fc2eab02cec39d0f2c28b192133cdc8f17ef105d29e3ffe400047b",
      "digest": "d80fc4ec24e7874760c6e42a8bce13791f15fe7f83d4cd055f614c25527a304b",
      "size": 484
    },
    "size": 1137
  }
"#;

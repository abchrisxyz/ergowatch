import sigpy

box_data = {
    "value": 1000000,
    "ergoTree": "100604000400050004000e20002693cd6c3dc7c156240dd1c7370e50c4d1f84a752c2f74d93a20cc22c2899d0e204759889b16a97b0c7ab5ccb30c7fafb7d9e17fd6dc41ab86ae380784abe03e4cd803d601b2a5730000d602e4c6a70407d603b2db6501fe730100ea02d1ededededed93e4c672010407720293e4c67201050ec5720391e4c672010605730293c27201c2a793db63087201db6308a7ed938cb2db6308720373030001730493cbc272037305cd7202",
    "assets": [
        {
            "tokenId": "01e6498911823f4d36deaf49a964e883b2c4ae2a4530926f18b9c1411ab2a2c2",
            "amount": 1,
        }
    ],
    "creationHeight": 599998,
    "additionalRegisters": {
        "R4": "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
        "R5": "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
        "R6": "05a4c3edd9998877",
    },
}

tx_id = "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8"
index = 0

box_id = sigpy.calc_box_id(str(box_data).replace("'", '"'), tx_id, index)
expected = "aa94183d21f9e8fee38d4f3326d2acf8258dd36e6dff38142fa93e633d01464d"
print(box_id)
assert box_id == expected

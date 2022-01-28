"""
Making sure the mock api works as intended.
"""
import requests

from api import mock_api


def test_api_fixture_connection(mock_api):
    """
    Test fixture starts mock api server
    """
    r = requests.get(f"http://localhost:9053/check")
    assert r.status_code == 200
    assert r.text == "working"


def test_api_fixture_info(mock_api):
    """
    Test height is set to latest block
    """
    r = requests.get(f"http://localhost:9053/info")
    assert r.status_code == 200
    assert r.json()["fullHeight"] == 600_000


def test_api_fixture_blocks_at(mock_api):
    """
    Test api returns right block
    """
    url = "http://localhost:9053/blocks/at/{}"

    h = 599_999
    r = requests.get(url.format(h))
    assert r.status_code == 200
    res = r.json()
    assert len(res) == 1
    assert res[0] == "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1"

    h = 600_000
    r = requests.get(url.format(h))
    assert r.status_code == 200
    res = r.json()
    assert len(res) == 1
    assert res[0] == "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"


def test_api_fixture_blocks(mock_api):
    """
    Test api returns right block
    """
    url = "http://localhost:9053/blocks/{}"

    h = "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1"
    r = requests.get(url.format(h))
    assert r.status_code == 200
    res = r.json()
    assert res["header"]["height"] == 599_999
    assert res["header"]["id"] == h

    h = "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
    r = requests.get(url.format(h))
    assert r.status_code == 200
    res = r.json()
    assert res["header"]["height"] == 600_000
    assert res["header"]["id"] == h

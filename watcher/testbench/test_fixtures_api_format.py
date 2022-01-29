"""
Making sure the mock node api mimics the real one.
"""
import requests
import pytest

from fixtures import bootstrapped_env
from local import NODE_URL


@pytest.mark.order(1)
def test_block_600k(bootstrapped_env):
    """
    Checks the mocked and real node api's return identical responses.
    """
    header = "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
    r = requests.get(f"{NODE_URL}/blocks/{header}/")
    assert r.status_code == 200
    real_data = r.json()

    r = requests.get(f"http://localhost:9053/blocks/{header}")
    assert r.status_code == 200
    mock_data = r.json()
    assert mock_data == real_data

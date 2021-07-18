import asyncio
import pytest


from ..main import db


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module", autouse=True)
async def init_db_connection_pool():
    await db.init_connection_pool()


@pytest.mark.asyncio
async def test_get_latest_block_height():
    height = await db.get_latest_block_height()
    assert height > 500000


@pytest.mark.asyncio
async def test_get_oracle_pools_ergusd_latest_posting():
    d = await db.get_oracle_pools_ergusd_latest()
    assert len(d) == 3
    assert d["height"] > 530000
    assert d["price"] > 0
    assert d["datapoints"] > 0


@pytest.mark.asyncio
async def test_get_oracle_pools_ergusd_oracle_stats():
    data = await db.get_oracle_pools_ergusd_oracle_stats()
    assert len(data) == 11
    assert "address" in data[0]
    assert data[0]["commits"] > 100
    assert data[0]["accepted_commits"] > 100
    assert data[0]["collections"] > 100
    assert "first_commit" in data[0]
    assert "last_commit" in data[0]
    assert "last_accepted" in data[0]
    assert "last_collection" in data[0]

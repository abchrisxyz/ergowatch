import asyncio
import pytest
import datetime


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
async def test_get_oracle_pools_ergusd_recent_epoch_durations():
    d = await db.get_oracle_pools_ergusd_recent_epoch_durations()
    assert len(d) == 100
    assert d[0]["h"] > 530000
    assert d[0]["n"] > 0


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


@pytest.mark.asyncio
async def test_get_sigmausd_state():
    d = await db.get_sigmausd_state()
    assert len(d) == 8
    assert d["reserves"] > 0
    assert d["circ_sigusd"] > 0
    assert d["circ_sigrsv"] > 0
    assert d["peg_rate_nano"] > 0
    assert abs(d["net_sc_erg"]) >= 0
    assert abs(d["net_rc_erg"]) >= 0
    assert d["cum_sc_erg_in"] > 0
    assert d["cum_rc_erg_in"] > 0


@pytest.mark.asyncio
async def test_get_sigmausd_sigrsv_ohlc_d():
    data = await db.get_sigmausd_sigrsv_ohlc_d()
    assert len(data) > 125
    assert data[0]["time"] == datetime.date(2021, 3, 26)
    assert data[0]["open"] > 0
    assert data[0]["high"] > 0
    assert data[0]["low"] > 0
    assert data[0]["close"] > 0


@pytest.mark.asyncio
async def test_get_sigmausd_history_5d():
    data = await db.get_sigmausd_history(days=5)
    assert len(data) == 5
    assert data["timestamps"][0] >= 1628393682


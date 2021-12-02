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


@pytest.mark.asyncio
async def test_get_metrics_preview():
    data = await db.get_metrics_preview()
    assert len(data) == 9
    assert data["total_addresses"] > 0
    assert data["top100_supply_fraction"] >= 0.0
    assert data["top100_supply_fraction"] < 1.0
    assert data["contracts_supply_fraction"] >= 0.0
    assert data["contracts_supply_fraction"] < 1.0
    assert data["utxos"] > 0
    assert data["mean_age_days"] > 0
    assert data["mean_age_days"] > 0
    assert data["transferred_value_24h"] > 0
    assert data["transactions_24h"] > 0


@pytest.mark.asyncio
async def test_get_metrics_address_counts_summary():
    data = await db.get_metrics_address_counts_summary()
    assert len(data) == 11
    assert len(data[0]) == 7
    assert data[0]["col"] == "total"
    assert data[0]["latest"] > 0


@pytest.mark.asyncio
async def test_get_metrics_addresses_series_30d():
    data = await db.get_metrics_addresses_series(days=30)
    assert len(data) == 13
    assert len(data["timestamps"]) == 30
    # Always larger than timestamp of first block
    assert data["timestamps"][0] >= 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_addresses_series_full():
    data = await db.get_metrics_addresses_series_full()
    assert len(data) == 13
    # Always starts with timestamp of first block
    assert data["timestamps"][0] == 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_contract_counts_summary():
    data = await db.get_metrics_contract_counts_summary()
    assert len(data) == 11
    assert len(data[0]) == 7
    assert data[0]["col"] == "total"
    assert data[0]["latest"] > 0


@pytest.mark.asyncio
async def test_get_metrics_contracts_series_30d():
    data = await db.get_metrics_contracts_series(days=30)
    assert len(data) == 13
    assert len(data["timestamps"]) == 30
    # Always larger than timestamp of first block
    assert data["timestamps"][0] >= 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_contracts_series_full():
    data = await db.get_metrics_contracts_series_full()
    assert len(data) == 13
    # Always starts with timestamp of first block
    assert data["timestamps"][0] == 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_distribution_summary():
    data = await db.get_metrics_distribution_summary()
    assert len(data) == 11
    assert len(data[0]) == 7
    assert data[0]["col"] == "top10"
    assert data[0]["latest"] > 0


@pytest.mark.asyncio
async def test_get_metrics_distribution_series_30d():
    data = await db.get_metrics_distribution_series(days=30)
    assert len(data) == 7
    assert len(data["timestamps"]) == 30
    # Always larger than timestamp of first block
    assert data["timestamps"][0] >= 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_distribution_series_full():
    data = await db.get_metrics_distribution_series_full()
    assert len(data) == 7
    # Always starts with timestamp of first block
    assert data["timestamps"][0] == 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_tvl_summary():
    data = await db.get_metrics_tvl_summary()
    assert len(data) == 11
    assert len(data[0]) == 7
    assert data[0]["col"] == "top10"
    assert data[0]["latest"] > 0


@pytest.mark.asyncio
async def test_get_metrics_tvl_series_30d():
    data = await db.get_metrics_tvl_series(days=30)
    assert len(data) == 7
    assert len(data["timestamps"]) == 30
    # Always larger than timestamp of first block
    assert data["timestamps"][0] >= 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_tvl_series_full():
    data = await db.get_metrics_tvl_series_full()
    assert len(data) == 7
    # Always starts with timestamp of first block
    assert data["timestamps"][0] == 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_cexs_summary():
    data = await db.get_metrics_cexs_summary()
    assert len(data) == 9
    assert len(data[0]) == 7
    assert data[0]["col"] == "circulating_supply"
    assert data[0]["latest"] > 0


@pytest.mark.asyncio
async def test_get_metrics_cexs_series_30d():
    data = await db.get_metrics_cexs_series(days=30)
    assert len(data) == 7
    assert len(data["timestamps"]) == 30
    # Always larger than timestamp of first block
    assert data["timestamps"][0] >= 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_cexs_series_full():
    data = await db.get_metrics_cexs_series_full()
    assert len(data) == 7
    # No cex on genesis, so check at least gte
    assert data["timestamps"][0] >= 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_cex_list():
    data = await db.get_metrics_cex_list()
    assert len(data) > 5
    assert isinstance(data[0]["address"], str)
    assert isinstance(data[0]["cex"], str)


@pytest.mark.asyncio
async def test_get_metrics_age_series_30d():
    data = await db.get_metrics_age_series(days=30)
    assert len(data) == 3
    assert len(data["timestamps"]) == 30
    # Always larger than timestamp of first block
    assert data["timestamps"][0] >= 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_age_series_full():
    data = await db.get_metrics_age_series_full()
    assert len(data) == 3
    # Always starts with timestamp of first block
    assert data["timestamps"][0] == 1561978977137 // 1000
    assert data["mean_age_days"][0] == 0
    assert data["mean_age_days"][1] >= 0


@pytest.mark.asyncio
async def test_get_metrics_transfer_volume_series_30d():
    data = await db.get_metrics_transfer_volume_series(days=30)
    assert len(data) == 3
    assert len(data["timestamps"]) == 30
    # Always larger than timestamp of first block
    assert data["timestamps"][0] >= 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_transfer_volume_series_full():
    data = await db.get_metrics_transfer_volume_series_full()
    assert len(data) == 3
    # Starts at midnight UTC of genesis day
    assert data["timestamps"][0] == 1562025329
    assert data["transferred_volume"][0] == 0
    assert data["transferred_volume"][1] >= 0


@pytest.mark.asyncio
async def test_get_metrics_transactions_series_30d():
    data = await db.get_metrics_transactions_series(days=30)
    assert len(data) == 3
    assert len(data["timestamps"]) == 30
    # Always larger than timestamp of first block
    assert data["timestamps"][0] >= 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_transactions_series_full():
    data = await db.get_metrics_transactions_series_full()
    assert len(data) == 3
    # Starts at midnight UTC of genesis day
    assert data["timestamps"][0] == 1562025329
    assert data["transactions"][0] > 0


@pytest.mark.asyncio
async def test_get_metrics_utxos_series_30d():
    data = await db.get_metrics_utxos_series(days=30)
    assert len(data) == 3
    assert len(data["timestamps"]) == 30
    # Always larger than timestamp of first block
    assert data["timestamps"][0] >= 1561978977137 // 1000


@pytest.mark.asyncio
async def test_get_metrics_utxos_series_full():
    data = await db.get_metrics_utxos_series_full()
    assert len(data) == 3
    # Always starts with timestamp of first block
    assert data["timestamps"][0] == 1561978977137 // 1000
    assert data["boxes"][0] == 1
    assert data["boxes"][1] >= 1


@pytest.mark.asyncio
async def test_get_metrics_utxos_list():
    data = await db.get_metrics_utxos_list()
    assert len(data) == 100
    assert isinstance(data[0]["address"], str)
    assert data[0]["boxes"] > 0

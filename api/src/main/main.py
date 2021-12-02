import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    import db
except ImportError:
    from . import db

app = FastAPI()
# app = FastAPI(openapi_prefix="/api")

if "DEVMODE" in os.environ:
    print("DEVMODE - Setting CORS")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.on_event("startup")
async def startup_event():
    await db.init_connection_pool()


@app.get("/height")
async def get_height():
    """
    Get latest block height.
    """
    h = await db.get_latest_block_height()
    return h


@app.get("/sync-height")
async def get_db_height():
    """
    Get latest height processed by db.
    """
    h = await db.get_latest_sync_height()
    return h


@app.get("/oracle-pools/ergusd/latest")
async def get_oracle_pools_ergusd_latest():
    return await db.get_oracle_pools_ergusd_latest()


@app.get("/oracle-pools/ergusd/recent-epoch-durations")
async def get_oracle_pools_ergusd_recent_epoch_durations():
    return await db.get_oracle_pools_ergusd_recent_epoch_durations()


@app.get("/oracle-pools/ergusd/oracle-stats")
async def get_oracle_pools_ergusd_oracle_stats():
    return await db.get_oracle_pools_ergusd_oracle_stats()


@app.get("/sigmausd/state")
async def get_sigmausd_state():
    return await db.get_sigmausd_state()


@app.get("/sigmausd/ohlc/sigrsv/1d")
async def get_sigmausd_sigrsv_ohlc_d():
    return await db.get_sigmausd_sigrsv_ohlc_d()


@app.get("/sigmausd/history/1d")
async def get_sigmausd_series_liabs_1d():
    return await db.get_sigmausd_history(1)


@app.get("/sigmausd/history/5d")
async def get_sigmausd_series_liabs_5d():
    return await db.get_sigmausd_history(5)


@app.get("/sigmausd/history/30d")
async def get_sigmausd_history_30d():
    return await db.get_sigmausd_history(30)


@app.get("/sigmausd/history/90d")
async def get_sigmausd_history_90d():
    return await db.get_sigmausd_history(90)


@app.get("/sigmausd/history/all")
async def get_sigmausd_history_all():
    return await db.get_sigmausd_history_full()


@app.get("/metrics/preview")
async def get_metrics_preview():
    return await db.get_metrics_preview()


@app.get("/metrics/addresses/summary")
async def get_metrics_addresses_summary():
    return await db.get_metrics_address_counts_summary()


@app.get("/metrics/addresses/series/30d")
async def get_metrics_addresses_series_30d():
    return await db.get_metrics_addresses_series(30)


@app.get("/metrics/addresses/series/90d")
async def get_metrics_addresses_series_90d():
    return await db.get_metrics_addresses_series(90)


@app.get("/metrics/addresses/series/1y")
async def get_metrics_addresses_series_1y():
    return await db.get_metrics_addresses_series(365)


@app.get("/metrics/addresses/series/all")
async def get_metrics_addresses_series_all():
    return await db.get_metrics_addresses_series_full()


@app.get("/metrics/contracts/summary")
async def get_metrics_contracts_summary():
    return await db.get_metrics_contract_counts_summary()


@app.get("/metrics/contracts/series/30d")
async def get_metrics_contracts_series_30d():
    return await db.get_metrics_contracts_series(30)


@app.get("/metrics/contracts/series/90d")
async def get_metrics_contracts_series_90d():
    return await db.get_metrics_contracts_series(90)


@app.get("/metrics/contracts/series/1y")
async def get_metrics_contracts_series_1y():
    return await db.get_metrics_contracts_series(365)


@app.get("/metrics/contracts/series/all")
async def get_metrics_contracts_series_all():
    return await db.get_metrics_contracts_series_full()


@app.get("/metrics/distribution/summary")
async def get_metrics_distribution_summary():
    return await db.get_metrics_distribution_summary()


@app.get("/metrics/distribution/series/30d")
async def get_metrics_distribution_series_30d():
    return await db.get_metrics_distribution_series(30)


@app.get("/metrics/distribution/series/90d")
async def get_metrics_distribution_series_90d():
    return await db.get_metrics_distribution_series(90)


@app.get("/metrics/distribution/series/1y")
async def get_metrics_distribution_series_1y():
    return await db.get_metrics_distribution_series(365)


@app.get("/metrics/distribution/series/all")
async def get_metrics_distribution_series_all():
    return await db.get_metrics_distribution_series_full()


@app.get("/metrics/tvl/summary")
async def get_metrics_tvl_summary():
    return await db.get_metrics_tvl_summary()


@app.get("/metrics/tvl/series/30d")
async def get_metrics_tvl_series_30d():
    return await db.get_metrics_tvl_series(30)


@app.get("/metrics/tvl/series/90d")
async def get_metrics_tvl_series_90d():
    return await db.get_metrics_tvl_series(90)


@app.get("/metrics/tvl/series/1y")
async def get_metrics_tvl_series_1y():
    return await db.get_metrics_tvl_series(365)


@app.get("/metrics/tvl/series/all")
async def get_metrics_tvl_series_all():
    return await db.get_metrics_tvl_series_full()


@app.get("/metrics/cexs/summary")
async def get_metrics_cexs_summary():
    return await db.get_metrics_cexs_summary()


@app.get("/metrics/cexs/series/30d")
async def get_metrics_cexs_series_30d():
    return await db.get_metrics_cexs_series(30)


@app.get("/metrics/cexs/series/90d")
async def get_metrics_cexs_series_90d():
    return await db.get_metrics_cexs_series(90)


@app.get("/metrics/cexs/series/1y")
async def get_metrics_cexs_series_1y():
    return await db.get_metrics_cexs_series(365)


@app.get("/metrics/cexs/series/all")
async def get_metrics_cexs_series_all():
    return await db.get_metrics_cexs_series_full()


@app.get("/metrics/age/series/30d")
async def get_metrics_age_series_30d():
    return await db.get_metrics_age_series(30)


@app.get("/metrics/age/series/90d")
async def get_metrics_age_series_90d():
    return await db.get_metrics_age_series(90)


@app.get("/metrics/age/series/1y")
async def get_metrics_age_series_1y():
    return await db.get_metrics_age_series(365)


@app.get("/metrics/age/series/all")
async def get_metrics_age_series_all():
    return await db.get_metrics_age_series_full()


@app.get("/metrics/transfer-volume/series/30d")
async def get_metrics_transfer_volume_series_30d():
    return await db.get_metrics_transfer_volume_series(30)


@app.get("/metrics/transfer-volume/series/90d")
async def get_metrics_transfer_volume_series_90d():
    return await db.get_metrics_transfer_volume_series(90)


@app.get("/metrics/transfer-volume/series/1y")
async def get_metrics_transfer_volume_series_1y():
    return await db.get_metrics_transfer_volume_series(365)


@app.get("/metrics/transfer-volume/series/all")
async def get_metrics_transfer_volume_series_all():
    return await db.get_metrics_transfer_volume_series_full()


@app.get("/metrics/transactions/series/30d")
async def get_metrics_transactions_series_30d():
    return await db.get_metrics_transactions_series(30)


@app.get("/metrics/transactions/series/90d")
async def get_metrics_transactions_series_90d():
    return await db.get_metrics_transactions_series(90)


@app.get("/metrics/transactions/series/1y")
async def get_metrics_transactions_series_1y():
    return await db.get_metrics_transactions_series(365)


@app.get("/metrics/transactions/series/all")
async def get_metrics_transactions_series_all():
    return await db.get_metrics_transactions_series_full()


@app.get("/metrics/utxos/summary")
async def get_metrics_utxos_summary():
    return await db.get_metrics_utxos_summary()


@app.get("/metrics/utxos/series/30d")
async def get_metrics_utxos_series_30d():
    return await db.get_metrics_utxos_series(30)


@app.get("/metrics/utxos/series/90d")
async def get_metrics_utxos_series_90d():
    return await db.get_metrics_utxos_series(90)


@app.get("/metrics/utxos/series/1y")
async def get_metrics_utxos_series_1y():
    return await db.get_metrics_utxos_series(365)


@app.get("/metrics/utxos/series/all")
async def get_metrics_utxos_series_all():
    return await db.get_metrics_utxos_series_full()


@app.get("/metrics/utxos/list")
async def get_metrics_utxos_list():
    return await db.get_metrics_utxos_list()

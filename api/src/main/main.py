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
    origins = [
        "http://localhost",
        "http://localhost:8000",
        "http://localhost:3000",
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
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

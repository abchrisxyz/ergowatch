import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse

try:
    import db
except ImportError:
    from . import db

app = FastAPI()
# app = FastAPI(openapi_prefix="/api")


@app.get("/height")
async def get_height():
    """
    Get latest block height.
    """
    h = await db.get_latest_block_height()
    return h


@app.get("/oracle-pools/{pool_id}/commits")
async def get_oracle_pool_commits(pool_id: int):
    d = await db.get_oracle_pool_commits(pool_id)
    return JSONResponse(content=d)

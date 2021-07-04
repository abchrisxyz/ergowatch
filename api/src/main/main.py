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


@app.get("/oracle-pools/commit-stats/ergusd")
async def get_oracle_pool_commit_stats_ergusd():
    return await db.get_oracle_pool_commit_stats_ergusd()
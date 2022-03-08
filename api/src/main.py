import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncpg

from api.routes.addresses import addresses_router
from api.routes.p2pk import p2pk_router
from api.routes.contracts import contracts_router
from api.routes.ranking import ranking_router

root_path = "/api/v0"
description = f"""
ErgoWatch API docs.

All ERG values expressed in nanoERG.

[Swagger]({root_path}/docs) | [ReDoc]({root_path}/redoc)
"""
# TODO: explain history vs series

tags_metadata = [
    {
        "name": "addresses",
        "description": "Address specific data",
    },
    {
        "name": "p2pk",
        "description": "P2PK address stats",
    },
    {
        "name": "contracts",
        "description": "P2S & P2SH address stats",
    },
]

app = FastAPI(
    title="ErgoWatch",
    version="0.1.0-alpha",
    description=description,
    openapi_tags=tags_metadata,
    root_path=root_path,
)

if "DEVMODE" in os.environ:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.on_event("startup")
async def startup_event():
    dsn = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}"
    app.state.db = await asyncpg.create_pool(dsn)


app.include_router(addresses_router, prefix="/api/v0/addresses", tags=["addresses"])
app.include_router(p2pk_router, prefix="/api/v0/p2pk", tags=["p2pk"])
app.include_router(contracts_router, prefix="/api/v0/contracts", tags=["contracts"])
app.include_router(ranking_router, prefix="/api/v0/ranking", tags=["misc"])

import os
from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncpg

try:
    # Normal use case
    from api.routes.addresses import addresses_router
    from api.routes.p2pk import p2pk_router
    from api.routes.contracts import contracts_router
    from api.routes.ranking import ranking_router
    from api.routes.tokens import tokens_router
    from api.routes.status import status_router
    from api.routes.exchanges import exchanges_router
    from api.routes.lists import lists_router
    from api.routes.utils import utils_router
    from api.routes.sigmausd import sigmausd_router
except ImportError:
    # When running pytest
    from .api.routes.addresses import addresses_router
    from .api.routes.p2pk import p2pk_router
    from .api.routes.contracts import contracts_router
    from .api.routes.ranking import ranking_router
    from .api.routes.tokens import tokens_router
    from .api.routes.status import status_router
    from .api.routes.exchanges import exchanges_router
    from .api.routes.lists import lists_router
    from .api.routes.utils import utils_router
    from .api.routes.sigmausd import sigmausd_router

root_path = ""
if "FASTAPI_ROOT_PATH" in os.environ:
    root_path = os.environ["FASTAPI_ROOT_PATH"]

description = f"""
ErgoWatch API docs.

Rules of thumb:
 - all ERG values expressed in nanoERG
 - all token values expressed in integer form
 - all timestamps in milliseconds since unix epoch (same as node api)
 
Some endpoints will accept a `?token_id=` query parameter to return data relating to a given token instead of ERG itself.

ERG/USD data provided by [CoinGecko](https://www.coingecko.com/en/api).
"""

tags_metadata = [
    {
        "name": "status",
        "description": "Database status",
    },
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
    {
        "name": "exchanges",
        "description": "Individual exchange data",
    },
    {
        "name": "lists",
        "description": "Rich lists etc.",
    },
    # {
    #     "name": "tokens",
    #     "description": "Token specific data",
    # },
    {
        "name": "utils",
        "description": "Sometimes helpful",
    },
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "ergo")
    user = os.getenv("POSTGRES_USER", "ergo")
    pw = os.getenv("POSTGRES_PASSWORD")
    dsn = f"postgresql://{user}:{pw}@{host}:{port}/{db}"
    app.state.db = await asyncpg.create_pool(dsn)
    yield


app = FastAPI(
    title="ErgoWatch",
    version="1.1.2"
    description=description,
    openapi_tags=tags_metadata,
    root_path=root_path,
    lifespan=lifespan,
)


app.include_router(status_router, tags=["status"])
app.include_router(addresses_router, prefix="/addresses", tags=["addresses"])
app.include_router(contracts_router, prefix="/contracts", tags=["contracts"])
app.include_router(exchanges_router, prefix="/exchanges", tags=["exchanges"])
app.include_router(lists_router, prefix="/lists", tags=["lists"])
app.include_router(p2pk_router, prefix="/p2pk", tags=["p2pk"])
# app.include_router(tokens_router, prefix="/tokens", tags=["tokens"])
app.include_router(utils_router, prefix="/utils", tags=["utils"])
app.include_router(ranking_router, prefix="/ranking", tags=["misc"])
app.include_router(sigmausd_router, prefix="/sigmausd", tags=["misc"])

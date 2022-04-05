import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncpg

try:
    # Normal use case
    from api.routes.addresses import addresses_router
    from api.routes.p2pk import p2pk_router
    from api.routes.contracts import contracts_router
    from api.routes.ranking import ranking_router
    from api.routes.tokens import tokens_router
    from api.routes.status import status_router
    from api.routes.metrics import metrics_router
except ImportError:
    # When running pytest
    from .api.routes.addresses import addresses_router
    from .api.routes.p2pk import p2pk_router
    from .api.routes.contracts import contracts_router
    from .api.routes.ranking import ranking_router
    from .api.routes.tokens import tokens_router
    from .api.routes.status import status_router
    from .api.routes.metrics import metrics_router

root_path = "/api/v0"
description = f"""
ErgoWatch API docs.

Rules of thumb:
 - all ERG values expressed in nanoERG
 - all token values expressed in integer form
 - all timestamps in milliseconds since unix epoch (same as node api)
 
Most endpoints will accept a `?token_id=` query parameter to return data relating to a given token instead of ERG itself.

[Swagger]({root_path}/docs) | [ReDoc]({root_path}/redoc)

[Release notes](https://github.com/abchrisxyz/ergowatch/blob/master/CHANGELOG.md)

### Time windows
Endpoints under `/metrics` take the following query parameters to define a time window and its resolution:
- `fr`: first timestamp of time window
- `to`: last timestamp of time window
- `r`: resolution of time window, one of `block`, `1h` or `24h`

The distance between `fr` and `to` should not exceed 1000 times the resolution size, allowing for the following time windows:
- `block`: 120,000 ms * 1000 (~1.4 days, assuming 120 second blocks)
- `1h`: 3,600,000 ms * 1000 (~41 days)
- `24h`: 86,400,000 ms * 1000 (~2.7 years)

If `r` is omitted, default `block` level is used.

If none of `fr` and `to` are specified, returns last `24h`, `1h` or `block` level record, according to `r`.

If `fr` is specified without `to`, returns records since `fr`, up to max of window size.

If `to` is specified without `fr`, returns records prior to (and including) `to`, up to max of window size.
"""
# TODO: explain history vs series

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
    {"name": "metrics", "description": "Metrics over time"},
]

app = FastAPI(
    title="ErgoWatch",
    version="0.2.0",
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
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "ergo")
    user = os.getenv("POSTGRES_USER", "ergo")
    pw = os.getenv("POSTGRES_PASSWORD")
    dsn = f"postgresql://{user}:{pw}@{host}:{port}/{db}"
    app.state.db = await asyncpg.create_pool(dsn)


app.include_router(status_router, tags=["status"])
app.include_router(addresses_router, prefix="/addresses", tags=["addresses"])
app.include_router(metrics_router, prefix="/metrics", tags=["metrics"])
app.include_router(p2pk_router, prefix="/p2pk", tags=["p2pk"])
app.include_router(contracts_router, prefix="/contracts", tags=["contracts"])
app.include_router(tokens_router, prefix="/tokens", tags=["tokens"])
app.include_router(ranking_router, prefix="/ranking", tags=["misc"])

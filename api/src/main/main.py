import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

try:
    import db
except ImportError:
    from . import db

app = FastAPI()
# app = FastAPI(openapi_prefix="/api")

if "DEVMODE" in os.environ:
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

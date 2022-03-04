import asyncpg
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.v0.routes.addresses import addresses_router

app = FastAPI(
    title="ErgoWatch",
    version="0.1.0",
    description="ErgoWatch API docs",
    root_path="/api/v0",
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


app.include_router(addresses_router, prefix="/api/v0/addresses", tags=["Addresses"])

from fastapi import APIRouter

HISTORY_LIMIT = 1000

from .utxos import utxos_router

metrics_router = r = APIRouter()

r.include_router(utxos_router, prefix="/utxos")

from enum import Enum
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict
from ...constants import GENESIS_TIMESTAMP

BLOCK_TIME_MS = 120_000
HOUR_MS = 3_600_000
DAY_MS = 86_400_000


class TimeResolution(str, Enum):
    block = "block"
    hourly = "1h"
    daily = "24h"


class MetricsRecord(BaseModel):
    t: int
    v: int


def generate_time_window_limits(limit: int) -> Dict[TimeResolution, int]:
    return {
        TimeResolution.block: BLOCK_TIME_MS * limit,
        TimeResolution.hourly: HOUR_MS * limit,
        TimeResolution.daily: DAY_MS * limit,
    }


TimeWindowLimits = generate_time_window_limits(1000)

from .utxos import utxos_router

metrics_router = r = APIRouter()

r.include_router(utxos_router, prefix="/utxos")

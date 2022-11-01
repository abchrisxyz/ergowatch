from enum import Enum
from typing import List
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


class MetricsSeries(BaseModel):
    timestamps: List[int]
    values: List[int]


def generate_time_window_limits(limit: int) -> Dict[TimeResolution, int]:
    return {
        TimeResolution.block: BLOCK_TIME_MS * limit,
        TimeResolution.hourly: HOUR_MS * limit,
        TimeResolution.daily: DAY_MS * limit,
    }


LIMIT = 1000
TimeWindowLimits = generate_time_window_limits(LIMIT)

from .addresses import router as addresses_router
from .exchanges import exchanges_router
from .utxos import router as utxos_router
from .supply_age import router as supply_age_router
from .supply_distribution import router as supply_distribution_router

metrics_router = r = APIRouter()

r.include_router(addresses_router, prefix="/addresses")
r.include_router(exchanges_router, prefix="/exchanges")
r.include_router(supply_age_router, prefix="/supply/age")
r.include_router(supply_distribution_router, prefix="/supply/distribution")
r.include_router(utxos_router, prefix="/utxos")

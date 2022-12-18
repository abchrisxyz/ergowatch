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


SUMMARY_FIELDS = [
    "label",
    "current",
    "diff_1d",
    "diff_1w",
    "diff_4w",
    "diff_6m",
    "diff_1y",
]


class MetricsSummaryRecord(BaseModel):
    label: str
    current: float
    diff_1d: float
    diff_1w: float
    diff_4w: float
    diff_6m: float
    diff_1y: float


def generate_time_window_limits(limit: int) -> Dict[TimeResolution, int]:
    return {
        TimeResolution.block: BLOCK_TIME_MS * limit,
        TimeResolution.hourly: HOUR_MS * limit,
        TimeResolution.daily: DAY_MS * limit,
    }


LIMIT = 2000
TimeWindowLimits = generate_time_window_limits(LIMIT)


from . import addresses
from . import exchanges
from . import supply_age
from . import supply_composition
from . import supply_distribution
from . import transactions
from . import utxos
from . import volume

metrics_router = r = APIRouter()

r.include_router(addresses.router, prefix="/addresses")
r.include_router(exchanges.router, prefix="/exchanges")
r.include_router(supply_age.router, prefix="/supply/age")
r.include_router(supply_composition.router, prefix="/supply/composition")
r.include_router(supply_distribution.router, prefix="/supply/distribution")
r.include_router(transactions.router, prefix="/transactions")
r.include_router(utxos.router, prefix="/utxos")
r.include_router(volume.router, prefix="/volume")

metrics_summary_router = s = APIRouter()

s.include_router(addresses.summary_router, prefix="/addresses")
s.include_router(exchanges.summary_router, prefix="/exchanges")
s.include_router(supply_age.summary_router, prefix="/supply/age")
s.include_router(supply_composition.summary_router, prefix="/supply/composition")
s.include_router(supply_distribution.summary_router, prefix="/supply/distribution")
s.include_router(transactions.summary_router, prefix="/transactions")
s.include_router(utxos.summary_router, prefix="/utxos")
s.include_router(volume.summary_router, prefix="/volume")

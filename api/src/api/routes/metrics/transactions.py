from enum import Enum
from textwrap import dedent
from typing import List
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel

router = r = APIRouter()
summary_router = s = APIRouter()

from . import GENESIS_TIMESTAMP, SUMMARY_FIELDS
from . import TimeResolution
from . import TimeWindowLimits
from . import MetricsSummaryRecord


class TransactionsSeries(BaseModel):
    timestamps: List[int]
    daily_1d: List[int]
    daily_7d: List[int]
    daily_28d: List[int]


@r.get(
    "",
    response_model=TransactionsSeries,
    summary="Transactions per day",
    description=dedent(
        """
        Number of transactions over past 24h:
        - **daily_1d**: raw values
        - **daily_7d**: mean of past 7 days
        - **daily_28d**: mean of past 28 days
    """
    ),
)
async def supply_on_addresses(
    request: Request,
    fr: int = Query(
        default=None,
        ge=GENESIS_TIMESTAMP,
        description="Start of time window",
    ),
    to: int = Query(
        default=None,
        ge=GENESIS_TIMESTAMP,
        description="End of time window",
    ),
    r: TimeResolution = Query(
        default=TimeResolution.block,
        description="Time window resolution",
    ),
):
    if fr is not None and to is not None:
        return await _get_fr_to(request, fr, to, r)
    time_interval_limit = TimeWindowLimits[r]
    if (fr, to) == (None, None):
        return await _get_last(request)
    if fr is not None:
        to = fr + time_interval_limit
    else:
        fr = to - time_interval_limit
    return await _get_fr_to(request, fr, to, r)


async def _get_last(request: Request):
    """Return last record"""
    query = f"""
        select h.timestamp
            , m.daily_1d
            , m.daily_7d
            , m.daily_28d
        from mtr.transactions m
        join core.headers h on h.height = m.height
        order by h.height desc 
        limit 1;
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
    return {
        "timestamps": [row["timestamp"]],
        "daily_1d": [row["daily_1d"]],
        "daily_7d": [row["daily_7d"]],
        "daily_28d": [row["daily_28d"]],
    }


async def _get_fr_to(request: Request, fr: int, to: int, r: TimeResolution):
    time_interval_limit = TimeWindowLimits[r]
    if fr > to:
        raise HTTPException(
            status_code=422,
            detail="Parameter `fr` cannot be higher than `to`",
        )
    if to - fr > time_interval_limit:
        raise HTTPException(
            status_code=422,
            detail=f"Time window is limited to {time_interval_limit} for {r} resolution",
        )
    if r == TimeResolution.block:
        query = f"""
            select h.timestamp
                , m.daily_1d
                , m.daily_7d
                , m.daily_28d
            from mtr.transactions m
            join core.headers h on h.height = m.height
            where h.timestamp >= $1 and h.timestamp <= $2
            order by h.height;
        """
    else:
        query = f"""
            select t.timestamp
                , m.daily_1d
                , m.daily_7d
                , m.daily_28d
            from mtr.transactions m
            join mtr.timestamps_{r.name} t on t.height = m.height
            where t.timestamp >= $1 and t.timestamp <= $2
            order by t.height;
        """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, fr, to)
    return {
        "timestamps": [r["timestamp"] for r in rows],
        "daily_1d": [r["daily_1d"] for r in rows],
        "daily_7d": [r["daily_7d"] for r in rows],
        "daily_28d": [r["daily_28d"] for r in rows],
    }


@s.get("", response_model=List[MetricsSummaryRecord], summary=" ")
async def change_summary(request: Request):
    query = f"""
        select label
            , current
            , diff_1d
            , diff_1w
            , diff_4w
            , diff_6m
            , diff_1y
        from mtr.transactions_summary;
    """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query)
    return [{f: r[f] for f in SUMMARY_FIELDS} for r in rows]

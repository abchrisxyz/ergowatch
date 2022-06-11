from typing import List
from typing import Tuple
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel

exchanges_router = r = APIRouter()

from . import GENESIS_TIMESTAMP
from . import TimeResolution
from . import TimeWindowLimits
from . import HOUR_MS
from . import DAY_MS


class ExchangeMetricsRecord(BaseModel):
    t: int
    s: int
    d: int


@r.get(
    "/supply",
    response_model=List[ExchangeMetricsRecord],
    description=f"Supply on all tracked exchanges",
)
async def supply(
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
    """
    Returns total supply (s) and supply on deposit addresses (d).
    """
    if fr is not None and to is not None:
        return await _count_fr_to(request, fr, to, r)
    time_interval_limit = TimeWindowLimits[r]
    if (fr, to) == (None, None):
        return await _count_last(request, r)
    if fr is not None:
        to = fr + time_interval_limit
    else:
        fr = to - time_interval_limit
    return await _count_fr_to(request, fr, to, r)


async def _count_last(request: Request, r: TimeResolution):
    if r == TimeResolution.block:
        query = """
            select h.timestamp
                , m.total
                , m.deposit
            from mtr.cex_supply m
            join core.headers h on h.height = m.height
            order by h.height desc
            limit 1;
        """
    else:
        round_ms = DAY_MS
        if r == TimeResolution.hourly:
            round_ms = HOUR_MS
        query = f"""
            with last_ts as (
                select timestamp / {round_ms} * {round_ms} as timestamp
                from core.headers
                order by 1 desc
                limit 1
            )
            select last_ts.timestamp
                , m.total
                , m.deposit
            from mtr.cex_supply m
            join core.headers h on h.height = m.height, last_ts
            where h.timestamp <= last_ts.timestamp
            order by h.height desc 
            limit 1;
        """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
    return [{"t": row["timestamp"], "s": row["total"], "d": row["deposit"]}]


async def _count_fr_to(request: Request, fr: int, to: int, r: TimeResolution):
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
        query = """
            select h.timestamp as t
                , m.total as s
                , m.deposit as d
            from mtr.cex_supply m
            join core.headers h on h.height = m.height
            where h.timestamp >= $1 and h.timestamp <= $2
            order by h.height;
        """
    else:
        round_ms = DAY_MS
        if r == TimeResolution.hourly:
            round_ms = HOUR_MS
        query = f"""
            with tagged as (
                select h.timestamp
                    , m.total
                    , m.deposit
                    , lag(m.total) over w as previous_total
                    , lag(m.deposit) over w as previous_deposit
                    , h.timestamp / {round_ms} - lag(h.timestamp / {round_ms}) over w = 1 as first_of_day
                from mtr.cex_supply m
                join core.headers h on h.height = m.height
                -- Include 1 record prior to fr to ensure a previous value is always available
                where h.height >= (
                        select height-1
                        from core.headers
                        where timestamp >= $1
                        order by 1
                        limit 1
                    )
                    and h.timestamp <= $2
                window w as (order by h.height)
            )
            select timestamp / {round_ms} * {round_ms} as t
                , case
                    when timestamp % {round_ms} = 0 then total
                    else previous_total
                end as s
                , case
                    when timestamp % {round_ms} = 0 then deposit
                    else previous_deposit
                end as d
            from tagged
            where (first_of_day or timestamp % {round_ms} = 0)
                -- In case the height-1 record we included turns out to have a round timestamp
                and timestamp >= $1
            order by 1;
        """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, fr, to)
    return rows

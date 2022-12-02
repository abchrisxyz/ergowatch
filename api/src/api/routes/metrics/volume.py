from textwrap import dedent
from typing import List
from typing import Optional
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


class VolumeSeries(BaseModel):
    timestamps: List[int]
    daily_1d: List[int]
    daily_7d: List[int]
    daily_28d: List[int]
    ergusd: Optional[List[float]]


@r.get(
    "",
    response_model=VolumeSeries,
    response_model_exclude_none=True,
    summary="Daily transfer volume",
    description=dedent(
        """
        Supply sent to different address over past 24h:

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
    ergusd: bool = Query(default=False, description="Include ERG/USD price data"),
):
    if fr is not None and to is not None:
        return await _get_fr_to(request, fr, to, r, ergusd)
    time_interval_limit = TimeWindowLimits[r]
    if (fr, to) == (None, None):
        return await _get_last(request, ergusd)
    if fr is not None:
        to = fr + time_interval_limit
    else:
        fr = to - time_interval_limit
    return await _get_fr_to(request, fr, to, r, ergusd)


async def _get_last(request: Request, ergusd: bool):
    """Return last record"""
    query = f"""
        select h.timestamp
            , m.daily_1d
            , m.daily_7d
            , m.daily_28d
            {{}}
        from mtr.volume m
        join core.headers h on h.height = m.height
        {{}}
        order by h.height desc 
        limit 1;
    """
    query = format_ergusd(query, ergusd)
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
    res = {
        "timestamps": [row["timestamp"]],
        "daily_1d": [row["daily_1d"]],
        "daily_7d": [row["daily_7d"]],
        "daily_28d": [row["daily_28d"]],
    }
    if ergusd:
        res["ergusd"] = [row["ergusd"]]
    return res


async def _get_fr_to(
    request: Request, fr: int, to: int, r: TimeResolution, ergusd: bool
):
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
                {{}}
            from mtr.volume m
            join core.headers h on h.height = m.height
            {{}}
            where h.timestamp >= $1 and h.timestamp <= $2
            order by h.height;
        """
    else:
        query = f"""
            select t.timestamp
                , m.daily_1d
                , m.daily_7d
                , m.daily_28d
                {{}}
            from mtr.volume m
            join mtr.timestamps_{r.name} t on t.height = m.height
            {{}}
            where t.timestamp >= $1 and t.timestamp <= $2
            order by t.height;
        """
    query = format_ergusd(query, ergusd)
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, fr, to)
    res = {
        "timestamps": [r["timestamp"] for r in rows],
        "daily_1d": [r["daily_1d"] for r in rows],
        "daily_7d": [r["daily_7d"] for r in rows],
        "daily_28d": [r["daily_28d"] for r in rows],
    }
    if ergusd:
        res["ergusd"] = [r["ergusd"] for r in rows]
    return res


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
        from mtr.volume_summary;
    """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query)
    return [{f: r[f] for f in SUMMARY_FIELDS} for r in rows]


def format_ergusd(qry: str, ergusd: bool):
    if ergusd:
        return qry.format(
            ", p.value as ergusd", "left join mtr.ergusd p on p.height = m.height"
        )
    else:
        return qry.format("", "")

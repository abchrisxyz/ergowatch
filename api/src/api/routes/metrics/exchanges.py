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


class ExchangeSupplySeries(BaseModel):
    timestamps: List[int]
    total: List[int]
    deposit: List[int]
    ergusd: Optional[List[float]]


class MetricsSummary(BaseModel):
    p2pks: MetricsSummaryRecord
    cex_main: MetricsSummaryRecord
    cex_deposits: MetricsSummaryRecord
    contracts: MetricsSummaryRecord
    miners: MetricsSummaryRecord
    treasury: MetricsSummaryRecord


@r.get(
    "/supply",
    response_model=ExchangeSupplySeries,
    response_model_exclude_none=True,
    summary="Supply on exchanges",
)
async def supply_across_all_tracked_exchanges(
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
    """
    Returns total supply and supply on deposit addresses.
    Supply on main addresses is total - deposit.
    """
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
            , m.total
            , m.deposit
            {{}}
        from mtr.cex_supply m
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
        "total": [row["total"]],
        "deposit": [row["deposit"]],
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
                , m.total
                , m.deposit
                {{}}
            from mtr.cex_supply m
            join core.headers h on h.height = m.height
            {{}}
            where h.timestamp >= $1 and h.timestamp <= $2
            order by h.height;
        """
    else:
        query = f"""
            select t.timestamp
                , m.total
                , m.deposit
                {{}}
            from mtr.cex_supply m
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
        "total": [r["total"] for r in rows],
        "deposit": [r["deposit"] for r in rows],
    }
    if ergusd:
        res["ergusd"] = [r["ergusd"] for r in rows]
    return res


@s.get(
    "/supply",
    response_model=List[MetricsSummaryRecord],
    summary=" ",
)
async def change_summary(request: Request):
    query = f"""
        select 'total' as label
            , sum(current) as current
            , sum(diff_1d) as diff_1d
            , sum(diff_1w) as diff_1w
            , sum(diff_4w) as diff_4w
            , sum(diff_6m) as diff_6m
            , sum(diff_1y) as diff_1y
        from mtr.supply_composition_summary
        where label in ('cex_main', 'cex_deposits')
        union
        select 'main' as label
            , current
            , diff_1d
            , diff_1w
            , diff_4w
            , diff_6m
            , diff_1y
        from mtr.supply_composition_summary
        where label = 'cex_main'
        union
        select 'deposits' as label
            , current
            , diff_1d
            , diff_1w
            , diff_4w
            , diff_6m
            , diff_1y
        from mtr.supply_composition_summary
        where label = 'cex_deposits'
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

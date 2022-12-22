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


class SupplyAgeSeries(BaseModel):
    timestamps: List[int]
    overall: List[float]
    p2pks: List[float]
    cexs: List[float]
    contracts: List[float]
    miners: List[float]
    ergusd: Optional[List[float]]


@r.get(
    "",
    response_model=SupplyAgeSeries,
    response_model_exclude_none=True,
    summary="Mean supply age",
    include_in_schema=False,
    description=dedent(
        """
        Mean age of supply, in days, across:\n
        - **overall**: all addresses
        - **p2pks**: all P2PK addresses, except main exchange addresses
        - **cexs**: main exchange addresses
        - **contracts**: contracts addresses, except mining contracts
        - **miners**: mining contracts

        Treasury and (re)emission addresses are never included.
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
            , m.overall
            , m.p2pks
            , m.cexs
            , m.contracts
            , m.miners
            {{}}
        from mtr.supply_age_days m
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
        "overall": [row["overall"]],
        "p2pks": [row["p2pks"]],
        "cexs": [row["cexs"]],
        "contracts": [row["contracts"]],
        "miners": [row["miners"]],
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
                , m.overall
                , m.p2pks
                , m.cexs
                , m.contracts
                , m.miners
                {{}}
            from mtr.supply_age_days m
            join core.headers h on h.height = m.height
            {{}}
            where h.timestamp >= $1 and h.timestamp <= $2
            order by h.height;
        """
    else:
        query = f"""
            select t.timestamp
                , m.overall
                , m.p2pks
                , m.cexs
                , m.contracts
                , m.miners
                {{}}
            from mtr.supply_age_days m
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
        "overall": [r["overall"] for r in rows],
        "p2pks": [r["p2pks"] for r in rows],
        "cexs": [r["cexs"] for r in rows],
        "contracts": [r["contracts"] for r in rows],
        "miners": [r["miners"] for r in rows],
    }
    if ergusd:
        res["ergusd"] = [r["ergusd"] for r in rows]
    return res


@s.get(
    "", response_model=List[MetricsSummaryRecord], summary=" ", include_in_schema=False
)
async def change_summary(request: Request):
    query = f"""
        select label
            , current
            , diff_1d
            , diff_1w
            , diff_4w
            , diff_6m
            , diff_1y
        from mtr.supply_age_days_summary;
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

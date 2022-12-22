from textwrap import dedent
from typing import Dict
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


class SupplyCompositionSeries(BaseModel):
    timestamps: List[int]
    p2pks: List[int]
    cex_main: List[int]
    cex_deposits: List[int]
    contracts: List[int]
    miners: List[int]
    treasury: List[int]
    ergusd: Optional[List[float]]


class MetricsSummary(BaseModel):
    p2pks: MetricsSummaryRecord
    cex_main: MetricsSummaryRecord
    cex_deposits: MetricsSummaryRecord
    contracts: MetricsSummaryRecord
    miners: MetricsSummaryRecord
    treasury: MetricsSummaryRecord


@r.get(
    "",
    response_model=SupplyCompositionSeries,
    response_model_exclude_none=True,
    summary="Supply by address type",
    description=dedent(
        """
        Supply by address type:\n
        - **p2pks**: all P2PK addresses, except exchange addresses
        - **cex_main**: main exchange addresses
        - **cex_deposits**: deposit exchange addresses
        - **contracts**: contract addresses
        - **miners**: mining contracts
        - **treasury**: unlocked supply on EF's treasury contract

        Excludes (re)emission addresses.
        
        No overlap between address types.
        Sum of all terms = emitted supply &ge; circulating supply (because of reemissions still in mining contracts).
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
            , m.p2pks
            , m.cex_main
            , m.cex_deposits
            , m.contracts
            , m.miners
            , m.treasury
            {{}}
        from mtr.supply_composition m
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
        "p2pks": [row["p2pks"]],
        "cex_main": [row["cex_main"]],
        "cex_deposits": [row["cex_deposits"]],
        "contracts": [row["contracts"]],
        "miners": [row["miners"]],
        "treasury": [row["treasury"]],
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
                , m.p2pks
                , m.cex_main
                , m.cex_deposits
                , m.contracts
                , m.miners
                , m.treasury
                {{}}
            from mtr.supply_composition m
            join core.headers h on h.height = m.height
            {{}}
            where h.timestamp >= $1 and h.timestamp <= $2
            order by h.height;
        """
    else:
        query = f"""
            select t.timestamp
                , m.p2pks
                , m.cex_main
                , m.cex_deposits
                , m.contracts
                , m.miners
                , m.treasury
                {{}}
            from mtr.supply_composition m
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
        "p2pks": [r["p2pks"] for r in rows],
        "cex_main": [r["cex_main"] for r in rows],
        "cex_deposits": [r["cex_deposits"] for r in rows],
        "contracts": [r["contracts"] for r in rows],
        "miners": [r["miners"] for r in rows],
        "treasury": [r["treasury"] for r in rows],
    }
    if ergusd:
        res["ergusd"] = [r["ergusd"] for r in rows]
    return res


@s.get(
    "",
    response_model=Dict[str, List[MetricsSummaryRecord]],
    summary=" ",
)
async def change_summary(request: Request):
    query = f"""
        with circ_supply as (
            select current
                , diff_1d
                , diff_1w
                , diff_4w
                , diff_6m
                , diff_1y
            from mtr.supply_composition_summary
            where label = 'total'
        )
        select s.label
            , s.current
            , s.diff_1d 
            , s.diff_1w 
            , s.diff_4w 
            , s.diff_6m 
            , s.diff_1y
            , s.current::numeric / cs.current as current_rel
            , (s.current::numeric / cs.current) - (s.current::numeric - s.diff_1d) / (cs.current::numeric - cs.diff_1d)  as diff_1d_rel
            , (s.current::numeric / cs.current) - (s.current::numeric - s.diff_1w) / (cs.current::numeric - cs.diff_1w)  as diff_1w_rel
            , (s.current::numeric / cs.current) - (s.current::numeric - s.diff_4w) / (cs.current::numeric - cs.diff_4w)  as diff_4w_rel
            , (s.current::numeric / cs.current) - (s.current::numeric - s.diff_6m) / (cs.current::numeric - cs.diff_6m)  as diff_6m_rel
            , (s.current::numeric / cs.current) - (s.current::numeric - s.diff_1y) / (cs.current::numeric - cs.diff_1y)  as diff_1y_rel
        from mtr.supply_composition_summary s, circ_supply cs
        where label <> 'total';
    """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query)
    return {
        "absolute": [{f: r[f] for f in SUMMARY_FIELDS} for r in rows],
        "relative": [
            {f: r[f + "_rel" if f != "label" else f] for f in SUMMARY_FIELDS}
            for r in rows
        ],
    }


def format_ergusd(qry: str, ergusd: bool):
    if ergusd:
        return qry.format(
            ", p.value as ergusd", "left join mtr.ergusd p on p.height = m.height"
        )
    else:
        return qry.format("", "")

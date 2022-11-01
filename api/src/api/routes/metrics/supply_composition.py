from enum import Enum
from textwrap import dedent
from typing import List
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel

router = r = APIRouter()

from . import GENESIS_TIMESTAMP
from . import TimeResolution
from . import TimeWindowLimits


class SupplyCompositionSeries(BaseModel):
    timestamps: List[int]
    p2pks: List[int]
    cex_main: List[int]
    cex_deposits: List[int]
    contracts: List[int]
    miners: List[int]
    treasury: List[int]


@r.get(
    "",
    response_model=SupplyCompositionSeries,
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
            , m.p2pks
            , m.cex_main
            , m.cex_deposits
            , m.contracts
            , m.miners
            , m.treasury
        from mtr.supply_composition m
        join core.headers h on h.height = m.height
        order by h.height desc 
        limit 1;
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
    return {
        "timestamps": [row["timestamp"]],
        "p2pks": [row["p2pks"]],
        "cex_main": [row["cex_main"]],
        "cex_deposits": [row["cex_deposits"]],
        "contracts": [row["contracts"]],
        "miners": [row["miners"]],
        "treasury": [row["treasury"]],
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
                , m.p2pks
                , m.cex_main
                , m.cex_deposits
                , m.contracts
                , m.miners
                , m.treasury
            from mtr.supply_composition m
            join core.headers h on h.height = m.height
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
            from mtr.supply_composition m
            join mtr.timestamps_{r.name} t on t.height = m.height
            where t.timestamp >= $1 and t.timestamp <= $2
            order by t.height;
        """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, fr, to)
    return {
        "timestamps": [r["timestamp"] for r in rows],
        "p2pks": [r["p2pks"] for r in rows],
        "cex_main": [r["cex_main"] for r in rows],
        "cex_deposits": [r["cex_deposits"] for r in rows],
        "contracts": [r["contracts"] for r in rows],
        "miners": [r["miners"] for r in rows],
        "treasury": [r["treasury"] for r in rows],
    }

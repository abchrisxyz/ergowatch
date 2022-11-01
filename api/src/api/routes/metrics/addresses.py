from enum import Enum
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


class AddressType(str, Enum):
    p2pk = "p2pk"
    contract = "contracts"
    miner = "miners"


class AddressCountsSeries(BaseModel):
    timestamps: List[int]
    gt_0: List[int]
    ge_0p001: List[int]
    ge_0p01: List[int]
    ge_0p1: List[int]
    ge_1: List[int]
    ge_10: List[int]
    ge_100: List[int]
    ge_1k: List[int]
    ge_10k: List[int]
    ge_100k: List[int]
    ge_1m: List[int]


@r.get(
    "/{address_type}",
    response_model=AddressCountsSeries,
    summary="Number of addresses by minimal balance",
    description=f"Number of addresses by minimal balance.",
)
async def supply_on_addresses(
    request: Request,
    address_type: AddressType,
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
        return await _get_fr_to(request, address_type, fr, to, r)
    time_interval_limit = TimeWindowLimits[r]
    if (fr, to) == (None, None):
        return await _get_last(request, address_type)
    if fr is not None:
        to = fr + time_interval_limit
    else:
        fr = to - time_interval_limit
    return await _get_fr_to(request, address_type, fr, to, r)


async def _get_last(request: Request, address_type: AddressType):
    """Return last record"""
    query = f"""
        select h.timestamp
            , m.total
            , m.ge_0p001
            , m.ge_0p01
            , m.ge_0p1
            , m.ge_1
            , m.ge_10
            , m.ge_100
            , m.ge_1k
            , m.ge_10k
            , m.ge_100k
            , m.ge_1m
        from mtr.address_counts_by_balance_{address_type} m
        join core.headers h on h.height = m.height
        order by h.height desc 
        limit 1;
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
    return {
        "timestamps": [row["timestamp"]],
        "gt_0": [row["total"]],
        "ge_0p001": [row["ge_0p001"]],
        "ge_0p01": [row["ge_0p01"]],
        "ge_0p1": [row["ge_0p1"]],
        "ge_1": [row["ge_1"]],
        "ge_10": [row["ge_10"]],
        "ge_100": [row["ge_100"]],
        "ge_1k": [row["ge_1k"]],
        "ge_10k": [row["ge_10k"]],
        "ge_100k": [row["ge_100k"]],
        "ge_1m": [row["ge_1m"]],
    }


async def _get_fr_to(
    request: Request, address_type: AddressType, fr: int, to: int, r: TimeResolution
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
                , m.ge_0p001
                , m.ge_0p01
                , m.ge_0p1
                , m.ge_1
                , m.ge_10
                , m.ge_100
                , m.ge_1k
                , m.ge_10k
                , m.ge_100k
                , m.ge_1m
            from mtr.address_counts_by_balance_{address_type} m
            join core.headers h on h.height = m.height
            where h.timestamp >= $1 and h.timestamp <= $2
            order by h.height;
        """
    else:
        query = f"""
            select t.timestamp
                , m.total
                , m.ge_0p001
                , m.ge_0p01
                , m.ge_0p1
                , m.ge_1
                , m.ge_10
                , m.ge_100
                , m.ge_1k
                , m.ge_10k
                , m.ge_100k
                , m.ge_1m
            from mtr.address_counts_by_balance_{address_type} m
            join mtr.timestamps_{r.name} t on t.height = m.height
            where t.timestamp >= $1 and t.timestamp <= $2
            order by t.height;
        """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, fr, to)
    return {
        "timestamps": [r["timestamp"] for r in rows],
        "gt_0": [r["total"] for r in rows],
        "ge_0p001": [r["ge_0p001"] for r in rows],
        "ge_0p01": [r["ge_0p01"] for r in rows],
        "ge_0p1": [r["ge_0p1"] for r in rows],
        "ge_1": [r["ge_1"] for r in rows],
        "ge_10": [r["ge_10"] for r in rows],
        "ge_100": [r["ge_100"] for r in rows],
        "ge_1k": [r["ge_1k"] for r in rows],
        "ge_10k": [r["ge_10k"] for r in rows],
        "ge_100k": [r["ge_100k"] for r in rows],
        "ge_1m": [r["ge_1m"] for r in rows],
    }

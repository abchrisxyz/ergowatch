from enum import Enum
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
    ergusd: Optional[List[float]]


@r.get(
    "/{address_type}",
    response_model=AddressCountsSeries,
    response_model_exclude_none=True,
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
    ergusd: bool = Query(default=False, description="Include ERG/USD price data"),
):
    if fr is not None and to is not None:
        return await _get_fr_to(request, address_type, fr, to, r, ergusd)
    time_interval_limit = TimeWindowLimits[r]
    if (fr, to) == (None, None):
        return await _get_last(request, address_type, ergusd)
    if fr is not None:
        to = fr + time_interval_limit
    else:
        fr = to - time_interval_limit
    return await _get_fr_to(request, address_type, fr, to, r, ergusd)


async def _get_last(request: Request, address_type: AddressType, ergusd: bool):
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
            {{}}
        from mtr.address_counts_by_balance_{address_type} m
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
    if ergusd:
        res["ergusd"] = [row["ergusd"]]
    return res


async def _get_fr_to(
    request: Request,
    address_type: AddressType,
    fr: int,
    to: int,
    r: TimeResolution,
    ergusd: bool,
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
                {{}}
            from mtr.address_counts_by_balance_{address_type} m
            join core.headers h on h.height = m.height
            {{}}
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
                {{}}
            from mtr.address_counts_by_balance_{address_type} m
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
    if ergusd:
        res["ergusd"] = [r["ergusd"] for r in rows]
    return res


@s.get("/{address_type}", response_model=List[MetricsSummaryRecord], summary=" ")
async def change_summary(request: Request, address_type: AddressType):
    query = f"""
        select label
            , current
            , diff_1d
            , diff_1w
            , diff_4w
            , diff_6m
            , diff_1y
        from mtr.address_counts_by_balance_{address_type}_summary;
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

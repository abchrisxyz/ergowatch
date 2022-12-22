from enum import Enum
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

# Extra _rel fields for relative change summary
SUMMARY_FIELDS_REL = [f + "_rel" for f in SUMMARY_FIELDS if f != "label"]


class AddressType(str, Enum):
    p2pk = "p2pk"
    contract = "contracts"
    miner = "miners"


class SupplyDistributionSeries(BaseModel):
    timestamps: List[int]
    top_1prc: List[int]
    top_1k: List[int]
    top_100: List[int]
    top_10: List[int]
    circ_supply: List[int]
    ergusd: Optional[List[float]]


@r.get(
    "/{address_type}",
    response_model=SupplyDistributionSeries,
    response_model_exclude_none=True,
    summary="Supply on top x addresses",
    description=f"Supply on top 10, 100, 1000 and top 1% addresses.",
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
            , top_1_prc
            , top_1k
            , top_100
            , top_10
            , s.circulating_supply
            {{}}
        from mtr.supply_on_top_addresses_{address_type} m
        join core.headers h on h.height = m.height
        join blk.stats s on s.height = m.height
        {{}}
        order by h.height desc 
        limit 1;
    """
    query = format_ergusd(query, ergusd)
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
    res = {
        "timestamps": [row["timestamp"]],
        "top_1prc": [row["top_1_prc"]],
        "top_1k": [row["top_1k"]],
        "top_100": [row["top_100"]],
        "top_10": [row["top_10"]],
        "circ_supply": [row["circulating_supply"]],
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
                , m.top_1_prc
                , m.top_1k
                , m.top_100
                , m.top_10
                , s.circulating_supply
                {{}}
            from mtr.supply_on_top_addresses_{address_type} m
            join core.headers h on h.height = m.height
            join blk.stats s on s.height = m.height
            {{}}
            where h.timestamp >= $1 and h.timestamp <= $2
            order by h.height;
        """
    else:
        query = f"""
            select t.timestamp
                , m.top_1_prc
                , m.top_1k
                , m.top_100
                , m.top_10
                , s.circulating_supply
                {{}}
            from mtr.supply_on_top_addresses_{address_type} m
            join mtr.timestamps_{r.name} t on t.height = m.height
            join blk.stats s on s.height = m.height
            {{}}
            where t.timestamp >= $1 and t.timestamp <= $2
            order by t.height;
        """
    query = format_ergusd(query, ergusd)
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, fr, to)
    res = {
        "timestamps": [r["timestamp"] for r in rows],
        "top_1prc": [r["top_1_prc"] for r in rows],
        "top_1k": [r["top_1k"] for r in rows],
        "top_100": [r["top_100"] for r in rows],
        "top_10": [r["top_10"] for r in rows],
        "circ_supply": [r["circulating_supply"] for r in rows],
    }
    if ergusd:
        res["ergusd"] = [r["ergusd"] for r in rows]
    return res


@s.get(
    "/{address_type}", response_model=Dict[str, List[MetricsSummaryRecord]], summary=" "
)
async def change_summary(request: Request, address_type: AddressType):
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
        from mtr.supply_on_top_addresses_{address_type}_summary s, circ_supply cs;
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

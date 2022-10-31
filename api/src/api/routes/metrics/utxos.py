from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request

router = r = APIRouter()

from . import GENESIS_TIMESTAMP
from . import TimeResolution
from . import TimeWindowLimits
from . import MetricsSeries


@r.get(
    "",
    response_model=MetricsSeries,
    description=f"UTxO counts",
    summary="Number of UTxO's",
)
async def counts(
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
        return await _count_fr_to(request, fr, to, r)
    time_interval_limit = TimeWindowLimits[r]
    if (fr, to) == (None, None):
        return await _count_last(request)
    if fr is not None:
        to = fr + time_interval_limit
    else:
        fr = to - time_interval_limit
    return await _count_fr_to(request, fr, to, r)


async def _count_last(request: Request):
    """Return last record"""
    query = f"""
        select h.timestamp
            , m.value
        from mtr.utxos m
        join core.headers h on h.height = m.height
        order by h.height desc 
        limit 1;
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
    return {"timestamps": [row["timestamp"]], "values": [row["value"]]}


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
            select h.timestamp
                , m.value
            from mtr.utxos m
            join core.headers h on h.height = m.height
            where h.timestamp >= $1 and h.timestamp <= $2
            order by h.height;
        """
    else:
        query = f"""
            select t.timestamp
                , m.value
            from mtr.utxos m
            join mtr.timestamps_{r.name} t on t.height = m.height
            where t.timestamp >= $1 and t.timestamp <= $2
            order by t.height;
        """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, fr, to)
    return {
        "timestamps": [r["timestamp"] for r in rows],
        "values": [r["value"] for r in rows],
    }

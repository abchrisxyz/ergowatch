from typing import List
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from fastapi import Request


from ..models import TokenID

utxos_router = r = APIRouter()

HISTORY_LIMIT = 10_000


@r.get("/count", response_model=int, description="Current number of UTxO's")
async def count(request: Request):
    query = """
        select value from mtr.utxos order by height desc limit 1;
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
    return row["value"]


@r.get(
    "/count/at/height/{height}",
    response_model=int,
    description="Number of UTxO's at given height",
)
async def count_at_height(
    request: Request,
    height: int = Path(None, ge=0),
):
    query = "select value from mtr.utxos where height = $1;"
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, height)
    if row is None:
        raise HTTPException(status_code=404)
    return row["value"]


@r.get(
    "/count/history",
    response_model=List[int],
    description=f"UTxO counts at each network height. First available height is 0 (genesis). Query interval is limited to {HISTORY_LIMIT:,} records.",
)
async def count_history(
    request: Request,
    from_height: int = Query(
        default=None,
        ge=0,
        description="Height lower bound.",
    ),
    to_height: int = Query(
        default=None,
        ge=0,
        description="Height upper bound. May be higher than current sync height.",
    ),
):
    query = """
        select height
            , value
        from mtr.utxos
        where true
    """
    args = []
    order = "asc"
    limit = HISTORY_LIMIT
    if from_height is not None and to_height is not None:
        if from_height > to_height:
            raise HTTPException(
                status_code=422,
                detail="Parameter `from_height` cannot be higher than `to_height`",
            )
        if to_height - from_height + 1 > limit:
            raise HTTPException(
                status_code=422, detail=f"Height interval is limited to {limit}"
            )
        args.append(from_height)
        args.append(to_height)
        query += f" and height >= $1 and height <= $2"
    elif from_height is not None:
        args.append(from_height)
        query += f" and height >= $1"
    elif to_height is not None:
        args.append(to_height)
        query += f" and height <= ${len(args)}"
        order = "desc"
    query += f" order by height {order} limit {limit}"

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *args)
    res = [r["value"] for r in rows]
    if order == "desc":
        res.reverse()
    return res

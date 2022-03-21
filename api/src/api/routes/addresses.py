from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from fastapi import Request

from ..models import Address
from ..models import TokenID

addresses_router = r = APIRouter()


DETAIL_404 = "No balance found"


@r.get("/{address}/balance", response_model=int)
async def address_balance(
    request: Request,
    address: Address,
    token_id: TokenID = Query(None, description="Optional token id"),
):
    """
    Current ERG or token balance of an address.
    """
    args = [address]
    query = """
        select value
        from bal.erg
        where address = $1;
    """
    if token_id is not None:
        args.append(token_id)
        query = """
            select value
            from bal.tokens
            where address = $1
                and token_id = $2;
        """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, *args)
        if row is None:
            raise HTTPException(status_code=404, detail=DETAIL_404)
        return row["value"]


@r.get("/{address}/balance/at/height/{height}", response_model=int)
async def address_balance_at_height(
    request: Request,
    address: Address,
    height: int = Path(None, ge=0),
    token_id: TokenID = Query(None, description="Optional token id"),
):
    opt_args = []
    query = """
        select sum(value) as value
        from bal.erg_diffs
        where address = $1 and height <= $2
    """
    if token_id is not None:
        opt_args = [token_id]
        query = """
            select sum(value) as value
            from bal.tokens_diffs
            where address = $1
                and height <= $2
                and token_id = $3
        """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, address, height, *opt_args)
        value = row["value"]
    if value is None:
        raise HTTPException(status_code=404, detail=DETAIL_404)
    return value


@r.get("/{address}/balance/at/timestamp/{timestamp}", response_model=int)
async def address_balance_at_timestamp(
    request: Request,
    address: Address,
    timestamp: int = Path(..., gt=0),
    token_id: TokenID = Query(None, description="Optional token id"),
):
    opt_args = []
    query = """
        select sum(d.value) as value
        from bal.erg_diffs d
        join core.headers h on h.height = d.height
        where d.address = $1 and h.timestamp <= $2
    """
    if token_id is not None:
        opt_args = [token_id]
        query = """
            select sum(value) as value
            from bal.tokens_diffs d
            join core.headers h on h.height = d.height
            where address = $1
                and h.timestamp <= $2
                and token_id = $3
        """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, address, timestamp, *opt_args)
        value = row["value"]
    if value is None:
        raise HTTPException(status_code=404, detail=DETAIL_404)
    return value


@r.get("/{address}/balance/history")
async def address_balance_history(
    request: Request,
    address: Address,
    token_id: TokenID = Query(None, description="Optional token id"),
    timestamps: bool = Query(
        False, description="Include timestamps in addition to block heights"
    ),
    flat: bool | None = Query(True, description="Return data as flat arrays."),
    limit: int | None = Query(50, gt=0, le=10000),
    offset: int | None = Query(0, ge=0),
    desc: bool | None = Query(True, description="Most recent first"),
):
    """
    ERG or token balance history of an address.
    """
    query = f"""
        select d.height
            {', h.timestamp' if timestamps else ''}
            , sum(d.value) over (order by d.height) as balance
        from bal.{'erg' if token_id is None else 'tokens'}_diffs d
        join core.headers h on h.height = d.height
        where d.address = $1
            {'' if token_id is None else 'and token_id = $4'}
        order by 1 {'desc' if desc else ''}
        limit $2 offset $3;
    """
    opt_args = [] if token_id is None else [token_id]
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, address, limit, offset, *opt_args)
    if not rows:
        raise HTTPException(status_code=404, detail=DETAIL_404)
    if flat:
        if timestamps:
            return {
                "heights": [r["height"] for r in rows],
                "timestamps": [r["timestamp"] for r in rows],
                "balances": [r["balance"] for r in rows],
            }
        else:
            return {
                "heights": [r["height"] for r in rows],
                "balances": [r["balance"] for r in rows],
            }
    else:
        return rows

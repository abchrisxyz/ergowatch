from fastapi import APIRouter
from fastapi import Path
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel
from pydantic import constr
from typing import List
from typing import Union

addresses_router = r = APIRouter()

Address = constr(regex="^[a-zA-Z0-9]+$")


@r.get("/{address}/balance", response_model=int)
async def address_balance(
    request: Request,
    address: Address,
    # token_id: str = Query(None, description="Optional token id"),
):
    """
    Current ERG or token balance of an address.
    """
    query = """
        select value
        from bal.erg
        where address = $1;
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, address)
        return row["value"] if row is not None else None


@r.get("/{address}/balance/at/height/{height}", response_model=int)
async def address_balance_at_height(
    request: Request, address: Address, height: int = Path(None, ge=0)
):
    query = """
        select sum(value) as value
        from bal.erg_diffs
        where address = $1 and height <= $2
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, address, height)
        value = row["value"]
        return value if value is not None else 0


@r.get("/{address}/balance/at/timestamp/{timestamp}", response_model=int)
async def address_balance_at_timestamp(
    request: Request, address: Address, timestamp: int = Path(..., gt=0)
):
    query = """
        select sum(d.value) as value
        from bal.erg_diffs d
        join core.headers h on h.height = d.height
        where d.address = $1 and h.timestamp <= $2
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, address, timestamp)
        value = row["value"]
        return value if value is not None else 0


@r.get("/{address}/balance/history")
async def address_balance_history(
    request: Request,
    address: Address,
    # token_id: str = Query(None, description="Optional token id"),
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
        from bal.erg_diffs d
        join core.headers h on h.height = d.height
        where d.address = $1
        order by 1 {'desc' if desc else ''}
        limit $2 offset $3;
    """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, address, limit, offset)
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

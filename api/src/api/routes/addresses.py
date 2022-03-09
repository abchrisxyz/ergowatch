from fastapi import APIRouter, Query, Request
from pydantic import constr

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


# TODO:
# @r.get("/{address}/balance/at/height/{height}", response_model=int)
# @r.get("/{address}/balance/at/height/{timestamp}", response_model=int)


@r.get("/{address}/balance/history")
async def address_balance_history(
    request: Request,
    address: Address,
    limit: int | None = Query(50, ge=0),
    offset: int | None = Query(0, ge=0),
    desc: bool | None = Query(True, description="Most recent first"),
    flat: bool | None = Query(True, description="Return data as flat arrays")
    # token_id: str = Query(None, description="Optional token id"),
):
    """
    ERG or token balance history of an address.
    """
    query = f"""
        select d.height
            , h.timestamp
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
            return {
                "heights": [r["height"] for r in rows],
                "timestamps": [r["timestamp"] for r in rows],
                "balances": [r["balance"] for r in rows],
            }
        else:
            return rows

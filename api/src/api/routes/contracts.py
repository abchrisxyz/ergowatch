from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request

from ..models import TokenID

contracts_router = r = APIRouter()


# @r.get("/count", response_model=int)
@r.get("/count")
async def get_contract_address_count(
    request: Request,
    token_id: TokenID = Query(None, description="Optional token id"),
    bal_ge: int = Query(
        description="Only count contract addresses with a balance greater or equal to *bal_ge*",
        default=None,
        ge=0,
    ),
    bal_lt: int = Query(
        description="Only count contract addresses with balance lower than *bal_lt*",
        default=None,
        ge=0,
    ),
):
    """
    Current contract addresses count.
    """
    query = f"""
        select count(*) as cnt
        from {'erg' if token_id is None else 'tokens'}.balances
        where address_id % 10 <> 1
    """
    args = []
    value_col = "nano" if token_id is None else "value"
    if token_id is not None:
        args.append(token_id)
        query += (
            f" and asset_id = (select asset_id from core.tokens where token_id = $1)"
        )
    if bal_ge is not None:
        args.append(bal_ge)
        query += f" and {value_col} >= ${len(args)}"
    if bal_lt is not None:
        args.append(bal_lt)
        query += f" and {value_col} < ${len(args)}"

    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, *args)
    if row["cnt"] is None:
        raise HTTPException(status_code=404)
    return row["cnt"]


@r.get("/supply", description="Supply in contracts")
async def supply_in_contracts(
    request: Request,
    token_id: TokenID = Query(None, description="Optional token id"),
):
    """
    Current supply in contract addresses. Excludes coinbase address.
    """
    value_col = "nano" if token_id is None else "value"
    query = f"""
        select sum({value_col}) as value
        from {'erg' if token_id is None else 'tokens'}.balances
        where address_id not in (13, 33) and address_id % 10 <> 1
    """
    args = []
    if token_id is not None:
        args.append(token_id)
        query += (
            f" and asset_id = (select asset_id from core.tokens where token_id = $1)"
        )

    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, *args)
        if row["value"] is None:
            raise HTTPException(status_code=404)
        return row["value"]

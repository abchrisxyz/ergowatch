from enum import Enum
from typing import List
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel

from ..models import TokenID

lists_router = r = APIRouter()

TOKEN_404 = "Token not found"


class AddressBalance(BaseModel):
    address: str
    balance: int


@r.get("/addresses/by/balance", response_model=List[AddressBalance])
async def rich_list(
    request: Request,
    token_id: TokenID = Query(
        None,
        description="Optional token id",
    ),
    limit: int = Query(
        default=100,
        gt=0,
        le=10000,
    ),
):
    """
    Get addresses with largest balance. Does not include (re-)emission addresses.
    """
    if token_id is None:
        query = f"""
            select a.address
                , b.nano as balance
            from erg.balances b
            join core.addresses a on a.id = b.address_id 
            where b.address_id not in (13, 5965233, 5993503)
            order by b.nano desc
            limit $1;
        """
        args = [limit]
    else:
        query = """
            select a.address
                , b.value as balance
            from tokens.balances b
            join core.addresses a on a.id = b.address_id
            where b.asset_id = (select asset_id from core.tokens where token_id = $2)
            order by b.value desc
            limit $1;
        """
        args = [limit, token_id]

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *args)
    if not rows:
        raise HTTPException(status_code=404, detail=TOKEN_404)
    return [AddressBalance(**r) for r in rows]

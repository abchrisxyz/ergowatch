from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Request
from pydantic import BaseModel

from ..models import TokenID

tokens_router = r = APIRouter()


class TokenSupply(BaseModel):
    total: int
    circulating: int
    burned: int


@r.get("/{token_id}/supply", response_model=TokenSupply)
async def token_supply(
    request: Request,
    token_id: TokenID,
):
    """
    Emitted, circulating and burned supply
    """
    query = """
        select 
            (
                select emission_amount
                from core.tokens
                where id = $1
            ) as total
            ,  
            (
                select sum(value)
                from bal.tokens
                where token_id = $1
            ) as circulating
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, token_id)
    if row["total"] is None:
        raise HTTPException(status_code=404)
    return {
        "total": row["total"],
        "circulating": row["circulating"],
        "burned": row["total"] - row["circulating"],
    }

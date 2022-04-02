from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Request
from pydantic import BaseModel

from ..models import TokenID

tokens_router = r = APIRouter()


class TokenDetails(BaseModel):
    token_id: TokenID
    emission_amount: int
    name: str | None
    description: str | None
    decimals: int
    standard: str | None


class TokenSupply(BaseModel):
    emitted: int
    in_p2pks: int
    in_contracts: int
    burned: int


@r.get("/{token_id}", response_model=TokenDetails)
async def token_details(
    request: Request,
    token_id: TokenID,
):
    query = """
        select id as token_id
            , emission_amount
            , name
            , description
            , coalesce(decimals, 0) as decimals
            , standard
        from core.tokens
        where id = $1;
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, token_id)
    if row is None:
        raise HTTPException(status_code=404)
    return row


@r.get("/{token_id}/supply", response_model=TokenSupply)
async def token_supply(
    request: Request,
    token_id: TokenID,
):
    """
    Token supply breakdown (emitted, in P2PK addresses, in contracts and burned). Emitted is sum of other three.
    """
    query = """
        select 
        (
            select emission_amount
            from core.tokens
            where id = $1
        ) as emitted
        ,  
        (
            select array[sum(value)
                , sum(value) filter (where address not like '9%' or length(address) <> 51)
            ] as total_and_contracts
            from bal.tokens
            where token_id = $1
        ) 
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, token_id)
    if row["emitted"] is None:
        raise HTTPException(status_code=404)
    total, in_contracts = row["total_and_contracts"]
    return {
        "emitted": row["emitted"],
        "in_p2pks": total - in_contracts,
        "in_contracts": in_contracts,
        "burned": row["emitted"] - total,
    }

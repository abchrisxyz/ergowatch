from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Request
from pydantic import BaseModel
from pydantic import constr


ranking_router = r = APIRouter()


P2PKAddress = constr(regex="^9[a-zA-Z0-9]{50}$")


class AddressRank(BaseModel):
    rank: int
    address: str
    balance: int


class RankResponse(BaseModel):
    above: None | AddressRank
    target: None | AddressRank
    under: None | AddressRank


@r.get("/{p2pk_address}", response_model=RankResponse, name="P2PK address rank")
async def p2pk_address_rank(
    request: Request,
    p2pk_address: P2PKAddress,
):
    """
    Get the rank of a P2PK address by current balance.
    Includes next and previous addresses as well.
    """
    query = f"""
        with ranked_p2pk as (
            select rank() over (order by value desc)
                , address
                , value
            from bal.erg
            where address like '9%'
                and length(address) = 51
            order by address desc
        ), target as (
            select rank
                , address
                , value
            from ranked_p2pk
            where address = $1
        )
        -- Target address
        select 'target' as label
            , rank
            , value
            , address
        from target
        union
        -- First higher ranked
        select * from (
            select 'above' as label
                , p.rank
                , p.value
                , p.address
            from ranked_p2pk p, target t
            where p.rank < t.rank
            order by p.rank desc, p.address
            limit 1
        ) above
        union
        -- First lower ranked
        select * from (
            select 'under' as label
                , p.rank
                , p.value
                , p.address
            from ranked_p2pk p, target t
            where p.rank > t.rank
            order by p.rank, p.address
            limit 1
        ) under
    """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, p2pk_address)
        if not rows:
            raise HTTPException(status_code=404, detail="Address not found")
        return {
            row["label"]: {
                "rank": row["rank"],
                "address": row["address"],
                "balance": row["value"],
            }
            for row in rows
        }

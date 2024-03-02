from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Request
from pydantic import BaseModel
from pydantic import constr


ranking_router = r = APIRouter()


P2PKAddress = constr(pattern="^9[a-zA-Z0-9]{50}$")


class AddressRank(BaseModel):
    rank: int
    address: str
    balance: int


class RankResponse(BaseModel):
    above: None | AddressRank = None
    target: None | AddressRank
    under: None | AddressRank = None


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
            select rank() over (order by nano desc)
                , address_id
                , nano
            from erg.balances
            where address_id % 10 = 1 -- p2pk's only
        ), target as (
            select rank
                , address_id
                , nano
            from ranked_p2pk
            where address_id = core.address_id($1)
        ), neighbours as (
            -- Target address
            select 'target' as label
                , rank
                , nano
                , address_id
            from target
            union
            -- First higher ranked
            select * from (
                select 'above' as label
                    , p.rank
                    , p.nano
                    , p.address_id
                from ranked_p2pk p, target t
                where p.rank < t.rank
                order by p.rank desc, p.address_id
                limit 1
            ) above
            union
            -- First lower ranked
            select * from (
                select 'under' as label
                    , p.rank
                    , p.nano
                    , p.address_id
                from ranked_p2pk p, target t
                where p.rank > t.rank
                order by p.rank, p.address_id
                limit 1
            ) under
        )
        select n.label
            , n.rank
            , n.nano
            , a.address
        from neighbours n
        join core.addresses a on a.id = n.address_id
    """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, p2pk_address)
        if not rows:
            raise HTTPException(status_code=404, detail="Address not found")
        return {
            row["label"]: {
                "rank": row["rank"],
                "address": row["address"],
                "balance": row["nano"],
            }
            for row in rows
        }

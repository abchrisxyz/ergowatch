from fastapi import APIRouter, Request, Query
from pydantic import BaseModel
from pydantic import constr
from enum import Enum


p2pk_router = r = APIRouter()


@r.get("/count", response_model=int, name="Number of P2PK addresses")
async def get_p2pk_address_count(
    request: Request,
    bal_ge: int = Query(
        default=None,
        description="Only count P2PK addresses with a balance greater or equal to *bal_ge* nanoErg",
        ge=0,
    ),
    bal_lt: int = Query(
        default=None,
        description="Only count P2PK addresses with balance lower than *bal_lt* nanoErg",
        ge=0,
    ),
):
    """
    Current P2PK addresses count.
    """
    query = f"""
        select count(*) as cnt
        from bal.erg
        where address like '9%' and length(address) = 51
    """
    args = []
    if bal_ge is not None:
        args.append(bal_ge)
        query += f" and value >= $1"
    if bal_lt is not None:
        args.append(bal_lt)
        query += f" and value < ${len(args)}"

    async with request.app.state.db.acquire() as conn:
        res = await conn.fetchrow(query, *args)
        return res["cnt"]

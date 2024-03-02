from enum import Enum
from typing import List
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel

from ..constants import HISTORY_LIMIT

exchanges_router = r = APIRouter()


class ExchangeAddressInfo(BaseModel):
    cex: str
    address: str
    balance: int


@r.get("/tracklist", response_model=List[ExchangeAddressInfo])
async def list_tracked_addresses(request: Request):
    """
    List tracked main addresses, by cex, with balance.
    """
    query = """
        select m.address
            , c.text_id
            , coalesce(b.nano, 0) as bal
        from exchanges.main_addresses m
        join exchanges.exchanges c on c.id = m.cex_id
        left join erg.balances b on b.address_id = m.address_id
        order by c.text_id, m.address_id desc;
    """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query)
    return [
        {
            "cex": r["text_id"],
            "address": r["address"],
            "balance": r["bal"],
        }
        for r in rows
    ]

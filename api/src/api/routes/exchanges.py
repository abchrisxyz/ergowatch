from enum import Enum
from typing import List
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel

from ..constants import HISTORY_LIMIT

exchanges_router = r = APIRouter()

DETAIL_404 = "Exchange not found"

TEXT2ID = {
    "coinex": 1,
    "gate": 2,
    "kucoin": 3,
    "probit": 4,
    "tradeogre": 5,
    "huobi": 6,
}


class Exchange(str, Enum):
    coinex = "coinex"
    gate = "gate"
    kucoin = "kucoin"
    probit = "probit"
    tradeogre = "tradeogre"
    huobi = "huobi"


class ExchangeSupply(BaseModel):
    timestamps: List[int]
    main: List[int]
    deposit: List[int]


@r.get("", response_model=List[str])
async def list_tracked_exchanges(request: Request):
    """
    List id's of tracked exchanges.
    """
    query = """
        select array_agg(text_id) as exchanges
        from cex.cexs
        order by 1;
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
        return row["exchanges"]


@r.get("/{exchange}/supply", response_model=ExchangeSupply)
async def exchange_supply_history(
    request: Request,
    exchange: Exchange,
    since: int = Query(
        default=None,
        ge=0,
        description="Height or timestamp of first record",
    ),
    limit: int = Query(
        default=None,
        gt=0,
        le=HISTORY_LIMIT,
    ),
):
    """
    Supply on known main and deposit addresses.

    Omit `since` to retrieve the latest record only.
    """
    if exchange not in TEXT2ID:
        raise HTTPException(status_code=404, detail=DETAIL_404)
    cex_id = TEXT2ID[exchange]
    if since is None:
        # Latest record
        query = f"""
            select h.timestamp
                , s.main
                , s.deposit
            from cex.supply s
            join core.headers h on h.height = s.height
            where s.cex_id = $1
            order by h.height desc
            limit 1;
        """
        args = [
            cex_id,
        ]
    else:
        limit = HISTORY_LIMIT if limit is None else limit
        if since < 1_000_000_000:
            # Since height
            query = f"""
                select h.timestamp
                    , s.main
                    , s.deposit
                from cex.supply s
                join core.headers h on h.height = s.height
                where s.cex_id = $1 and s.height >= coalesce(
                    (
                        -- get first supply height before `since` height
                        select height
                        from cex.supply
                        where cex_id = $1 and height <= $2
                        order by 1 desc
                        limit 1
                    ), 0
                )
                order by h.height
                limit $3;
            """
        else:
            # Since timestamp
            if since < 10_000_000_000:
                raise HTTPException(
                    status_code=422,
                    detail="`since` timestamp doesn't appear to be in milliseconds",
                )
            query = f"""
                select h.timestamp
                    , s.main
                    , s.deposit
                from cex.supply s
                join core.headers h on h.height = s.height
                where s.cex_id = $1 and s.height >= coalesce(
                    (
                        -- get first supply height before timestamp
                        select s.height
                        from cex.supply s
                        join core.headers h on h.height = s.height
                        where s.cex_id = $1 and h.timestamp <= $2
                        order by 1 desc
                        limit 1
                    ), 0
                )
                order by h.height
                limit $3;
            """
        args = [cex_id, since, limit]
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *args)
    return {
        "timestamps": [r["timestamp"] for r in rows],
        "main": [r["main"] for r in rows],
        "deposit": [r["deposit"] for r in rows],
    }

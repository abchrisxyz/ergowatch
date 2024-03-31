from enum import Enum
from typing import List
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel

from ..models import TokenID

sigmausd_router = r = APIRouter()


class SigmaUSDState(BaseModel):
    reserves: int
    circ_sigusd: float
    circ_sigrsv: int
    peg_rate_nano: int


@r.get("/state", response_model=SigmaUSDState)
async def state(request: Request):
    """
    Current SigmaUSD contract state:
        - reserves: Total nanoERG in reserves
        - circ_sigusd: Circulating (i.e. minted) SigUSD
        - circ_sigrsv: Circulating (i.e. minted) SigRSV
        - peg_rate_nano: The ERG/USD oracle peg rate (nanoERG equivalent to 1 USD)
    """
    query = f"""
        select reserves
            , round(circ_sc / 100., 2) as circ_sigusd
            , circ_rc as circ_sigrsv
            , oracle as peg_rate_nano
        from sigmausd.history
        order by height desc
        limit 1;
    """
    async with request.app.state.db.acquire() as conn:
        row = (await conn.fetch(query))[0]
    return SigmaUSDState(**row)

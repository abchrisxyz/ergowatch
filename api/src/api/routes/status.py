from typing import Tuple
from datetime import datetime

from fastapi import APIRouter
from fastapi import Request
from pydantic import BaseModel

status_router = r = APIRouter()


class Status(BaseModel):
    height: int


class Repairs(BaseModel):
    started: datetime
    range: Tuple[int, int]
    at: int


@r.get("/sync_height", response_model=Status)
async def sync_height(request: Request):
    query = "select height from core.headers order by 1 desc limit 1;"
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
        return {
            "height": row["height"],
        }


@r.get(
    "/repairs",
    response_model=Repairs | None,
    summary="Running repair session details",
)
async def repairs(request: Request):
    query = "select * from ew.repairs;"
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
        if row is None:
            return None
        r = {
            "started": row["started"],
            "range": [row["from_height"], row["last_height"]],
            "at": row["next_height"],
        }
        print(r)
        return r

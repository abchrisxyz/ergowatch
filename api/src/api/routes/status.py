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
    query = "select max(height) as height from core.headers;"
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
        return {
            "height": row["height"],
        }

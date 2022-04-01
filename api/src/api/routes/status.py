from fastapi import APIRouter
from fastapi import Request
from pydantic import BaseModel

status_router = r = APIRouter()


class Status(BaseModel):
    height: int


@r.get("/sync_height", response_model=Status)
async def sync_height(request: Request):
    query = "select height from core.headers order by 1 desc limit 1;"
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query)
        return {
            "height": row["height"],
        }

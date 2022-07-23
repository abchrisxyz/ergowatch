from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Path
from fastapi import Request
from ..constants import GENESIS_TIMESTAMP

utils_router = r = APIRouter()


@r.get("/height2timestamp/{height}", response_model=int)
async def height_to_timestamp(
    request: Request,
    height: int = Path(None, ge=0),
):
    """
    Get timestamp (in milliseconds) of block at given height.
    """
    query = """
        select timestamp
        from core.headers
        where height = $1;
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, height)
    if not row:
        raise HTTPException(status_code=404)
    return row["timestamp"]


@r.get("/timestamp2height/{timestamp}", response_model=int)
async def timestamp_to_height(
    request: Request,
    timestamp: int = Path(None, ge=GENESIS_TIMESTAMP, description="milliseconds"),
):
    """
    Convert timestamp (in milliseconds) to height.
    """
    query = """
        select height
        from core.headers
        where timestamp <= $1
        order by timestamp desc
        limit 1;
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, timestamp)
    if not row:
        raise HTTPException(status_code=404)
    return row["height"]

from enum import Enum
from typing import List
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel

from ..models import TokenID

lists_router = r = APIRouter()

TOKEN_404 = "Token not found"


class AddressBalance(BaseModel):
    address: str
    balance: int


@r.get("/addresses/by/balance", response_model=List[AddressBalance])
async def rich_list(
    request: Request,
    token_id: TokenID = Query(
        None,
        description="Optional token id",
    ),
    limit: int = Query(
        default=100,
        gt=0,
        le=10000,
    ),
):
    """
    Get addresses with largest balance.
    """
    if token_id is None:
        query = f"""
            select a.address
                , b.value as balance
            from adr.erg b
            join core.addresses a on a.id = b.address_id 
            where a.address <> '2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU'
            order by b.value desc
            limit $1;
        """
        args = [limit]
    else:
        query = """
            select a.address
                , b.value as balance
            from adr.tokens b
            join core.addresses a on a.id = b.address_id
            where b.token_id = $2
            order by b.value desc
            limit $1;  
        """
        args = [limit, token_id]

    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, *args)
    if not rows:
        raise HTTPException(status_code=404, detail=TOKEN_404)
    return rows

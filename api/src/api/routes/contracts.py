from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request

from ..models import TokenID

contracts_router = r = APIRouter()

# @r.get("/count", response_model=int)
@r.get("/count")
async def get_contract_address_count(
    request: Request,
    token_id: TokenID = Query(None, description="Optional token id"),
    bal_ge: int = Query(
        description="Only count contract addresses with a balance greater or equal to *bal_ge*",
        default=None,
        ge=0,
    ),
    bal_lt: int = Query(
        description="Only count contract addresses with balance lower than *bal_lt*",
        default=None,
        ge=0,
    ),
):
    """
    Current contract addresses count.
    """
    query = """
        select count(*) as cnt
        from bal.erg
        where (address not like '9%' or length(address) <> 51)
    """
    args = []
    if token_id is not None:
        args.append(token_id)
        query = query.replace("bal.erg", "bal.tokens")
        query += f" and token_id = $1"
    if bal_ge is not None:
        args.append(bal_ge)
        query += f" and value >= ${len(args)}"
    if bal_lt is not None:
        args.append(bal_lt)
        query += f" and value < ${len(args)}"

    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, *args)
    if row["cnt"] is None:
        raise HTTPException(status_code=404)
    return row["cnt"]


@r.get("/supply", description="Supply in contracts")
async def supply_in_contracts(
    request: Request,
    token_id: TokenID = Query(None, description="Optional token id"),
):
    """
    Current supply in contract addresses. Excludes coinbase address.
    """
    query = """
        select sum(value) as value
        from bal.erg
        where address <> '2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU' 
            and address <> '4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy'
            and (address not like '9%' or length(address) <> 51)
    """
    args = []
    if token_id is not None:
        args.append(token_id)
        query = query.replace("bal.erg", "bal.tokens")
        query += f" and token_id = $1"

    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, *args)
        if row["value"] is None:
            raise HTTPException(status_code=404)
        return row["value"]

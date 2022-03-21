from fastapi import APIRouter
from fastapi import Query
from fastapi import Request


from ..models import TokenID

p2pk_router = r = APIRouter()


@r.get("/count", response_model=int, name="Number of P2PK addresses")
async def get_p2pk_address_count(
    request: Request,
    token_id: TokenID = Query(None, description="Optional token id"),
    bal_ge: int = Query(
        default=None,
        description="Only count P2PK addresses with a balance greater or equal to *bal_ge*",
        ge=0,
    ),
    bal_lt: int = Query(
        default=None,
        description="Only count P2PK addresses with balance lower than *bal_lt*",
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
        res = await conn.fetchrow(query, *args)
        return res["cnt"]

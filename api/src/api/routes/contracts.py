from fastapi import APIRouter, Request, Query


contracts_router = r = APIRouter()

# @r.get("/count", response_model=int)
@r.get("/count")
async def get_contract_address_count(
    request: Request,
    bal_ge: int = Query(
        description="Only count contract addresses with a balance greater or equal to *bal_ge* nanoErg",
        default=None,
        ge=0,
    ),
    bal_lt: int = Query(
        description="Only count contract addresses with balance lower than *bal_lt* nanoErg",
        default=None,
        ge=0,
    ),
):
    """
    Current contract addresses count.
    """
    query = f"""
        select count(*) as cnt
        from bal.erg
        where (address not like '9%' or length(address) <> 51)
    """
    args = []
    if bal_ge is not None:
        args.append(bal_ge)
        query += f" and value >= $1"
    if bal_lt is not None:
        args.append(bal_lt)
        query += f" and value < ${len(args)}"

    async with request.app.state.db.acquire() as conn:
        res = await conn.fetchrow(query, *args)
        return res["cnt"]

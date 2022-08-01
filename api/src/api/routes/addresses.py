from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from fastapi import Request
from typing import List

from ..models import Address
from ..models import TokenID

addresses_router = r = APIRouter()


DETAIL_404 = "No balance found"

TAGS = {
    "4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy": [
        "ef-treasury"
    ],
    "MUbV38YgqHy7XbsoXWF5z7EZm524Ybdwe5p9WDrbhruZRtehkRPT92imXer2eTkjwPDfboa1pR3zb3deVKVq3H7Xt98qcTqLuSBSbHb7izzo5jphEpcnqyKJ2xhmpNPVvmtbdJNdvdopPrHHDBbAGGeW7XYTQwEeoRfosXzcDtiGgw97b2aqjTsNFmZk7khBEQywjYfmoDc9nUCJMZ3vbSspnYo3LarLe55mh2Np8MNJqUN9APA6XkhZCrTTDRZb1B4krgFY1sVMswg2ceqguZRvC9pqt3tUUxmSnB24N6dowfVJKhLXwHPbrkHViBv1AKAJTmEaQW2DN1fRmD9ypXxZk8GXmYtxTtrj3BiunQ4qzUCu1eGzxSREjpkFSi2ATLSSDqUwxtRz639sHM6Lav4axoJNPCHbY8pvuBKUxgnGRex8LEGM8DeEJwaJCaoy8dBw9Lz49nq5mSsXLeoC4xpTUmp47Bh7GAZtwkaNreCu74m9rcZ8Di4w1cmdsiK1NWuDh9pJ2Bv7u3EfcurHFVqCkT3P86JUbKnXeNxCypfrWsFuYNKYqmjsix82g9vWcGMmAcu5nagxD4iET86iE2tMMfZZ5vqZNvntQswJyQqv2Wc6MTh4jQx1q2qJZCQe4QdEK63meTGbZNNKMctHQbp3gRkZYNrBtxQyVtNLR8xEY8zGp85GeQKbb37vqLXxRpGiigAdMe3XZA4hhYPmAAU5hpSMYaRAjtvvMT3bNiHRACGrfjvSsEG9G2zY5in2YWz5X9zXQLGTYRsQ4uNFkYoQRCBdjNxGv6R58Xq74zCgt19TxYZ87gPWxkXpWwTaHogG1eps8WXt8QzwJ9rVx6Vu9a5GjtcGsQxHovWmYixgBU8X9fPNJ9UQhYyAWbjtRSuVBtDAmoV1gCBEPwnYVP5GCGhCocbwoYhZkZjFZy6ws4uxVLid3FxuvhWvQrVEDYp7WRvGXbNdCbcSXnbeTrPMey1WPaXX": [
        "sigmausd-v2"
    ],
}


@r.get("/{address}/balance", response_model=int)
async def address_balance(
    request: Request,
    address: Address,
    token_id: TokenID = Query(None, description="Optional token id"),
):
    """
    Current ERG or token balance of an address.
    """
    args = [address]
    query = """
        select value
        from adr.erg
        where address_id = core.address_id($1);
    """
    if token_id is not None:
        args.append(token_id)
        query = """
            select value
            from adr.tokens
            where address_id = core.address_id($1)
                and token_id = $2;
        """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, *args)
        if row is None:
            raise HTTPException(status_code=404, detail=DETAIL_404)
        return row["value"]


@r.get("/{address}/balance/at/height/{height}", response_model=int)
async def address_balance_at_height(
    request: Request,
    address: Address,
    height: int = Path(None, ge=0),
    token_id: TokenID = Query(None, description="Optional token id"),
):
    opt_args = []
    query = """
        select sum(value) as value
        from adr.erg_diffs
        where address_id = core.address_id($1) and height <= $2
    """
    if token_id is not None:
        opt_args = [token_id]
        query = """
            select sum(value) as value
            from adr.tokens_diffs
            where address_id = core.address_id($1)
                and height <= $2
                and token_id = $3
        """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, address, height, *opt_args)
        value = row["value"]
    if value is None:
        raise HTTPException(status_code=404, detail=DETAIL_404)
    return value


@r.get("/{address}/balance/at/timestamp/{timestamp}", response_model=int)
async def address_balance_at_timestamp(
    request: Request,
    address: Address,
    timestamp: int = Path(..., gt=0),
    token_id: TokenID = Query(None, description="Optional token id"),
):
    opt_args = []
    query = """
        select sum(d.value) as value
        from adr.erg_diffs d
        join core.headers h on h.height = d.height
        where d.address_id = core.address_id($1) and h.timestamp <= $2
    """
    if token_id is not None:
        opt_args = [token_id]
        query = """
            select sum(value) as value
            from adr.tokens_diffs d
            join core.headers h on h.height = d.height
            where address_id = core.address_id($1)
                and h.timestamp <= $2
                and token_id = $3
        """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, address, timestamp, *opt_args)
        value = row["value"]
    if value is None:
        raise HTTPException(status_code=404, detail=DETAIL_404)
    return value


@r.get("/{address}/balance/history")
async def address_balance_history(
    request: Request,
    address: Address,
    token_id: TokenID = Query(None, description="Optional token id"),
    timestamps: bool = Query(
        False, description="Include timestamps in addition to block heights"
    ),
    flat: bool | None = Query(True, description="Return data as flat arrays."),
    limit: int | None = Query(50, gt=0, le=10000),
    offset: int | None = Query(0, ge=0),
    desc: bool | None = Query(True, description="Most recent first"),
):
    """
    ERG or token balance history of an address.
    """
    query = f"""
        select d.height
            {', h.timestamp' if timestamps else ''}
            , sum(d.value) over (order by d.height) as balance
        from adr.{'erg' if token_id is None else 'tokens'}_diffs d
        join core.headers h on h.height = d.height
        where d.address_id = core.address_id($1)
            {'' if token_id is None else 'and token_id = $4'}
        order by 1 {'desc' if desc else ''}
        limit $2 offset $3;
    """
    opt_args = [] if token_id is None else [token_id]
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query, address, limit, offset, *opt_args)
    if not rows:
        raise HTTPException(status_code=404, detail=DETAIL_404)
    if flat:
        if timestamps:
            return {
                "heights": [r["height"] for r in rows],
                "timestamps": [r["timestamp"] for r in rows],
                "balances": [r["balance"] for r in rows],
            }
        else:
            return {
                "heights": [r["height"] for r in rows],
                "balances": [r["balance"] for r in rows],
            }
    else:
        return rows


@r.get("/{address}/tags", response_model=List[str])
async def address_tags(
    request: Request,
    address: Address,
):
    """
    Returns all tags assotiated with `address` (e.g. exchange address, known contract, etc.)
    """
    if address in TAGS:
        return TAGS[address]
    tags = []
    # Exchange address
    query = """
        select c.text_id
            , a.type
        from cex.addresses a
        join cex.cexs c on c.id = a.cex_id
        where a.address_id = core.address_id($1);
    """
    async with request.app.state.db.acquire() as conn:
        row = await conn.fetchrow(query, address)
    if row is not None:
        tags.append(f"exchange")
        tags.append(f"exchange-{row['type']}")
        tags.append(f"exchange-{row['text_id']}")
    return tags

import requests
from datetime import datetime, timezone
import asyncio
import logging
from textwrap import dedent
from typing import List

import asyncpg as pg

from utils import prep_logger

logger = logging.getLogger("coingecko")
prep_logger(logger)


API_ROOT = "https://api.coingecko.com/api/v3"


def _cg_get_price(timestamp: int):
    """
    Return USD price of coin for given date.
    """
    half_window = 40000
    at_unix = timestamp // 1000
    fr_unix = at_unix - half_window
    to_unix = at_unix + half_window

    qry = (
        API_ROOT
        + f"/coins/ergo/market_chart/range?vs_currency=usd&from={fr_unix}&to={to_unix}"
    )

    r = requests.get(qry)
    d = r.json()

    diffs = [(ts, p, abs(ts - timestamp)) for (ts, p) in d["prices"]]
    best =  [d for d in diffs if d[2] == min([d[2] for d in diffs])][0]

    ts, usd, diff = best

    return ts, usd


async def qry_unprocessed_first_of_day_block_timestamps(conn: pg.Connection) -> List[int]:
    """
    Returns timestamps of all unprocessed first-of-day blocks
    """
    qry = dedent(
        """
        with last_processed_day as (
            select 0 as timestamp -- ensure at least one row when starting from scratch
            union
            select timestamp
            from cgo.price_at_first_of_day_block
            order by 1 desc
            limit 1
        ), first_of_day_blocks as (
            select nhs.timestamp / 86400000 as day_ts
                , min(nhs.timestamp) as timestamp
            from node_headers nhs, last_processed_day lpd
            where main_chain
                -- >= and not > or you get the second of day, thrid and so on...
                and nhs.timestamp >= lpd.timestamp
            group by 1
        )
        select array_agg(fdb.timestamp order by fdb.timestamp)
        from first_of_day_blocks fdb
        -- Keep new blocks only
        left join cgo.price_at_first_of_day_block prc
            on prc.timestamp = fdb.timestamp
        where prc.timestamp is null
        order by 1;
        """
    )
    r = await conn.fetchrow(qry)
    return r[0] if r[0] is not None else []


async def qry_unprocessed_last_of_day_block_timestamps(conn: pg.Connection) -> List[int]:
    """
    Returns timestamps of all unprocessed last-of-day blocks
    """
    qry = dedent(
        """
        with last_processed_day as (
            select 0 as timestamp -- ensure at least one row when starting from scratch
            union
            select timestamp
            from cgo.price_at_last_of_day_block
            order by 1 desc
            limit 1
        ), last_of_day_blocks as (
            select nhs.timestamp / 86400000 as day_ts
                , max(nhs.timestamp) as timestamp
            from node_headers nhs, last_processed_day lpd
            where main_chain
                and nhs.timestamp > lpd.timestamp
            group by 1
            order by 1 desc
            offset 1
        )
        select array_agg(timestamp order by timestamp)
        from last_of_day_blocks;
        """
    )
    r = await conn.fetchrow(qry)
    return r[0] if r[0] is not None else []


async def _sync(conn: pg.Connection, variant: str):
    """
    Update first-of-day or last-of-day.
    """
    logger.info(f"Updating {variant}-of-day blocks")

    ergo_timestamps = []
    if variant == 'first':
        ergo_timestamps = await qry_unprocessed_first_of_day_block_timestamps(conn)
    elif variant == 'last':
        ergo_timestamps = await qry_unprocessed_last_of_day_block_timestamps(conn)
    else:
        logger.error("Unknown variant")
        return

    logger.info(f"Number of {variant}-of-day timestamps to process: {len(ergo_timestamps)}")

    while ergo_timestamps:
        ts = ergo_timestamps.pop(0)

        # Fetch price from coingecko
        price_ts, price_usd = _cg_get_price(ts)

        # Insert in db
        await conn.execute(
            f"insert into cgo.price_at_{variant}_of_day_block (timestamp, usd, coingecko_ts) values ($1, $2, $3);",
            ts,
            price_usd,
            price_ts
        )

        # Show progress when processing many dates
        if len(ergo_timestamps) > 30:
            logger.info(f"{variant}-of-day - {datetime.fromtimestamp(ts / 1000, tz=timezone.utc)}: {price_usd}")

        # Be nice with the gecko
        if ergo_timestamps:
            await asyncio.sleep(1)


async def sync(conn: pg.Connection):
    """
    Fetches ERG price from Coingecko and inserts into db.

    Price is retrieved for timestamps of first-of-day and
    last-of-dayblocks that don't have a datapoint yet.
    """
    logger.info("Syncing started")
    await _sync(conn, 'first')
    await _sync(conn, 'last')
    logger.info("Syncing completed")



async def main():
    """
    Convenience wrapper to call sync() on it's own.

    Usefull when boostrapping the db.
    """
    conn = await pg.connect(DBSTR)
    await sync(conn)
    await conn.close()


def debug(timestamp):
    from datetime import timezone

    print(timestamp, datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc))

    at_unix = timestamp // 1000
    half_window = 40000
    fr_unix = at_unix - half_window
    to_unix = at_unix + half_window

    print(datetime.fromtimestamp(fr_unix, tz=timezone.utc))
    print(datetime.fromtimestamp(to_unix, tz=timezone.utc))

    qry = (
        API_ROOT
        + f"/coins/ergo/market_chart/range?vs_currency=usd&from={fr_unix}&to={to_unix}"
    )

    r = requests.get(qry)
    d = r.json()
    prices = d["prices"]

    diffs = [(ts, p, abs(ts - timestamp)) for (ts, p) in prices]
    best =  [d for d in diffs if d[2] == min([d[2] for d in diffs])][0]
    print(best)

    for (ts, p) in prices:
        print(ts, datetime.fromtimestamp(ts / 1000, tz=timezone.utc), p)
    print('-----')

    print(ts, datetime.fromtimestamp(best[0] / 1000, tz=timezone.utc), best[1])


if __name__ == "__main__":
    from local import DBSTR

    asyncio.get_event_loop().run_until_complete(main())

    # debug(1563062518473)

    # debug(1562976228965)

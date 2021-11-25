import requests
from datetime import datetime, timezone
import asyncio
import logging

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


async def sync(conn: pg.Connection):
    """
    Fetches ERG price from Coingecko and inserts into db.

    Price is retrieved for all timestamps of first-of-day blocks
    that don't have a datapoint yet.
    """
    logger.info("Syncing started")
    row = await conn.fetchrow("select cgo.get_new_first_of_day_blocks();")
    ergo_timestamps = row[0] if row[0] is not None else []
    logger.info(f"Number of timestamps to process: {len(ergo_timestamps)}")

    while ergo_timestamps:
        ts = ergo_timestamps.pop(0)

        # Fetch price from coingecko
        price_ts, price_usd = _cg_get_price(ts)

        # Insert in db
        await conn.execute(
            "insert into cgo.price_at_first_of_day_block (timestamp, usd, coingecko_ts) values ($1, $2, $3);",
            ts,
            price_usd,
            price_ts
        )

        # Show progress when processing many dates
        if len(ergo_timestamps) > 30:
            logger.info(f"{datetime.fromtimestamp(ts / 1000, tz=timezone.utc)}: {price_usd}")

        # Be nice with the gecko
        if ergo_timestamps:
            await asyncio.sleep(1)


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

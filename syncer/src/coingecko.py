import requests
from datetime import datetime
import asyncio
import logging

import asyncpg as pg

from utils import prep_logger

logger = logging.getLogger("coingecko")
prep_logger(logger)


API_ROOT = "https://api.coingecko.com/api/v3"


def _cg_get_price(date_dt: datetime):
    """
    Return USD price of coin for given date.
    """
    # Format date to CoinGecko api standard
    date_cg = date_dt.strftime("%d-%m-%Y")

    # Query
    qry = API_ROOT + f"/coins/ergo/history?date={date_cg}"
    r = requests.get(qry)
    d = r.json()

    return d["market_data"]["current_price"]["usd"]


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

        # Convert ergo timestamp to python datetime
        dt = datetime.fromtimestamp(ts / 1000)

        # Fetch price from coingecko
        price_usd = _cg_get_price(dt)

        # Insert in db
        await conn.execute(
            "insert into cgo.price_at_first_of_day_block (timestamp, usd) values ($1, $2);",
            ts,
            price_usd,
        )

        # Show progress when processing many dates
        if len(ergo_timestamps) > 30:
            print(dt, price_usd)

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


if __name__ == "__main__":
    from local import DBSTR
    asyncio.get_event_loop().run_until_complete(main())
